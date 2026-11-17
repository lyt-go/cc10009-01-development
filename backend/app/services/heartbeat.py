"""心跳处理：读取采集心跳事件、校验、续租并生成可调度节点结果。

处理口径：

* 事件先落 ``heartbeat`` 表，处理时只消费 ``待处理`` 的事件，重复的事件编号不再生效。
* 一次处理里同一个节点最多延长一次租约；采集时间相同的并发心跳会被合并。
* 槽位快照整份校验，任一项非法（或显式标记写入失败）就把节点移出可调度集合，
  并留下原因码为 ``slot_snapshot_failed`` 的最终记录。
* 处理结束后统一发布一个批次，再驱动请求分流侧清单更新。
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from app.config import settings
from app.lock import DOMAIN_LOCK
from app.services import node_lease, pipeline, schedulable
from app.services.slots import default_snapshot, snapshot_digest, validate_snapshot
from app.store import store
from app.timeutil import format_time, now_utc, parse_time, to_int

MODULE = "heartbeat"

STATUS_PENDING = "待处理"
STATUS_DONE = "已处理"
STATUS_MERGED = "已合并"
STATUS_REJECTED = "已拒绝"
STATUS_LATE = "迟到"
BLOCKING_KINDS = ("人工离线", "槽位快照失败")


def _parse_event_time(value: Any) -> tuple[Any, str | None]:
    if value in (None, ""):
        return now_utc(), None
    parsed = parse_time(value)
    if parsed is None:
        return None, f"采集时间「{value}」无法解析"
    ahead = (parsed - now_utc()).total_seconds()
    if ahead > settings.heartbeat_clock_skew_seconds:
        return None, f"采集时间超前当前时间 {int(ahead)} 秒，按节点时钟漂移拒绝"
    return parsed, None


def _parse_lease(value: Any) -> tuple[int | None, str | None]:
    if value in (None, ""):
        return settings.lease_seconds_default, None
    seconds = to_int(value, None)
    if seconds is None or seconds <= 0:
        return None, f"租约秒数「{value}」必须是正整数"
    if seconds > settings.lease_seconds_max:
        return None, f"租约秒数不能超过 {settings.lease_seconds_max}"
    return seconds, None


def list_events(
    *,
    node_code: str | None = None,
    status: str | None = None,
    page: int = 1,
    size: int = 20,
) -> tuple[list[dict[str, Any]], int]:
    rows = store.rows(MODULE)
    if node_code:
        rows = [row for row in rows if node_code in str(row.get("节点编号", ""))]
    if status:
        rows = [row for row in rows if row.get("处理状态") == status]
    total = len(rows)
    start = max(page - 1, 0) * size
    return rows[start:start + size], total


def collect(values: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    """采集一批心跳事件；只做结构校验，业务校验留到处理阶段。"""
    raw_events = values.get("events")
    if raw_events is None:
        raw_events = [values]
    if not isinstance(raw_events, list):
        return [], ["events 必须是事件列表"]
    created: list[dict[str, Any]] = []
    errors: list[str] = []
    with DOMAIN_LOCK:
        for index, raw in enumerate(raw_events, start=1):
            if not isinstance(raw, dict):
                errors.append(f"第 {index} 条事件不是对象")
                continue
            node_code = str(raw.get("节点编号") or "").strip()
            event_code = str(raw.get("事件编号") or "").strip()
            if not node_code:
                errors.append(f"第 {index} 条事件缺少节点编号")
                continue
            if not event_code:
                event_code = f"HB-{store.next_sequence():08d}"
            existing = next(
                (row for row in store.rows(MODULE) if row.get("事件编号") == event_code),
                None,
            )
            if existing is not None:
                errors.append(f"事件 {event_code} 已采集，未重复入库")
                continue
            event = {
                "id": store.next_id(MODULE),
                "status": STATUS_PENDING,
                "pending": True,
                "abnormal": False,
                "事件编号": event_code,
                "节点编号": node_code,
                "采集时间": raw.get("采集时间") or format_time(now_utc()),
                "租约秒数": raw.get("租约秒数", settings.lease_seconds_default),
                "槽位快照": raw.get("槽位快照"),
                "槽位写入失败": bool(raw.get("槽位写入失败")),
                "采集来源": str(raw.get("采集来源") or "节点上报"),
                "处理状态": STATUS_PENDING,
                "处理说明": "",
                "租约版本": None,
                "批次版本": None,
                "记录时间": format_time(now_utc()),
                "处理时间": None,
            }
            store.rows(MODULE).append(event)
            created.append(event)
    return created, errors


def _fail_event(event: dict[str, Any], status: str, message: str) -> dict[str, Any]:
    event["处理状态"] = status
    event["status"] = status
    event["pending"] = False
    event["abnormal"] = status == STATUS_REJECTED
    event["处理说明"] = message
    event["处理时间"] = format_time(now_utc())
    return event


def _block_on_slot_failure(
    node: dict[str, Any], event_time: Any, message: str
) -> tuple[dict[str, Any] | None, str]:
    """槽位快照失败是硬停止：即使报告迟到也必须让节点立刻停止接单。"""
    decision, record, note = node_lease.mark_offline(
        node,
        at=event_time,
        priority=node_lease.PRIORITY_SLOT_FAILURE,
        kind="槽位快照失败",
        reason=message,
        reason_code="slot_snapshot_failed",
        actor="system",
        force=True,
    )
    if decision is None:
        return None, note
    return record, note


def _apply_heartbeat(
    node: dict[str, Any],
    *,
    event_time: Any,
    lease_seconds: int,
    snapshot: list[dict[str, Any]] | None,
    event_code: str,
) -> tuple[str, str, int | None]:
    """返回 (处理状态, 说明, 延长后的租约版本)。"""
    current_expiry = parse_time(node.get("lease_expires_at"))
    if current_expiry is not None and event_time > current_expiry:
        # 租约已经走过边界：先落一条租约到期记录，再让这次心跳按新的租约上线。
        # 采集时间恰好等于到期时间视为连续存活，不额外制造一条到期记录。
        node_lease.mark_offline(
            node,
            at=current_expiry,
            priority=node_lease.PRIORITY_LEASE_EXPIRED,
            kind="租约到期",
            reason="租约到期未续约",
            reason_code="lease_expired",
            actor="system",
        )
        current_expiry = None

    last = node_lease.current_decision(node)
    if last is not None and last.get("来源") in BLOCKING_KINDS:
        # 人工离线与槽位快照失败是粘性的：必须由人工恢复在线后才能再续租，
        # 否则操作员的离线和失败现场会被一条补报心跳悄悄改写。
        return (
            STATUS_REJECTED,
            f"节点处于「{last.get('来源')}」状态，需人工恢复在线后才能续租",
            None,
        )
    last_key = node_lease.decision_key(last)
    if last is not None and event_time < last_key[0]:
        return STATUS_LATE, "心跳采集时间早于节点最近判定，未改写节点状态", None
    if last is not None and event_time == last_key[0] and last.get("来源") == "心跳":
        return STATUS_MERGED, "同一采集时间的并发心跳只延长一次租约", int(node.get("lease_epoch", 0) or 0)

    new_expiry = event_time + timedelta(seconds=lease_seconds)
    if current_expiry is not None and new_expiry <= current_expiry:
        return STATUS_MERGED, "租约未到期且新租约不晚于当前到期时间，本次只合并不续租", int(
            node.get("lease_epoch", 0) or 0
        )

    slots = snapshot if snapshot is not None else node.get("slots") or default_snapshot(
        node_lease.capacity_of(node)
    )
    digest = snapshot_digest(slots)
    epoch = int(node.get("lease_epoch", 0) or 0) + 1
    free_slots = sum(1 for slot in slots if slot.get("状态") == "空闲")
    decision, message = node_lease.mark_online(
        node,
        at=event_time,
        priority=node_lease.PRIORITY_HEARTBEAT,
        kind="心跳",
        reason=f"心跳事件 {event_code} 续租 {lease_seconds} 秒",
        actor="system",
        lease_epoch=epoch,
        lease_expires_at=new_expiry,
        free_slots=free_slots,
        slot_digest=digest,
    )
    if decision is None:
        return STATUS_LATE, message, None
    node["slots"] = slots
    node["slots_digest"] = digest
    node["slots_updated_at"] = format_time(event_time)
    node["last_heartbeat_at"] = format_time(event_time)
    node["上次心跳时间"] = format_time(event_time)
    return STATUS_DONE, f"租约已延长至 {format_time(new_expiry)}", epoch


def ingest(request_id: str | None = None) -> dict[str, Any]:
    """读取待处理心跳事件并生成结果；并发触发时后到者直接复用同一份结果。"""
    with DOMAIN_LOCK:
        pending = sorted(
            (row for row in store.rows(MODULE) if row.get("处理状态") == STATUS_PENDING),
            key=lambda row: (str(row.get("采集时间") or ""), int(row.get("id", 0))),
        )
        expired = node_lease.expire_due(now_utc())
        if not pending:
            # 没有新事件时不要凭空多造批次；但刚扫出的到期记录必须落到结果里。
            batch = (
                pipeline.refresh(trigger="租约到期扫描")
                if expired
                else schedulable.ensure_initial_batch()
            )
            return {
                "ok": True,
                "message": "没有待处理的心跳事件，复用最近一次可调度结果",
                "读取事件数": 0,
                "有效心跳数": 0,
                "延长租约数": 0,
                "合并心跳数": 0,
                "拒绝数": 0,
                "到期节点数": len(expired),
                "批次版本": batch.get("版本"),
                "重复提交": True,
                "批次": batch,
            }

        counters = {
            "读取事件数": len(pending),
            "有效心跳数": 0,
            "延长租约数": 0,
            "合并心跳数": 0,
            "拒绝数": 0,
            "到期节点数": len(expired),
        }
        changed = bool(expired)

        for event in pending:
            event_time, time_error = _parse_event_time(event.get("采集时间"))
            if time_error:
                _fail_event(event, STATUS_REJECTED, time_error)
                counters["拒绝数"] += 1
                continue
            lease_seconds, lease_error = _parse_lease(event.get("租约秒数"))
            if lease_error:
                _fail_event(event, STATUS_REJECTED, lease_error)
                counters["拒绝数"] += 1
                continue
            node = node_lease.find_node(event.get("节点编号"))
            if node is None:
                _fail_event(event, STATUS_REJECTED, "节点未登记，心跳未生效")
                counters["拒绝数"] += 1
                continue
            node_lease.ensure_initialized(node)
            if node.get("status") == "归档":
                _fail_event(event, STATUS_REJECTED, "节点已归档，心跳不再生效")
                counters["拒绝数"] += 1
                continue
            if node.get("status") == "草稿":
                _fail_event(event, STATUS_REJECTED, "节点尚未提交注册，心跳未生效")
                counters["拒绝数"] += 1
                continue

            if event.get("槽位写入失败"):
                _block_on_slot_failure(node, event_time, "槽位快照写入失败，节点已停止接单")
                _fail_event(event, STATUS_REJECTED, "槽位快照写入失败，节点已移出可调度集合")
                counters["拒绝数"] += 1
                changed = True
                continue

            snapshot, snapshot_error = validate_snapshot(
                node_lease.capacity_of(node), event.get("槽位快照")
            )
            if snapshot_error:
                record, _note = _block_on_slot_failure(node, event_time, snapshot_error)
                _fail_event(
                    event,
                    STATUS_REJECTED,
                    f"{snapshot_error}，节点已移出可调度集合"
                    + (f"，最终记录 #{record.get('id')}" if record else ""),
                )
                counters["拒绝数"] += 1
                changed = True
                continue

            status, message, epoch = _apply_heartbeat(
                node,
                event_time=event_time,
                lease_seconds=lease_seconds,
                snapshot=snapshot,
                event_code=str(event.get("事件编号")),
            )
            event["处理说明"] = message
            event["租约版本"] = epoch if epoch is not None else node.get("lease_epoch")
            event["处理时间"] = format_time(now_utc())
            if status in (STATUS_DONE, STATUS_MERGED):
                counters["有效心跳数"] += 1
                if status == STATUS_DONE:
                    counters["延长租约数"] += 1
                    changed = True
                else:
                    counters["合并心跳数"] += 1
                event["处理状态"] = status
                event["status"] = status
                event["pending"] = False
                event["abnormal"] = False
            elif status == STATUS_LATE:
                counters["合并心跳数"] += 0
                counters["拒绝数"] += 1
                _fail_event(event, STATUS_LATE, message)
            else:
                counters["拒绝数"] += 1
                _fail_event(event, STATUS_REJECTED, message)

        if not changed:
            batch = pipeline.latest_batch()
            counters["批次版本"] = batch.get("版本") if batch else None
            return {
                "ok": True,
                "message": "心跳事件未改变可调度状态，保留上一批次结果",
                "重复提交": False,
                "批次": batch,
                **counters,
            }

        batch = pipeline.refresh(trigger="心跳处理", request_id=request_id)
        for event in pending:
            if event.get("处理状态") == STATUS_PENDING:
                _fail_event(event, STATUS_REJECTED, "处理阶段未能应用该事件")
                counters["拒绝数"] += 1
            event["批次版本"] = batch.get("版本")
        counters["批次版本"] = batch.get("版本")
        counters["可调度节点数"] = batch.get("条目数")
        counters["批次"] = batch
        return {
            "ok": True,
            "message": f"已处理 {len(pending)} 条心跳事件，可调度节点 {batch.get('条目数')} 个",
            "重复提交": False,
            **counters,
        }


def sweep() -> dict[str, Any]:
    """主动扫描到期租约；返回新生成的离线最终记录。

    没有节点到期且已经有批次时，不新建版本，避免空扫描把批次历史撑满。
    """
    with DOMAIN_LOCK:
        expired = node_lease.expire_due(now_utc())
        batch = (
            pipeline.refresh(trigger="租约到期扫描")
            if expired
            else schedulable.ensure_initial_batch()
        )
        return {
            "ok": True,
            "message": f"扫描完成，{len(expired)} 个节点租约到期",
            "到期节点数": len(expired),
            "到期节点": expired,
            "批次版本": batch.get("版本"),
            "批次": batch,
        }
