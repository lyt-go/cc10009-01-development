"""节点生命周期与租约判定：所有在线/离线判断的唯一来源。

关键约定：

* 每个节点维护一份追加式的 ``decisions``（判定记录）。在线判断只从这个列表
  推导，因此节点清单不会同时出现两种在线结论。
* 判定按 ``(判定时间, 优先级, 判定序号)`` 排序。时间相同的时候，优先级高的
  判定获胜：人工离线 > 槽位快照失败 > 租约到期 > 人工上线 > 心跳。这样
  "心跳、离线动作与租约到期同时到达" 有一个确定的答案，重复补报也不会改写。
* 租约到期按 "租约到期时间 <= 判定时间" 生效；心跳在自己的采集时间已经
  到达或超过租约到期时间时，一律先让租约到期，心跳不再续约。
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from app.store import store
from app.timeutil import format_time, now_utc, parse_time, to_int

MODULE = "worker"
FINAL_MODULE = "_node_final_records"

PRIORITY_HEARTBEAT = 10
PRIORITY_MANUAL_ONLINE = 20
PRIORITY_LEASE_EXPIRED = 30
PRIORITY_MANUAL_OFFLINE = 40
PRIORITY_SLOT_FAILURE = 50

ONLINE = "在线"
OFFLINE = "离线"

REASON_CODES = {
    "manual_offline": "人工离线",
    "lease_expired": "租约到期",
    "slot_snapshot_failed": "槽位快照写入失败",
}

_EPOCH = parse_time("1970-01-01T00:00:00+00:00")
_TICK = timedelta(microseconds=1)


def decision_key(decision: dict[str, Any] | None) -> tuple[Any, int, int]:
    if not decision:
        return (_EPOCH, 0, 0)
    return (
        parse_time(decision.get("判定时间"), _EPOCH),
        int(decision.get("判定优先级", 0) or 0),
        int(decision.get("判定序号", 0) or 0),
    )


def capacity_of(node: dict[str, Any]) -> int:
    return max(1, to_int(node.get("capacity", node.get("并发槽位数")), 1) or 1)


def ensure_initialized(node: dict[str, Any]) -> dict[str, Any]:
    """给老记录补齐租约、槽位与判定字段，保持向后兼容。"""
    if "capacity" not in node:
        node["capacity"] = capacity_of(node)
    if "slots" not in node or not isinstance(node.get("slots"), list):
        node["slots"] = [
            {"槽位": index, "状态": "空闲"} for index in range(capacity_of(node))
        ]
    if "slots_digest" not in node:
        from app.services.slots import snapshot_digest

        node["slots_digest"] = snapshot_digest(node["slots"])
    node.setdefault("slots_updated_at", None)
    node.setdefault("lease_epoch", 0)
    node.setdefault("lease_expires_at", None)
    node.setdefault("last_heartbeat_at", None)
    node.setdefault("blocked_reason", None)
    node.setdefault("decisions", [])
    node.setdefault("history", [])
    node.setdefault("archived_at", None)
    if not node["history"]:
        node["history"].append({
            "序号": store.next_sequence(),
            "时间": format_time(now_utc()),
            "原状态": None,
            "新状态": node.get("status"),
            "动作": "示例数据载入",
            "原因": "初始化节点记录",
            "操作人": "system",
        })
    return node


def find_node(node_code: str | int | None) -> dict[str, Any] | None:
    if node_code is None:
        return None
    text = str(node_code).strip()
    for row in store.rows(MODULE):
        if str(row.get("节点编号")) == text:
            return row
    numeric = to_int(text)
    if numeric is not None:
        return store.find(MODULE, numeric)
    return None


def current_decision(node: dict[str, Any]) -> dict[str, Any] | None:
    decisions = node.get("decisions") or []
    return decisions[-1] if decisions else None


def decision_as_of(node: dict[str, Any], at: Any) -> dict[str, Any] | None:
    boundary = parse_time(at, _EPOCH)
    chosen = None
    for decision in node.get("decisions") or []:
        if decision_key(decision)[0] <= boundary and (
            chosen is None or decision_key(decision) > decision_key(chosen)
        ):
            chosen = decision
    return chosen


def is_newer(node: dict[str, Any], at: Any, priority: int) -> bool:
    candidate = parse_time(at, _EPOCH)
    last = current_decision(node)
    last_at, last_priority, _ = decision_key(last)
    return (candidate, priority) > (last_at, last_priority)


def free_slots_of(node: dict[str, Any]) -> int:
    return sum(1 for item in node.get("slots") or [] if item.get("状态") == "空闲")


def _append_history(
    node: dict[str, Any],
    *,
    at: Any,
    from_status: str | None,
    to_status: str | None,
    action: str,
    reason: str,
    actor: str,
) -> None:
    node.setdefault("history", []).append({
        "序号": store.next_sequence(),
        "时间": format_time(parse_time(at, now_utc())),
        "原状态": from_status,
        "新状态": to_status,
        "动作": action,
        "原因": reason,
        "操作人": actor,
    })


def append_final_record(
    node: dict[str, Any],
    decision: dict[str, Any],
    *,
    reason_code: str,
) -> dict[str, Any]:
    """离线最终记录只追加不覆盖，原因码区分租约到期/人工离线/槽位失败。"""
    rows = store.rows(FINAL_MODULE)
    for row in rows:
        if (
            int(row.get("节点记录id", -1)) == int(node.get("id", -1))
            and int(row.get("判定序号", -1)) == int(decision.get("判定序号", -2))
        ):
            return row
    record = {
        "id": store.next_id(FINAL_MODULE),
        "节点编号": node.get("节点编号"),
        "节点记录id": node.get("id"),
        "原因码": reason_code,
        "原因": REASON_CODES.get(reason_code, decision.get("原因")),
        "判定时间": decision.get("判定时间"),
        "判定序号": decision.get("判定序号"),
        "租约版本": decision.get("租约版本"),
        "租约到期时间": decision.get("租约到期时间"),
        "记录时间": format_time(now_utc()),
    }
    rows.append(record)
    return record


def record_decision(
    node: dict[str, Any],
    *,
    at: Any,
    priority: int,
    result: str,
    kind: str,
    reason: str,
    actor: str = "system",
    lease_epoch: int | None = None,
    lease_expires_at: Any = None,
    free_slots: int | None = None,
    slot_digest: str | None = None,
) -> dict[str, Any]:
    """应用一条判定并写入节点记录；调用方必须已经持有领域锁。"""
    ensure_initialized(node)
    decision = {
        "判定时间": format_time(parse_time(at, now_utc())),
        "判定优先级": priority,
        "判定序号": store.next_sequence(),
        "结果": result,
        "来源": kind,
        "原因": reason,
        "操作人": actor,
        "租约版本": int(lease_epoch if lease_epoch is not None else node.get("lease_epoch", 0) or 0),
        "租约到期时间": format_time(parse_time(lease_expires_at)) if lease_expires_at else None,
        "并发槽位数": capacity_of(node),
        "空闲槽位": free_slots if free_slots is not None else free_slots_of(node),
        "槽位快照摘要": slot_digest if slot_digest is not None else node.get("slots_digest"),
    }
    previous_status = node.get("status")
    node.setdefault("decisions", []).append(decision)
    node["status"] = ONLINE if result == ONLINE else OFFLINE
    node["pending"] = node["status"] != OFFLINE
    node["abnormal"] = node["status"] == OFFLINE
    node["lease_epoch"] = decision["租约版本"]
    node["lease_expires_at"] = decision["租约到期时间"]
    node["blocked_reason"] = None if result == ONLINE else kind
    node["节点状态标签"] = _status_label(node)
    if result == ONLINE:
        node["archived_at"] = None
    if previous_status != node["status"]:
        _append_history(
            node,
            at=decision["判定时间"],
            from_status=previous_status,
            to_status=node["status"],
            action=kind,
            reason=reason,
            actor=actor,
        )
    return decision


def _status_label(node: dict[str, Any]) -> str:
    status = node.get("status")
    if status == "归档":
        return "已归档"
    if status == "草稿":
        return "未注册"
    if status == "注册中":
        return "注册中"
    decision = current_decision(node)
    if not decision or decision.get("结果") != ONLINE:
        return "离线"
    expires = parse_time(node.get("lease_expires_at"))
    if expires is None:
        return "在线（无有效租约）"
    return "可调度"


def mark_online(
    node: dict[str, Any],
    *,
    at: Any,
    priority: int,
    kind: str,
    reason: str,
    actor: str,
    lease_epoch: int | None = None,
    lease_expires_at: Any = None,
    free_slots: int | None = None,
    slot_digest: str | None = None,
) -> tuple[dict[str, Any] | None, str]:
    if node.get("status") == "归档":
        return None, "节点已归档，不能重新上线"
    if not is_newer(node, at, priority):
        return None, "判定时间早于节点最近一次判定，本次心跳/动作不改写节点状态"
    decision = record_decision(
        node,
        at=at,
        priority=priority,
        result=ONLINE,
        kind=kind,
        reason=reason,
        actor=actor,
        lease_epoch=lease_epoch,
        lease_expires_at=lease_expires_at,
        free_slots=free_slots,
        slot_digest=slot_digest,
    )
    return decision, "节点在线判定已更新"


def mark_offline(
    node: dict[str, Any],
    *,
    at: Any,
    priority: int,
    kind: str,
    reason: str,
    reason_code: str,
    actor: str,
    force: bool = False,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
    """记录一条离线判定。

    ``force`` 用于人工动作：操作员刚点下的按钮不应该因为节点上已经有一条
    "租约到期"（时间是到期时刻）而被丢掉。此时把判定时间顺延到最后一条判定
    之后 1 微秒，既保留两条最终记录，也保持判定链单调。
    """
    if node.get("status") == "归档":
        return None, None, "节点已归档，不能执行离线判定"
    moment = parse_time(at, _EPOCH)
    last = current_decision(node)
    if force and last is not None and moment <= decision_key(last)[0]:
        moment = decision_key(last)[0] + _TICK
    if not force and not is_newer(node, moment, priority):
        return None, None, "判定时间早于节点最近一次判定，本次离线动作未生效"
    decision = record_decision(
        node,
        at=moment,
        priority=priority,
        result=OFFLINE,
        kind=kind,
        reason=reason,
        actor=actor,
        lease_epoch=int(node.get("lease_epoch", 0) or 0),
        lease_expires_at=None,
        free_slots=free_slots_of(node),
    )
    record = append_final_record(node, decision, reason_code=reason_code)
    return decision, record, f"节点已离线：{REASON_CODES.get(reason_code, reason)}"


def expire_due(cutoff: Any) -> list[dict[str, Any]]:
    """把截止时间之前到期的租约落成离线判定，返回本次到期的节点。"""
    boundary = parse_time(cutoff, now_utc())
    expired: list[dict[str, Any]] = []
    for node in list(store.rows(MODULE)):
        ensure_initialized(node)
        if node.get("status") == "归档":
            continue
        expires = parse_time(node.get("lease_expires_at"))
        if expires is None or expires > boundary:
            continue
        decision, record, message = mark_offline(
            node,
            at=expires,
            priority=PRIORITY_LEASE_EXPIRED,
            kind="租约到期",
            reason="租约到期未续约",
            reason_code="lease_expired",
            actor="system",
        )
        if decision is None:
            # 已有更新的判定（例如人工离线）覆盖了这次到期，只清理租约字段。
            node["lease_expires_at"] = None
            if not node.get("blocked_reason"):
                node["blocked_reason"] = "租约已到期"
            continue
        expired.append({
            "节点编号": node.get("节点编号"),
            "节点记录id": node.get("id"),
            "租约到期时间": format_time(expires),
            "判定时间": decision["判定时间"],
            "判定序号": decision["判定序号"],
            "最终记录": record,
        })
    return expired


def online_state(node: dict[str, Any], at: Any | None = None) -> str:
    """当前在线判断。

    始终从 ``decisions`` 判定链按时刻回看，只有一个结论：节点清单、可调度结果
    和批次边界都读同一份判定，不会出现两套在线/离线判断。
    """
    ensure_initialized(node)
    moment = parse_time(at, now_utc())
    if node.get("status") == "归档":
        return "已归档"
    if node.get("status") == "草稿":
        return "未注册"
    decision = decision_as_of(node, moment)
    if decision is None:
        return "离线" if node.get("status") == "离线" else "无心跳记录"
    if decision.get("结果") != ONLINE:
        return "离线"
    expires = parse_time(decision.get("租约到期时间"))
    if expires is None:
        return "在线（无有效租约）"
    if expires <= moment:
        return "租约已到期"
    return "在线"


def is_schedulable(node: dict[str, Any], at: Any | None = None) -> tuple[bool, str]:
    ensure_initialized(node)
    moment = parse_time(at, now_utc())
    if node.get("status") == "归档":
        return False, "节点已归档"
    decision = decision_as_of(node, moment)
    if decision is None:
        return False, "无心跳判定记录"
    if decision.get("结果") != ONLINE:
        return False, str(decision.get("原因") or "节点离线")
    expires = parse_time(decision.get("租约到期时间"))
    if expires is None:
        return False, "无有效租约"
    if expires <= moment:
        return False, "租约已到期"
    if int(decision.get("空闲槽位", 0) or 0) <= 0:
        return False, "无空闲并发槽位"
    return True, "可调度"
