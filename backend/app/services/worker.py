"""采集节点业务规则：状态流转、租约判定、槽位与筛选口径都收在这里。

节点的 "在线/离线" 只有一个来源：``node_lease.decisions`` 判定链。
人工动作与心跳写的是同一份记录，所以登记、人工切换、离线恢复、归档的顺序
可以从同一条节点记录的 ``history`` 与 ``decisions`` 完整解释。
"""
from __future__ import annotations

from typing import Any

from app.lock import DOMAIN_LOCK
from app.services import node_lease, pipeline
from app.services.slots import default_snapshot, snapshot_digest
from app.store import store
from app.timeutil import format_time, now_utc, to_int

MODULE = "worker"
REQUIRED_FIELDS = ["节点编号", "主机地址", "并发槽位数"]
STATUS_ORDER = ["草稿", "注册中", "在线", "离线", "归档"]
ACTION_RULES = {
    "提交注册": "注册中",
    "标记在线": "在线",
    "标记离线": "离线",
    "恢复在线": "在线",
    "归档": "归档",
}
# 合法的人工流转：草稿 -> 注册中 -> 在线/离线；离线 -> 在线/归档。
ALLOWED_TRANSITIONS = {
    "草稿": {"提交注册"},
    "注册中": {"提交注册", "标记在线", "标记离线"},
    "在线": {"标记在线", "标记离线"},
    # 已离线的节点允许再次确认离线（用于区分租约到期与人工离线的最终记录），
    # 也可以人工恢复或归档。
    "离线": {"标记离线", "恢复在线", "归档"},
    "归档": set(),
}


class WorkerService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        with DOMAIN_LOCK:
            # 惰性到期：读节点清单时先把已过期的租约落成离线判定，
            # 保证清单里的 status 与在线判断只给出一个结论。
            node_lease.expire_due(now_utc())
            rows = []
            for row in store.rows(MODULE):
                node_lease.ensure_initialized(row)
                rows.append({
                    **row,
                    "在线判断": node_lease.online_state(row),
                    "可调度": node_lease.is_schedulable(row)[0],
                    "空闲槽位": node_lease.free_slots_of(row),
                })
            if keyword:
                rows = [
                    row for row in rows
                    if keyword in str(row.get("节点编号", ""))
                    or keyword in str(row.get("主机地址", ""))
                ]
            if status:
                rows = [
                    row for row in rows
                    if row.get("status") == status or row.get("在线判断") == status
                ]
            total = len(rows)
            start = max(page - 1, 0) * size
            return rows[start:start + size], total

    def stats(self) -> dict[str, Any]:
        with DOMAIN_LOCK:
            node_lease.expire_due(now_utc())
            rows = [node_lease.ensure_initialized(row) for row in store.rows(MODULE)]
            online = [row for row in rows if node_lease.online_state(row) == "在线"]
            schedulable = [row for row in rows if node_lease.is_schedulable(row)[0]]
            offline = [
                row for row in rows
                if row.get("status") not in ("草稿", "归档")
                and node_lease.online_state(row) != "在线"
            ]
            return {
                "工作节点总数": len(rows),
                "在线节点数": len(online),
                "可调度节点数": len(schedulable),
                "离线节点数": len(offline),
                "归档节点数": sum(1 for row in rows if row.get("status") == "归档"),
                "空闲槽位总数": sum(node_lease.free_slots_of(row) for row in schedulable),
            }

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        with DOMAIN_LOCK:
            node_lease.expire_due(now_utc())
            node = store.find(MODULE, entry_id)
            if node is None:
                return None
            node_lease.ensure_initialized(node)
            online, reason = node_lease.is_schedulable(node)
            record = {
                **node,
                "在线判断": node_lease.online_state(node),
                "可调度": online,
                "不可调度原因": reason,
                "空闲槽位": node_lease.free_slots_of(node),
                "判定记录": list(node.get("decisions") or []),
                "状态历史": list(node.get("history") or []),
            }
            return record

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        capacity = to_int(values.get("并发槽位数"), None)
        if capacity is None or capacity <= 0:
            return ["并发槽位数"]
        capacity = min(capacity, settings_capacity_max())
        with DOMAIN_LOCK:
            rows = store.rows(MODULE)
            node_code = str(values.get("节点编号")).strip()
            if any(str(row.get("节点编号")) == node_code for row in rows):
                return None, [f"节点编号 {node_code} 已存在"]
            entry = {
                "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
                "status": "草稿",
                "pending": True,
                "abnormal": False,
                "节点编号": node_code,
                "主机地址": values.get("主机地址"),
                "并发槽位数": str(capacity),
                "capacity": capacity,
                "地域标签": values.get("地域标签") or "未标注",
                "CPU使用率": values.get("CPU使用率") or "0%",
                "内存可用MB": values.get("内存可用MB") or "0",
                "上次心跳时间": None,
                "节点状态标签": "未注册",
                "slots": default_snapshot(capacity),
                "slots_digest": snapshot_digest(default_snapshot(capacity)),
                "slots_updated_at": None,
                "lease_epoch": 0,
                "lease_expires_at": None,
                "last_heartbeat_at": None,
                "blocked_reason": None,
                "decisions": [],
                "history": [{
                    "序号": store.next_sequence(),
                    "时间": format_time(now_utc()),
                    "原状态": None,
                    "新状态": "草稿",
                    "动作": "登记",
                    "原因": "工作节点登记",
                    "操作人": str(values.get("操作人") or "值班管理员"),
                }],
                "archived_at": None,
            }
            rows.append(entry)
            return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        with DOMAIN_LOCK:
            node = store.find(MODULE, entry_id)
            if node is None:
                return None, f"工作节点 {entry_id} 不存在或已归档"
            node_lease.ensure_initialized(node)
            if action not in ACTION_RULES:
                return None, f"动作「{action}」不属于采集节点可执行范围"
            status = str(node.get("status"))
            if status == "归档":
                return None, "节点已归档，不能再执行状态动作"
            if action not in ALLOWED_TRANSITIONS.get(status, set()):
                return None, f"当前状态「{status}」不允许执行「{action}」"
            actor = "值班管理员"
            if action == "提交注册":
                node["status"] = "注册中"
                node["pending"] = True
                node["abnormal"] = False
                node["节点状态标签"] = "注册中"
                node.setdefault("history", []).append({
                    "序号": store.next_sequence(),
                    "时间": format_time(now_utc()),
                    "原状态": status,
                    "新状态": "注册中",
                    "动作": action,
                    "原因": "人工提交注册，等待心跳确认",
                    "操作人": actor,
                })
                return node, "工作节点已提交注册"
            if action == "标记在线":
                decision, message = node_lease.mark_online(
                    node,
                    at=now_utc(),
                    priority=node_lease.PRIORITY_MANUAL_ONLINE,
                    kind="人工上线",
                    reason="人工切换为在线，等待心跳授予租约",
                    actor=actor,
                    lease_epoch=int(node.get("lease_epoch", 0) or 0),
                    lease_expires_at=None,
                    free_slots=node_lease.free_slots_of(node),
                )
                if decision is None:
                    return None, message
                node["pending"] = False
                return node, "工作节点已标记在线，需心跳续租后才进入可调度结果"
            if action == "恢复在线":
                decision, message = node_lease.mark_online(
                    node,
                    at=now_utc(),
                    priority=node_lease.PRIORITY_MANUAL_ONLINE,
                    kind="离线恢复",
                    reason="人工从离线恢复为在线，等待心跳授予租约",
                    actor=actor,
                    lease_epoch=int(node.get("lease_epoch", 0) or 0),
                    lease_expires_at=None,
                    free_slots=node_lease.free_slots_of(node),
                )
                if decision is None:
                    return None, message
                node["pending"] = False
                pipeline.refresh(trigger="节点离线恢复")
                return node, "工作节点已恢复在线"
            if action == "标记离线":
                decision, record, message = node_lease.mark_offline(
                    node,
                    at=now_utc(),
                    priority=node_lease.PRIORITY_MANUAL_OFFLINE,
                    kind="人工离线",
                    reason="人工标记离线",
                    reason_code="manual_offline",
                    actor=actor,
                    force=True,
                )
                if decision is None:
                    return None, message
                pipeline.refresh(trigger="节点人工离线")
                node["最终记录"] = record
                return node, message
            if action == "归档":
                if node.get("status") != "离线":
                    return None, "节点需先处于离线状态才能归档"
                node["status"] = "归档"
                node["pending"] = False
                node["abnormal"] = False
                node["节点状态标签"] = "已归档"
                node["archived_at"] = format_time(now_utc())
                node.setdefault("history", []).append({
                    "序号": store.next_sequence(),
                    "时间": format_time(now_utc()),
                    "原状态": status,
                    "新状态": "归档",
                    "动作": action,
                    "原因": "节点退出调度并归档",
                    "操作人": actor,
                })
                pipeline.refresh(trigger="节点归档")
                return node, "工作节点已归档"
            return None, f"动作「{action}」未实现"


def settings_capacity_max() -> int:
    from app.config import settings

    return int(settings.worker_capacity_max)


def final_records(
    *,
    reason_code: str | None = None,
    node_code: str | None = None,
    page: int = 1,
    size: int = 20,
) -> tuple[list[dict[str, Any]], int]:
    with DOMAIN_LOCK:
        rows = list(store.rows(node_lease.FINAL_MODULE))
        if reason_code:
            rows = [row for row in rows if row.get("原因码") == reason_code]
        if node_code:
            rows = [row for row in rows if row.get("节点编号") == node_code]
        rows = sorted(rows, key=lambda row: int(row.get("id", 0)))
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total
