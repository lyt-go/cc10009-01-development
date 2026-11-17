"""请求分流侧：读取可调度节点结果，写入目标域名的新增请求并维护分流清单。

规则：

* 分流侧只认可调度节点结果（同一份批次视图），不自己猜节点在线。
* 新请求先分配到一个在岗节点；分配结果是追加式的历史记录，后续心跳补报
  不会改写已经落下的分配行。
* 节点退出可调度集合时，指向它的未完成请求不会被静默丢弃：能转派就写一条
  转派记录，转派不了就留在分流侧标为待处理，并说明原因。
* 同一批次只同步一次；同一请求编号重复提交只生效一次。
"""
from __future__ import annotations

from typing import Any

from app.lock import DOMAIN_LOCK
from app.services import schedulable
from app.store import store
from app.timeutil import format_time, now_utc

ROSTER_MODULE = "_routing_roster"
ASSIGNMENT_MODULE = "_routing_assignments"
SYNC_MODULE = "_routing_syncs"
DISPATCH_MODULE = "dispatch"

STATE_ACTIVE = "在岗"
STATE_LEFT = "已退出"
DISPOSITION_ASSIGNED = "已分配"
DISPOSITION_TRANSFERRED = "已转派"
DISPOSITION_PENDING = "待处理"
DISPOSITION_EXITED = "原节点退出"


def domains() -> list[dict[str, Any]]:
    rows = store.rows(DISPATCH_MODULE)
    if not rows:
        return []
    return rows


def all_roster(domain: str | None = None) -> list[dict[str, Any]]:
    rows = store.rows(ROSTER_MODULE)
    if domain:
        rows = [row for row in rows if row.get("目标域名") == domain]
    return rows


def active_roster(domain: str | None = None) -> list[dict[str, Any]]:
    return [row for row in all_roster(domain) if row.get("状态") == STATE_ACTIVE]


def assignments(
    *,
    domain: str | None = None,
    node_code: str | None = None,
    page: int = 1,
    size: int = 20,
) -> tuple[list[dict[str, Any]], int]:
    rows = list(store.rows(ASSIGNMENT_MODULE))
    if domain:
        rows = [row for row in rows if row.get("目标域名") == domain]
    if node_code:
        rows = [row for row in rows if row.get("节点编号") == node_code]
    rows = sorted(rows, key=lambda row: int(row.get("id", 0)))
    total = len(rows)
    start = max(page - 1, 0) * size
    return rows[start:start + size], total


def sync_logs() -> list[dict[str, Any]]:
    return sorted(store.rows(SYNC_MODULE), key=lambda row: int(row.get("同步版本", 0)))


def _item_map(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(item.get("节点编号")): item for item in items}


def _upsert_roster(domain: str, item: dict[str, Any], batch_version: int) -> dict[str, Any]:
    for row in store.rows(ROSTER_MODULE):
        if row.get("目标域名") == domain and row.get("节点编号") == item.get("节点编号"):
            if row.get("状态") == STATE_LEFT:
                row["状态"] = STATE_ACTIVE
                row["重新加入时间"] = format_time(now_utc())
                row["退出时间"] = None
                row["退出原因"] = None
            row["空闲槽位"] = item.get("空闲槽位")
            row["并发槽位数"] = item.get("并发槽位数")
            row["源批次版本"] = batch_version
            return row
    row = {
        "id": store.next_id(ROSTER_MODULE),
        "目标域名": domain,
        "节点编号": item.get("节点编号"),
        "节点记录id": item.get("节点记录id"),
        "状态": STATE_ACTIVE,
        "并发槽位数": item.get("并发槽位数"),
        "空闲槽位": item.get("空闲槽位"),
        "源批次版本": batch_version,
        "加入时间": format_time(now_utc()),
        "重新加入时间": None,
        "退出时间": None,
        "退出原因": None,
    }
    store.rows(ROSTER_MODULE).append(row)
    return row


def _pending_load(node_code: str, domain: str) -> int:
    """分流侧已经压在某个节点上的未完成请求数（用于避免超卖槽位）。"""
    return sum(
        1
        for row in store.rows(ASSIGNMENT_MODULE)
        if str(row.get("节点编号")) == str(node_code)
        and row.get("目标域名") == domain
        and row.get("处置") == DISPOSITION_ASSIGNED
    )


def _loads(domain: str) -> dict[str, int]:
    """一次算好某域名下每个节点的未完成请求数，避免逐个节点扫表。"""
    loads: dict[str, int] = {}
    for row in store.rows(ASSIGNMENT_MODULE):
        if row.get("目标域名") != domain or row.get("处置") != DISPOSITION_ASSIGNED:
            continue
        code = str(row.get("节点编号"))
        loads[code] = loads.get(code, 0) + 1
    return loads


def _candidates(
    domain: str,
    items: list[dict[str, Any]],
    exclude: set[str] | None = None,
    loads: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    """可承接请求的节点：既要在可调度结果里，也要还有未被分流占用的空闲槽位。"""
    exclude = exclude or set()
    loads = _loads(domain) if loads is None else loads
    result = []
    for item in items:
        code = str(item.get("节点编号"))
        if code in exclude:
            continue
        free = int(item.get("空闲槽位", 0) or 0) - int(loads.get(code, 0))
        if free <= 0:
            continue
        result.append({**item, "可承接槽位": free})
    return sorted(result, key=lambda item: (-int(item["可承接槽位"]), str(item.get("节点编号"))))


def _pick_replacement(domain: str, exclude: set[str]) -> dict[str, Any] | None:
    """替代节点直接看当前可调度结果，而不是清单里记录的旧槽位数。"""
    items = list(schedulable.merged_view().get("条目") or [])
    candidates = _candidates(domain, items, exclude)
    if not candidates:
        return None
    chosen = candidates[0]
    return {
        "节点编号": chosen.get("节点编号"),
        "节点记录id": chosen.get("节点记录id"),
        "源批次版本": chosen.get("源批次版本"),
    }


def _open_assignments(domain: str, node_code: str) -> list[dict[str, Any]]:
    return [
        row for row in store.rows(ASSIGNMENT_MODULE)
        if row.get("目标域名") == domain
        and str(row.get("节点编号")) == str(node_code)
        and row.get("处置") == DISPOSITION_ASSIGNED
    ]


def _reassign(assignment: dict[str, Any], *, reason: str) -> str:
    """为一条未完成请求留下后续记录；原始分配行只改处置，不改节点指向。

    返回后续处置：能承接就新开一条 ``已分配`` 记录，承接不了就新开一条
    ``待处理`` 记录，两种情况下原始分配行都保留节点编号与分配时间，方便追溯。
    """
    original_code = str(assignment.get("节点编号"))
    domain = str(assignment.get("目标域名"))
    replacement = _pick_replacement(domain, {original_code})
    assignment["处置"] = DISPOSITION_EXITED
    assignment["说明"] = f"{reason}；原分配保留，后续记录见下一条"
    assignment["更新时间"] = format_time(now_utc())
    if replacement is None:
        store.rows(ASSIGNMENT_MODULE).append({
            "id": store.next_id(ASSIGNMENT_MODULE),
            "请求编号": assignment.get("请求编号"),
            "目标域名": domain,
            "目标URL": assignment.get("目标URL"),
            "节点编号": None,
            "节点记录id": None,
            "源批次版本": assignment.get("源批次版本"),
            "前序分配": assignment.get("id"),
            "转派次数": int(assignment.get("转派次数", 0) or 0) + 1,
            "处置": DISPOSITION_PENDING,
            "说明": f"{reason}；当前没有可调度替代节点，请求保留在分流侧待处理",
            "分配时间": format_time(now_utc()),
            "更新时间": format_time(now_utc()),
        })
        return DISPOSITION_PENDING
    store.rows(ASSIGNMENT_MODULE).append({
        "id": store.next_id(ASSIGNMENT_MODULE),
        "请求编号": assignment.get("请求编号"),
        "目标域名": assignment.get("目标域名"),
        "目标URL": assignment.get("目标URL"),
        "节点编号": replacement.get("节点编号"),
        "节点记录id": replacement.get("节点记录id"),
        "源批次版本": replacement.get("源批次版本") or assignment.get("源批次版本"),
        "前序分配": assignment.get("id"),
        "转派次数": int(assignment.get("转派次数", 0) or 0) + 1,
        "处置": DISPOSITION_ASSIGNED,
        "说明": f"承接 {assignment.get('请求编号')}，原节点退出",
        "分配时间": format_time(now_utc()),
        "更新时间": format_time(now_utc()),
    })
    return DISPOSITION_TRANSFERRED


def _refresh_dispatch_rows(batch: dict[str, Any]) -> int:
    items = list(batch.get("条目") or [])
    item_by_code = _item_map(items)
    changed = 0
    for row in store.rows(DISPATCH_MODULE):
        domain = str(row.get("目标域名") or "")
        roster = active_roster(domain)
        roster_codes = [str(entry.get("节点编号")) for entry in roster]
        online = [code for code in roster_codes if code in item_by_code]
        total_free = sum(
            int(item_by_code[code].get("空闲槽位", 0) or 0) for code in online
        )
        snapshot = {
            "可调度节点数": len(online),
            "可调度节点": online,
            "可用槽位": total_free,
            "批次版本": batch.get("版本"),
            "内容摘要": batch.get("内容摘要"),
            "同步时间": format_time(now_utc()),
        }
        before = row.get("可调度节点结果")
        row["可调度节点结果"] = snapshot
        row["可调度节点数"] = snapshot["可调度节点数"]
        row["可调度节点"] = online
        row["可用槽位"] = total_free
        row["批次版本"] = snapshot["批次版本"]
        row["最近同步时间"] = snapshot["同步时间"]
        if before != snapshot:
            changed += 1
    return changed


def sync(batch: dict[str, Any] | None = None) -> dict[str, Any]:
    """用一个可调度批次刷新分流侧清单；同一批次版本只做一次。"""
    with DOMAIN_LOCK:
        if batch is None:
            batch = schedulable.merged_view()
        batch_version = int(batch.get("版本") or 0)
        for log in store.rows(SYNC_MODULE):
            if int(log.get("批次版本", 0)) == batch_version:
                log["重复触发"] = int(log.get("重复触发", 0) or 0) + 1
                return log
        items = list(batch.get("条目") or [])
        item_codes = set(_item_map(items))
        exited: list[dict[str, Any]] = []
        reassigned = 0
        pending = 0
        domain_rows = domains()
        if not domain_rows:
            return {
                "id": None,
                "同步版本": len(sync_logs()),
                "批次版本": batch_version,
                "变更数": 0,
                "说明": "请求分流侧还没有目标域名，未生成清单",
            }
        for domain_row in domain_rows:
            domain = str(domain_row.get("目标域名") or "")
            for roster_row in all_roster(domain):
                if roster_row.get("状态") != STATE_ACTIVE:
                    continue
                code = str(roster_row.get("节点编号"))
                if code in item_codes:
                    continue
                roster_row["状态"] = STATE_LEFT
                roster_row["退出时间"] = format_time(now_utc())
                roster_row["退出原因"] = "节点退出可调度集合"
                exited.append({"目标域名": domain, "节点编号": code})
                for assignment in _open_assignments(domain, code):
                    disposition = _reassign(assignment, reason="原节点退出可调度集合")
                    if disposition == DISPOSITION_PENDING:
                        pending += 1
                    else:
                        reassigned += 1
            for item in items:
                _upsert_roster(domain, item, batch_version)
        changed_rows = _refresh_dispatch_rows(batch)
        log = {
            "id": store.next_id(SYNC_MODULE),
            "同步版本": len(sync_logs()) + 1,
            "批次版本": batch_version,
            "批次数": batch.get("批次数"),
            "内容摘要": batch.get("内容摘要"),
            "可调度节点数": batch.get("条目数"),
            "目标域名数": len(domain_rows),
            "退出节点": exited,
            "转派请求数": reassigned,
            "待处理请求数": pending,
            "更新清单行数": changed_rows,
            "触发": batch.get("触发"),
            "同步时间": format_time(now_utc()),
            "重复触发": 0,
        }
        store.rows(SYNC_MODULE).append(log)
        return log


def submit_requests(values: dict[str, Any]) -> dict[str, Any]:
    """把目标域名的新增请求写入请求分流侧结果。"""
    with DOMAIN_LOCK:
        raw_requests = values.get("requests")
        if raw_requests is None:
            raw_requests = [values]
        if not isinstance(raw_requests, list):
            return {"ok": False, "message": "requests 必须是请求列表", "已写入": 0, "重复": 0}
        view = schedulable.merged_view()
        items = list(view.get("条目") or [])
        written: list[dict[str, Any]] = []
        duplicates = 0
        unassigned = 0
        loads: dict[str, dict[str, int]] = {}
        for raw in raw_requests:
            if not isinstance(raw, dict):
                continue
            domain = str(raw.get("目标域名") or "").strip()
            if not domain:
                continue
            request_code = str(raw.get("请求编号") or "").strip()
            if not request_code:
                request_code = f"REQ-{store.next_sequence():08d}"
            existing = next(
                (row for row in store.rows(ASSIGNMENT_MODULE) if row.get("请求编号") == request_code),
                None,
            )
            if existing is not None:
                duplicates += 1
                continue
            roster = active_roster(domain)
            if not roster:
                # 目标域名第一次出现：按可调度结果补一份在岗清单再挑节点。
                for item in items:
                    _upsert_roster(domain, item, int(view.get("当前版本") or 0))
                roster = active_roster(domain)
            domain_loads = loads.setdefault(domain, _loads(domain))
            candidates = _candidates(domain, items, loads=domain_loads)
            if candidates:
                chosen = candidates[0]
                row = {
                    "id": store.next_id(ASSIGNMENT_MODULE),
                    "请求编号": request_code,
                    "目标域名": domain,
                    "目标URL": raw.get("目标URL"),
                    "节点编号": chosen.get("节点编号"),
                    "节点记录id": chosen.get("节点记录id"),
                    "源批次版本": view.get("当前版本"),
                    "前序分配": None,
                    "转派次数": 0,
                    "处置": DISPOSITION_ASSIGNED,
                    "说明": raw.get("说明") or "按可调度节点结果分配",
                    "租约到期时间": chosen.get("租约到期时间"),
                    "分配时间": format_time(now_utc()),
                    "更新时间": format_time(now_utc()),
                }
                store.rows(ASSIGNMENT_MODULE).append(row)
                written.append(row)
                code = str(row["节点编号"])
                domain_loads[code] = int(domain_loads.get(code, 0)) + 1
            else:
                unassigned += 1
                row = {
                    "id": store.next_id(ASSIGNMENT_MODULE),
                    "请求编号": request_code,
                    "目标域名": domain,
                    "目标URL": raw.get("目标URL"),
                    "节点编号": None,
                    "节点记录id": None,
                    "源批次版本": view.get("当前版本"),
                    "前序分配": None,
                    "转派次数": 0,
                    "处置": DISPOSITION_PENDING,
                    "说明": "当前没有可调度节点承接该请求，保留在分流侧待处理",
                    "租约到期时间": None,
                    "分配时间": format_time(now_utc()),
                    "更新时间": format_time(now_utc()),
                }
                store.rows(ASSIGNMENT_MODULE).append(row)
                written.append(row)
        _refresh_dispatch_rows_by_view(view)
        return {
            "ok": True,
            "message": f"已写入 {len(written)} 条请求，重复 {duplicates} 条，待处理 {unassigned} 条",
            "已写入": len(written),
            "重复": duplicates,
            "待处理": unassigned,
            "批次版本": view.get("当前版本"),
            "条目": written,
        }


def _refresh_dispatch_rows_by_view(view: dict[str, Any]) -> None:
    batch = {
        "版本": view.get("当前版本"),
        "内容摘要": view.get("内容摘要"),
        "条目": view.get("条目"),
    }
    _refresh_dispatch_rows(batch)


def verify() -> dict[str, Any]:
    """校验分流侧与可调度结果的一致性：在岗节点必须来自当前可调度条目。"""
    view = schedulable.merged_view()
    codes = {str(item.get("节点编号")) for item in view.get("条目") or []}
    checks: list[dict[str, Any]] = []
    stale = [
        row for row in store.rows(ROSTER_MODULE)
        if row.get("状态") == STATE_ACTIVE and str(row.get("节点编号")) not in codes
    ]
    checks.append({
        "校验项": "在岗清单来自可调度结果",
        "通过": not stale,
        "详情": "、".join(f"{row.get('目标域名')}:{row.get('节点编号')}" for row in stale[:5]),
    })
    dangling = [
        row for row in store.rows(ASSIGNMENT_MODULE)
        if row.get("处置") == DISPOSITION_ASSIGNED
        and str(row.get("节点编号")) not in codes
    ]
    checks.append({
        "校验项": "在途请求指向可调度节点",
        "通过": not dangling,
        "详情": "、".join(str(row.get("请求编号")) for row in dangling[:5]),
    })
    domainless = [row for row in store.rows(ASSIGNMENT_MODULE) if not row.get("目标域名")]
    checks.append({
        "校验项": "请求记录保留目标域名",
        "通过": not domainless,
        "详情": f"{len(domainless)} 条记录缺少目标域名",
    })
    return {
        "ok": all(item["通过"] for item in checks),
        "批次版本": view.get("当前版本"),
        "在岗节点数": len(active_roster()),
        "请求记录数": len(store.rows(ASSIGNMENT_MODULE)),
        "校验项": checks,
    }
