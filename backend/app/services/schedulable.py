"""可调度节点结果数据集：批次生成、两版本合并与完整性校验。

数据集本身是只读快照：每次生成都会得到一个递增版本，旧版本原样保留。
"读"默认返回最近两个批次的合并视图——遇到批次边界上的在线判断冲突时，
按节点判定链的确定顺序裁决（判定时间、优先级、判定序号），因此同一个节点
不会在结果里出现两种在线结论；冲突会单独列在 ``边界冲突`` 里便于追查。
"""
from __future__ import annotations

from typing import Any

from app.lock import DOMAIN_LOCK
from app.services import node_lease
from app.services.slots import items_digest
from app.store import store
from app.timeutil import format_time, now_utc, parse_time, to_int

BATCH_MODULE = "_schedulable_batches"
META_MODULE = "_schedulable_meta"
INGEST_MODULE = "_ingest_requests"

REQUIRED_ITEM_FIELDS = (
    "节点编号",
    "节点记录id",
    "并发槽位数",
    "空闲槽位",
    "租约版本",
    "租约到期时间",
    "判定序号",
    "槽位快照摘要",
)


def _meta() -> dict[str, Any]:
    rows = store.rows(META_MODULE)
    if not rows:
        rows.append({
            "id": 1,
            "当前版本": 0,
            "上一版本": None,
            "已合并版本": [],
            "最后生成时间": None,
            "最后批次序号": 0,
        })
    return rows[0]


def batches() -> list[dict[str, Any]]:
    return sorted(store.rows(BATCH_MODULE), key=lambda row: int(row.get("版本", 0)))


def latest_batch() -> dict[str, Any] | None:
    rows = batches()
    return rows[-1] if rows else None


def get_batch(version: int) -> dict[str, Any] | None:
    for row in store.rows(BATCH_MODULE):
        if int(row.get("版本", 0)) == version:
            return row
    return None


def build_items(at: Any) -> list[dict[str, Any]]:
    """按给定时刻的判定链算出可调度节点条目。调用方需持有领域锁。"""
    moment = parse_time(at, now_utc())
    items: list[dict[str, Any]] = []
    for node in list(store.rows(node_lease.MODULE)):
        node_lease.ensure_initialized(node)
        if node.get("status") == "归档":
            continue
        schedulable, _reason = node_lease.is_schedulable(node, moment)
        if not schedulable:
            continue
        decision = node_lease.decision_as_of(node, moment) or {}
        capacity = node_lease.capacity_of(node)
        free = node_lease.free_slots_of(node)
        items.append({
            "节点编号": node.get("节点编号"),
            "节点记录id": node.get("id"),
            "主机地址": node.get("主机地址"),
            "地域标签": node.get("地域标签"),
            "并发槽位数": capacity,
            "空闲槽位": free,
            "占用槽位": capacity - free,
            "租约版本": int(decision.get("租约版本", 0) or 0),
            "租约到期时间": decision.get("租约到期时间"),
            "最后心跳时间": node.get("last_heartbeat_at"),
            "判定时间": decision.get("判定时间"),
            "判定优先级": int(decision.get("判定优先级", 0) or 0),
            "判定序号": int(decision.get("判定序号", 0) or 0),
            "槽位快照摘要": decision.get("槽位快照摘要") or node.get("slots_digest"),
            "在线状态": "在线",
        })
    items.sort(key=lambda item: str(item.get("节点编号", "")))
    return items


def _presence_conflicts(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
    at: Any,
) -> list[dict[str, Any]]:
    if previous is None:
        return []
    prev_map = {str(item.get("节点编号")): item for item in previous.get("条目", [])}
    curr_map = {str(item.get("节点编号")): item for item in current.get("条目", [])}
    conflicts: list[dict[str, Any]] = []
    for code in sorted(set(prev_map) | set(curr_map)):
        if (code in prev_map) == (code in curr_map):
            continue
        node = node_lease.find_node(code)
        decision = node_lease.decision_as_of(node, at) if node else None
        conflicts.append({
            "节点编号": code,
            "前一版本": {
                "版本": previous.get("版本"),
                "可调度": code in prev_map,
                "判定序号": (prev_map.get(code) or {}).get("判定序号"),
            },
            "当前版本": {
                "版本": current.get("版本"),
                "可调度": code in curr_map,
                "判定序号": (curr_map.get(code) or {}).get("判定序号"),
            },
            "采用判定序号": (decision or {}).get("判定序号"),
            "采用结果": (decision or {}).get("结果"),
            "裁决依据": "按判定时间、优先级、判定序号取最大者",
        })
    return conflicts


def publish(
    *,
    trigger: str,
    at: Any | None = None,
    request_id: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """生成一个新批次。

    同一请求编号重复提交且结果内容相同，直接返回旧批次，不产生新版本。
    如果重试时节点状态已经变了，说明这是不同的结果，允许生成新批次并标注冲突，
    避免把新状态当成重复提交吞掉。
    """
    with DOMAIN_LOCK:
        meta = _meta()
        moment = parse_time(at, now_utc())
        items = build_items(moment)
        digest = items_digest(items)
        if request_id:
            for row in store.rows(INGEST_MODULE):
                if row.get("请求编号") == request_id and row.get("结果类型") == "可调度批次":
                    stored = get_batch(int(row.get("批次版本", 0)))
                    if stored is not None and str(stored.get("内容摘要")) == digest:
                        return {**stored, "重复提交": True}
        version = int(meta.get("当前版本", 0) or 0) + 1
        previous = latest_batch()
        batch = {
            "id": store.next_id(BATCH_MODULE),
            "版本": version,
            "批次数": version,
            "水位时间": format_time(moment),
            "生成时间": format_time(now_utc()),
            "触发": trigger,
            "提交编号": request_id,
            "上一版本": previous.get("版本") if previous else None,
            "条目": items,
            "条目数": len(items),
            "内容摘要": digest,
            "状态": "已合并" if previous else "初始",
            "边界冲突": [],
            "幂等冲突": bool(
                request_id
                and any(
                    row.get("请求编号") == request_id
                    and row.get("结果类型") == "可调度批次"
                    for row in store.rows(INGEST_MODULE)
                )
            ),
            **(extra or {}),
        }
        batch["边界冲突"] = _presence_conflicts(previous, batch, moment)
        store.rows(BATCH_MODULE).append(batch)
        meta.update({
            "当前版本": version,
            "上一版本": previous.get("版本") if previous else None,
            "已合并版本": [row.get("版本") for row in (batches()[-2:])],
            "最后生成时间": batch["生成时间"],
            "最后批次序号": store.next_sequence(),
        })
        if request_id:
            store.rows(INGEST_MODULE).append({
                "id": store.next_id(INGEST_MODULE),
                "请求编号": request_id,
                "结果类型": "可调度批次",
                "批次版本": version,
                "记录时间": format_time(now_utc()),
            })
        return batch


def ensure_initial_batch() -> dict[str, Any]:
    with DOMAIN_LOCK:
        batch = latest_batch()
        if batch is not None:
            return batch
        return publish(trigger="初始化", at=now_utc())


def merged_view(at: Any | None = None) -> dict[str, Any]:
    """最近两个批次的合并视图。

    条目按节点判定链重新推导：两次批次之间如果有心跳、离线动作或租约到期，
    合并视图只会给出一个在线结论，不会把两个批次的旧判断并排展示。默认按
    当前时刻推导，所以租约自然到期后节点清单和可调度结果会同时反映出来。
    到期租约的最终记录由 ``/api/heartbeat/sweep`` 或下一次处理落库，读取本身
    不写数据。
    """
    ensure_initial_batch()
    rows = batches()
    window = rows[-2:]
    moment = parse_time(at, now_utc()) if at is not None else now_utc()
    items = build_items(moment)
    conflicts: list[dict[str, Any]] = []
    if len(window) == 2:
        view = {"版本": window[1].get("版本"), "条目": items}
        conflicts = _presence_conflicts(window[0], view, moment)
    return {
        "版本": window[-1].get("版本"),
        "批次数": window[-1].get("批次数"),
        "当前版本": window[-1].get("版本"),
        "合并版本": [row.get("版本") for row in window],
        "合并水位": format_time(moment),
        "生成时间": window[-1].get("生成时间"),
        "触发": window[-1].get("触发"),
        "条目": items,
        "条目数": len(items),
        "可调度节点数": len(items),
        "内容摘要": items_digest(items),
        "边界冲突": conflicts,
        "批次状态": "已合并" if len(window) > 1 else "单批次",
    }


def verify(
    *,
    version: int | None = None,
    items: list[dict[str, Any]] | None = None,
    digest: str | None = None,
    against_nodes: bool = True,
) -> dict[str, Any]:
    """校验数据集完整性：摘要、条数、唯一性、槽位口径与节点判定来源。"""
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: str = "") -> None:
        checks.append({"校验项": name, "通过": bool(passed), "详情": detail})

    if items is None:
        if version is None:
            batch = latest_batch()
        else:
            batch = get_batch(version)
        if batch is None:
            return {
                "ok": False,
                "批次版本": version,
                "校验项": [{"校验项": "批次存在", "通过": False, "详情": "未找到可校验的批次"}],
                "摘要一致": False,
                "条目数一致": False,
            }
        stored_items = list(batch.get("条目") or [])
        stored_digest = str(batch.get("内容摘要") or "")
        expected_count = int(batch.get("条目数", len(stored_items)) or 0)
        version = int(batch.get("版本", 0))
        batch_at = batch.get("水位时间")
        current_version = int(_meta().get("当前版本", 0) or 0)
        compare_against_nodes = against_nodes and version == current_version
    else:
        stored_items = list(items)
        stored_digest = digest or items_digest(stored_items)
        expected_count = len(stored_items)
        batch_at = None
        compare_against_nodes = against_nodes

    recomputed = items_digest(stored_items)
    check("内容摘要", recomputed == stored_digest, f"重算 {recomputed} / 记录 {stored_digest}")
    check("条目数", len(stored_items) == expected_count, f"实际 {len(stored_items)} / 记录 {expected_count}")
    if digest is not None and items is not None:
        check("外部摘要", recomputed == digest, f"重算 {recomputed} / 提交 {digest}")

    codes = [str(item.get("节点编号", "")) for item in stored_items]
    check("节点编号唯一", len(codes) == len(set(codes)), "存在重复节点编号" if len(codes) != len(set(codes)) else "")
    missing_fields = [
        f"{item.get('节点编号')}:{field}"
        for item in stored_items
        for field in REQUIRED_ITEM_FIELDS
        if item.get(field) in (None, "")
    ]
    check("必填字段完整", not missing_fields, "、".join(missing_fields[:5]))
    slot_errors = []
    for item in stored_items:
        capacity = to_int(item.get("并发槽位数"), 0) or 0
        free = to_int(item.get("空闲槽位"), 0) or 0
        used = to_int(item.get("占用槽位"), 0) or 0
        if capacity <= 0 or free < 0 or used < 0 or free + used != capacity or free > capacity:
            slot_errors.append(str(item.get("节点编号")))
    check("槽位口径一致", not slot_errors, "、".join(slot_errors[:5]))
    expiry_errors = []
    for item in stored_items:
        expires = parse_time(item.get("租约到期时间"))
        boundary = parse_time(item.get("判定时间"))
        if expires is None:
            expiry_errors.append(str(item.get("节点编号")))
        elif batch_at and expires <= parse_time(batch_at, now_utc()):
            expiry_errors.append(str(item.get("节点编号")))
        elif boundary is None:
            expiry_errors.append(str(item.get("节点编号")))
    check("租约有效", not expiry_errors, "、".join(expiry_errors[:5]))

    if compare_against_nodes:
        mismatch = []
        for item in stored_items:
            node = node_lease.find_node(item.get("节点编号"))
            if node is None:
                mismatch.append(f"{item.get('节点编号')}:节点不存在")
                continue
            node_lease.ensure_initialized(node)
            if node.get("status") == "归档":
                mismatch.append(f"{item.get('节点编号')}:节点已归档")
                continue
            decision = node_lease.decision_as_of(node, batch_at or now_utc())
            if decision is None or int(decision.get("判定序号", 0)) != int(item.get("判定序号", 0)):
                mismatch.append(f"{item.get('节点编号')}:判定序号不一致")
            if str(node.get("slots_digest")) != str(item.get("槽位快照摘要")):
                mismatch.append(f"{item.get('节点编号')}:槽位快照摘要不一致")
        check("节点记录一致", not mismatch, "、".join(mismatch[:5]))

    return {
        "ok": all(item["通过"] for item in checks),
        "批次版本": version,
        "条目数": len(stored_items),
        "内容摘要": recomputed,
        "摘要一致": checks[0]["通过"],
        "条目数一致": checks[1]["通过"],
        "校验项": checks,
    }
