"""槽位快照：校验、摘要与原子替换。

一次心跳带来的槽位快照要么整份通过校验并落盘，要么整份不生效。
调用方只有在拿到 ``ok=True`` 时才允许继续延长租约。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from app.timeutil import to_int

SLOT_STATUSES = ("空闲", "占用")


def default_snapshot(capacity: int) -> list[dict[str, Any]]:
    return [{"槽位": index, "状态": "空闲"} for index in range(capacity)]


def snapshot_digest(snapshot: list[dict[str, Any]]) -> str:
    payload = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_snapshot(
    capacity: int, raw: Any
) -> tuple[list[dict[str, Any]] | None, str | None]:
    """校验整份快照；``raw`` 为 None 表示本次心跳没有携带快照。"""
    if raw is None:
        return None, None
    if not isinstance(raw, (list, tuple)):
        return None, "槽位快照必须是列表"
    if len(raw) != capacity:
        return None, f"槽位快照数量应为 {capacity}，实际为 {len(raw)}"
    seen: set[int] = set()
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(raw):
        if isinstance(item, dict):
            slot = to_int(item.get("槽位", item.get("slot", index)), None)
            status = str(item.get("状态", item.get("status", "")) or "").strip()
        elif isinstance(item, str):
            slot, status = index, item.strip()
        else:
            slot, status = index, ""
        if slot is None or slot < 0 or slot >= capacity:
            return None, f"第 {index + 1} 个槽位编号非法"
        if slot in seen:
            return None, f"槽位 {slot} 重复出现"
        if status not in SLOT_STATUSES:
            return None, f"槽位 {slot} 状态「{status}」不受支持"
        seen.add(slot)
        normalized.append({"槽位": slot, "状态": status})
    if len(seen) != capacity:
        return None, "槽位快照不完整"
    return sorted(normalized, key=lambda item: int(item["槽位"])), None


def free_slots(snapshot: list[dict[str, Any]]) -> int:
    return sum(1 for item in snapshot if item.get("状态") == "空闲")


def items_digest(items: list[dict[str, Any]]) -> str:
    """可调度结果的完整性摘要：按节点编号排序后做规范化 JSON 哈希。"""
    ordered = sorted(items, key=lambda item: str(item.get("节点编号", "")))
    payload = json.dumps(ordered, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()
