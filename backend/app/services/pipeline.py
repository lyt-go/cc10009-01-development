"""处理流水线：心跳/租约 -> 可调度批次 -> 请求分流侧清单。

``refresh`` 是唯一的编排入口，保证 "本次处理结果驱动请求分流侧清单更新，
并保留可调度节点结果" 的顺序。领域锁让并发触发退化成串行：后到者看到已落库
的批次后只会同步已存在的版本，不会产生第二份结果。
"""
from __future__ import annotations

from typing import Any

from app.lock import DOMAIN_LOCK
from app.services import routing, schedulable
from app.timeutil import now_utc


def latest_batch() -> dict[str, Any] | None:
    return schedulable.latest_batch()


def refresh(
    *,
    trigger: str,
    request_id: str | None = None,
    at: Any | None = None,
) -> dict[str, Any]:
    with DOMAIN_LOCK:
        batch = schedulable.publish(trigger=trigger, at=at or now_utc(), request_id=request_id)
        batch["分流同步"] = routing.sync(batch)
        return batch


def run_once(request_id: str | None = None) -> dict[str, Any]:
    """手动触发一次完整处理：扫描租约 + 消费待处理心跳。"""
    from app.services import heartbeat

    return heartbeat.ingest(request_id=request_id)
