"""可调度节点结果接口：批次生成、合并视图查询与完整性校验。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import heartbeat, pipeline, schedulable

router = APIRouter(prefix="/api/schedulable", tags=["可调度节点结果"])


@router.get("", response_model=PageResult[dict])
def list_items(
    keyword: str | None = Query(default=None, description="按节点编号检索"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """读取最近两个批次合并后的可调度节点结果。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    view = schedulable.merged_view()
    rows = list(view.get("条目") or [])
    if keyword:
        rows = [row for row in rows if keyword in str(row.get("节点编号", ""))]
    total = len(rows)
    start = max(page - 1, 0) * size
    items = rows[start:start + size]
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/meta", response_model=dict)
def dataset_meta() -> dict[str, Any]:
    """数据集元信息：当前版本、参与合并的批次、边界冲突与内容摘要。"""
    view = schedulable.merged_view()
    view.pop("条目", None)
    return view


@router.get("/batches", response_model=PageResult[dict])
def list_batches(page: int = 1, size: int = 20) -> PageResult[dict]:
    """列出保留的可调度结果批次；旧版本不覆盖，便于追溯。"""
    rows = list(reversed(schedulable.batches()))
    total = len(rows)
    start = max(page - 1, 0) * size
    items = []
    for row in rows[start:start + size]:
        trimmed = {key: value for key, value in row.items() if key != "条目"}
        trimmed["条目数"] = row.get("条目数", len(row.get("条目") or []))
        items.append(trimmed)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/batches/{version}", response_model=dict)
def get_batch(version: int) -> dict:
    """按版本读取完整批次（含条目与内容摘要）。"""
    batch = schedulable.get_batch(version)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"可调度批次 {version} 不存在")
    return batch


@router.post("/publish", response_model=ActionResult)
def publish_batch(payload: EntryPayload | None = None) -> ActionResult:
    """按当前租约状态生成一个新的可调度批次，并驱动分流侧清单更新。"""
    values = payload.merged_values() if payload else {}
    at = values.get("at") or values.get("水位时间")
    request_id = values.get("request_id") or values.get("请求编号")
    batch = pipeline.refresh(
        trigger=str(values.get("trigger") or "手动发布"),
        request_id=str(request_id) if request_id else None,
        at=_parse_at(at),
    )
    return ActionResult(
        ok=True,
        message=f"可调度节点结果已生成，版本 {batch.get('版本')}，共 {batch.get('条目数')} 个节点",
        entry=batch,
    )


@router.get("/verify", response_model=dict)
def verify_dataset(
    version: int | None = Query(default=None, description="不传则校验最新批次"),
    against_nodes: bool = Query(default=True, description="是否比对节点清单的判定记录"),
) -> dict[str, Any]:
    """校验可调度结果完整性：摘要、条数、去重、槽位口径与节点记录一致性。"""
    return schedulable.verify(version=version, against_nodes=against_nodes)


@router.post("/verify", response_model=dict)
def verify_submitted(payload: EntryPayload) -> dict[str, Any]:
    """校验外部提交的可调度结果：按提交内容重算摘要并核对必填字段。"""
    values = payload.merged_values()
    items = values.get("items") or values.get("条目")
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="items 必须是可调度节点条目列表")
    return schedulable.verify(
        items=items,
        digest=str(values.get("digest") or values.get("内容摘要") or "") or None,
        against_nodes=False,
    )


@router.get("/expired", response_model=ActionResult)
def expire_now() -> ActionResult:
    """立即扫描到期租约，并把结果落到可调度批次。"""
    result = heartbeat.sweep()
    return ActionResult(ok=True, message=str(result.get("message")), entry=result)


def _parse_at(value: Any) -> Any:
    from app.timeutil import now_utc, parse_time

    if not value:
        return now_utc()
    parsed = parse_time(value)
    if parsed is None:
        raise HTTPException(status_code=400, detail=f"水位时间「{value}」无法解析")
    return parsed
