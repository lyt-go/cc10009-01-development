"""节点心跳租约接口：采集、处理心跳事件，扫描到期租约并读取最终记录。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import heartbeat, node_lease, worker

router = APIRouter(prefix="/api/heartbeat", tags=["节点心跳租约"])

STATUSES = [heartbeat.STATUS_PENDING, heartbeat.STATUS_DONE, heartbeat.STATUS_MERGED, heartbeat.STATUS_LATE, heartbeat.STATUS_REJECTED]


@router.get("", response_model=PageResult[dict])
def list_events(
    node_code: str | None = Query(default=None, description="按节点编号检索"),
    status: str | None = Query(default=None, description="待处理、已处理、已合并、迟到、已拒绝"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按节点编号与处理状态过滤心跳事件；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = heartbeat.list_events(node_code=node_code, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/collect", response_model=ActionResult)
def collect_events(payload: EntryPayload) -> ActionResult:
    """采集一条或多条心跳事件；重复的事件编号不会重复入库。"""
    created, errors = heartbeat.collect(payload.merged_values())
    if not created:
        message = "心跳事件未入库：" + ("；".join(errors) if errors else "缺少事件内容")
        return ActionResult(ok=False, message=message)
    message = f"已采集 {len(created)} 条心跳事件"
    if errors:
        message += "；" + "；".join(errors)
    return ActionResult(ok=True, message=message, entry={"events": created, "skipped": errors})


@router.post("/ingest", response_model=ActionResult)
def ingest_events(payload: EntryPayload | None = None) -> ActionResult:
    """读取待处理心跳事件，校验并延长租约，生成可调度节点结果并刷新分流清单。"""
    request_id = None
    if payload is not None:
        values = payload.merged_values()
        raw = values.get("request_id") or values.get("请求编号")
        request_id = str(raw) if raw else None
    result = heartbeat.ingest(request_id=request_id)
    return ActionResult(
        ok=bool(result.get("ok")),
        message=str(result.get("message")),
        entry=result,
    )


@router.post("/sweep", response_model=ActionResult)
def sweep_expired() -> ActionResult:
    """主动扫描到期租约，生成离线最终记录并保留一份新的可调度结果。"""
    result = heartbeat.sweep()
    return ActionResult(ok=True, message=str(result.get("message")), entry=result)


@router.get("/final-records", response_model=PageResult[dict])
def list_final_records(
    reason_code: str | None = Query(default=None, description="manual_offline、lease_expired、slot_snapshot_failed"),
    node_code: str | None = Query(default=None, description="按节点编号检索"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """读取离线最终记录：租约到期与人工离线按原因码区分。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = worker.final_records(
        reason_code=reason_code, node_code=node_code, page=page, size=size
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/reason-codes")
def reason_codes() -> dict[str, Any]:
    """离线原因码字典，便于前端按原因筛最终记录。"""
    return {"items": [{"code": key, "label": label} for key, label in node_lease.REASON_CODES.items()]}
