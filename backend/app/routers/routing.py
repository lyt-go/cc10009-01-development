"""请求分流侧接口：读取可调度结果、写入新增请求、查看分流清单与同步记录。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import pipeline, routing, schedulable

router = APIRouter(prefix="/api/routing", tags=["请求分流侧"])


@router.get("/roster", response_model=PageResult[dict])
def list_roster(
    domain: str | None = Query(default=None, description="按目标域名过滤"),
    status: str | None = Query(default=None, description="在岗、已退出"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """分流侧在岗清单：来源是可调度节点结果，可追溯批次版本。"""
    rows = routing.all_roster(domain)
    if status:
        rows = [row for row in rows if row.get("状态") == status]
    total = len(rows)
    start = max(page - 1, 0) * size
    return PageResult(items=rows[start:start + size], total=total, page=page, size=size)


@router.get("/assignments", response_model=PageResult[dict])
def list_assignments(
    domain: str | None = Query(default=None, description="按目标域名过滤"),
    node_code: str | None = Query(default=None, description="按节点编号过滤"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """新增请求的分流结果；分配历史只追加，后续心跳补报不改写。"""
    items, total = routing.assignments(domain=domain, node_code=node_code, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/requests", response_model=ActionResult)
def submit_requests(payload: EntryPayload) -> ActionResult:
    """把目标域名的新增请求写入请求分流侧结果；请求编号重复时只生效一次。"""
    result = routing.submit_requests(payload.merged_values())
    return ActionResult(
        ok=bool(result.get("ok")),
        message=str(result.get("message")),
        entry=result,
    )


@router.post("/sync", response_model=ActionResult)
def sync_roster(payload: EntryPayload | None = None) -> ActionResult:
    """用当前可调度结果刷新分流侧清单；同一批次版本重复触发只生效一次。"""
    values = payload.merged_values() if payload else {}
    view = schedulable.merged_view()
    if values.get("publish"):
        batch = pipeline.refresh(trigger=str(values.get("trigger") or "分值同步"))
        log = batch.get("分流同步")
    else:
        log = routing.sync(view)
    return ActionResult(
        ok=True,
        message=f"请求分流侧清单已同步，批次版本 {log.get('批次版本')}" if log else "没有可同步的批次",
        entry=log,
    )


@router.get("/syncs", response_model=PageResult[dict])
def list_syncs(page: int = 1, size: int = 20) -> PageResult[dict]:
    """分流侧同步记录：每次批次只应出现一条。"""
    rows = list(reversed(routing.sync_logs()))
    total = len(rows)
    start = max(page - 1, 0) * size
    return PageResult(items=rows[start:start + size], total=total, page=page, size=size)


@router.get("/verify", response_model=dict)
def verify_routing() -> dict[str, Any]:
    """校验分流侧在岗清单与在途请求是否仍指向可调度节点。"""
    return routing.verify()
