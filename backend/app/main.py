"""分布式网页抓取调度平台 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.services import node_lease, schedulable
from app.store import store

app = FastAPI(title="分布式网页抓取调度平台", version="1.0.0")


def prepare_domain() -> None:
    """启动时把示例节点转成判定链，并准备一份可调度节点结果。

    示例节点里标为 "在线/注册中" 的会补一条明确标注为示例数据的心跳，这样首屏
    看到的可调度结果和节点清单是同一套判定推导出来的，而不是另写一份状态。
    """
    from app.services import heartbeat
    from app.timeutil import format_time, now_utc

    candidates = []
    for row in store.rows(node_lease.MODULE):
        node_lease.ensure_initialized(row)
        if row.get("status") in ("在线", "注册中"):
            candidates.append(row.get("节点编号"))
    if candidates and not store.rows(heartbeat.MODULE):
        moment = format_time(now_utc())
        heartbeat.collect({"events": [
            {
                "事件编号": f"SEED-{code}",
                "节点编号": code,
                "采集时间": moment,
                "租约秒数": settings.lease_seconds_max,
                "采集来源": "示例数据",
            }
            for code in candidates
        ]})
        heartbeat.ingest()
    schedulable.ensure_initial_batch()


app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for module in ROUTERS:
    app.include_router(module.router)


@app.on_event("startup")
def startup_prepare_domain() -> None:
    """启动时准备一份可调度节点结果，前端首屏就有数据可看。"""
    prepare_domain()


@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


@app.get("/api/domain/summary")
def domain_summary() -> dict[str, object]:
    """新领域一屏概览：心跳、可调度结果、分流侧与最终记录的当前状态。"""
    from app.services import heartbeat as heartbeat_service
    from app.services import routing

    view = schedulable.merged_view()
    _events, event_total = heartbeat_service.list_events(page=1, size=1)
    return {
        "可调度节点数": view.get("可调度节点数"),
        "可调度批次": {
            "当前版本": view.get("当前版本"),
            "合并版本": view.get("合并版本"),
            "内容摘要": view.get("内容摘要"),
            "批次状态": view.get("批次状态"),
            "边界冲突数": len(view.get("边界冲突") or []),
        },
        "心跳事件数": event_total,
        "待处理心跳": len([
            row for row in store.rows("heartbeat") if row.get("处理状态") == "待处理"
        ]),
        "在岗节点数": len(routing.active_roster()),
        "请求记录数": len(store.rows("_routing_assignments")),
        "最终记录数": len(store.rows("_node_final_records")),
    }
