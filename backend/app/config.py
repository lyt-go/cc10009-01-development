"""运行配置：端口、跨域、运行环境。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    app_name: str = "分布式网页抓取调度平台"
    env: str = "local"
    port: int = 8000
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    # 心跳租约：默认时长、允许的最长时长，以及节点并发槽位上限。
    lease_seconds_default: int = 60
    lease_seconds_max: int = 3600
    worker_capacity_max: int = 128
    # 采集时间允许的超前量：超过就按时钟漂移拒绝，避免未来的判定改写当前结果。
    heartbeat_clock_skew_seconds: int = 60


settings = Settings()
