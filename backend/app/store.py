"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

from typing import Any

from app.seed import SEED_ROWS


# 新增的业务表放在这里初始化；下划线开头的是内部表，不出现在运营概览里。
VISIBLE_MODULES = ("heartbeat", "schedulable", "routing")
INTERNAL_MODULES = (
    "_node_final_records",
    "_schedulable_batches",
    "_schedulable_meta",
    "_routing_roster",
    "_routing_assignments",
    "_routing_syncs",
    "_ingest_requests",
)


class Store:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        """恢复示例数据；测试与本地演示可以反复从干净状态开始。"""
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        for name in VISIBLE_MODULES + INTERNAL_MODULES:
            self._tables.setdefault(name, [])
        self._sequence = 0

    def module_names(self) -> list[str]:
        return sorted(name for name in self._tables if not name.startswith("_"))

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def next_sequence(self) -> int:
        """全局单调序号：同一时刻发生的多个判定也能稳定排序。"""
        self._sequence += 1
        return self._sequence

    def next_id(self, module: str) -> int:
        return max((int(row.get("id", 0)) for row in self.rows(module)), default=0) + 1

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
