"""跨服务共享的领域锁。

节点租约、可调度结果和请求分流共用一份内存状态，任何一次变更都必须在同一把
可重入锁里完成。FastAPI 的同步接口跑在线程池里，只用单线程假设会漏掉并发场景。
"""
from __future__ import annotations

import threading

DOMAIN_LOCK = threading.RLock()
