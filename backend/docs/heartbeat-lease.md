# 心跳租约 / 可调度节点结果 / 请求分流侧

本次改动把 "采集心跳事件 -> 节点租约 -> 可调度节点结果 -> 请求分流侧清单" 串成一条可追溯链路。

## 1. 数据流

```
采集心跳事件 (heartbeat)
   │  校验时间、租约、槽位快照
   ▼
节点租约判定 (node_lease.decisions)      ← 人工上线/离线/归档也写同一份判定链
   │  取判定时间 <= 水位 的最后一条（时间、优先级、序号）
   ▼
可调度节点结果批次 (_schedulable_batches)  ← 只追加，旧版本保留
   │  最近两个批次合并 + 边界冲突裁决
   ▼
请求分流侧清单 (_routing_roster)          ← 在岗/已退出，可追溯到批次版本
   │
   ▼
目标域名新增请求 (_routing_assignments)   ← 只追加，节点退出时转派或保留待处理
```

所有状态变更都在 `app/lock.py` 的同一把可重入锁里完成，FastAPI 的同步接口在
线程池里并发执行时也不会出现两份结果。

## 2. 判定链与优先级

同一节点只有一个在线结论：`node_lease.decisions` 按 `(判定时间, 优先级, 判定序号)`
排序，取水位时刻之前的最后一条。优先级：

| 优先级 | 来源 | 说明 |
| --- | --- | --- |
| 50 | 槽位快照失败 | 任一槽位非法/写入失败，节点停止接单 |
| 40 | 人工离线 | 粘性：补报心跳不能覆盖，必须人工恢复在线 |
| 30 | 租约到期 | 到期时间早于等于水位即视为离线 |
| 20 | 人工上线 / 离线恢复 | 授予在线身份但不授予租约 |
| 10 | 心跳 | 延长租约，租约版本 +1 |

因此 "心跳、离线动作与租约到期在边界附近同时到达" 时不会出现两套在线判断；
优先级与判定序号会写进 `边界冲突`，便于追查。

## 3. 关键口径

* **并发心跳只延长一次租约**：同一节点同一采集时间的多条心跳合并；
  租约未到期且新租约不晚于当前到期时间时也只合并。
* **槽位快照写入失败**：整份快照校验（数量、编号、状态、去重），任一项失败则
  不落半份快照，节点被移出可调度结果并留下 `slot_snapshot_failed` 最终记录。
* **可调度结果幂等**：同一 `request_id` 且结果内容摘要相同，返回旧批次；内容变化
  则生成新批次并标记 `幂等冲突`，不会把新状态当成重复提交吞掉。
* **请求幂等**：同一 `请求编号` 只写一条分配记录。
* **节点退出**：分流侧把清单行标为 `已退出`，原始分配行改为 `原节点退出` 并保留
  节点编号与时间；能转派就追加一条 `已分配`，不能转派就追加 `待处理`，不静默丢弃。
* **最终记录可区分**：`/api/heartbeat/final-records?reason_code=` 支持
  `manual_offline` / `lease_expired` / `slot_snapshot_failed`。
* **历史不被改写**：分配记录只追加；迟到心跳（采集时间早于最近判定）整条拒绝，
  不会回写历史分配或最终记录。
* **槽位不超卖**：分流侧按 "可调度结果空闲槽位 - 该域名已分配未完成请求数" 选节点。

## 4. 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/heartbeat` | 心跳事件列表（`node_code` / `status`） |
| POST | `/api/heartbeat/collect` | 采集心跳事件（可批量，`events` 或 `values.events`） |
| POST | `/api/heartbeat/ingest` | 处理待处理心跳，发布可调度批次并刷新分流清单 |
| POST | `/api/heartbeat/sweep` | 主动扫描到期租约 |
| GET | `/api/heartbeat/final-records` | 离线最终记录（`reason_code` / `node_code`） |
| GET | `/api/schedulable` | 最近两个批次合并后的可调度节点 |
| GET | `/api/schedulable/meta` | 版本、合并批次、边界冲突、摘要 |
| GET | `/api/schedulable/batches` | 保留的批次列表 |
| GET | `/api/schedulable/batches/{version}` | 指定版本完整批次 |
| POST | `/api/schedulable/publish` | 生成新批次（`request_id` 幂等） |
| GET/POST | `/api/schedulable/verify` | 校验最新批次 / 校验外部提交的条目 |
| GET | `/api/routing/roster` | 分流侧在岗清单 |
| GET | `/api/routing/assignments` | 请求分流记录 |
| POST | `/api/routing/requests` | 写入目标域名新增请求 |
| POST | `/api/routing/sync` | 用当前可调度结果刷新清单（同批次只生效一次） |
| GET | `/api/routing/syncs` | 同步记录 |
| GET | `/api/routing/verify` | 分流侧一致性校验 |
| GET | `/api/domain/summary` | 新领域一屏汇总 |

采集节点新增 `GET /api/worker/stats`；节点明细 `GET /api/worker/{id}` 会附上
`判定记录` 与 `状态历史`，登记、人工切换、离线恢复、归档可由同一条节点记录解释。

## 5. 测试

```bash
cd backend
.venv/bin/python -m pytest tests -q
```

覆盖：有效心跳上线、并发心跳只续租一次、槽位失败移出结果集、重复事件/重复提交、
并发处理只保存一份结果、批次边界冲突单一结论、迟到心跳不改写、请求幂等与转派/待处理、
节点生命周期可解释性、数据集篡改可检出、分流侧同批次只同步一次。
