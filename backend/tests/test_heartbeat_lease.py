"""心跳租约、可调度结果与请求分流的行为测试。

运行：cd backend && .venv/bin/python -m pytest tests -q
"""
from __future__ import annotations

import threading
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services import heartbeat, node_lease, pipeline, routing, schedulable
from app.services.worker import WorkerService
from app.store import store
from app.timeutil import format_time, now_utc


@pytest.fixture()
def client() -> TestClient:
    store.reset()
    return TestClient(app)


def make_node(code: str = "WORK-T1", capacity: int = 2, *, register: bool = True) -> dict:
    service = WorkerService()
    entry, missing = service.create_entry({
        "节点编号": code,
        "主机地址": "10.1.0.1:9100",
        "并发槽位数": str(capacity),
        "地域标签": "测试区",
    })
    assert not missing and entry is not None
    if register:
        node, message = service.run_action(entry["id"], "提交注册")
        assert node is not None, message
    return entry


def heartbeat_event(code: str, node_code: str, *, at=None, lease: int = 60, slots=None, event=None):
    return {
        "事件编号": event or f"EV-{code}-{store.next_sequence()}",
        "节点编号": node_code,
        "采集时间": format_time(at or now_utc()),
        "租约秒数": lease,
        "槽位快照": slots,
    }


def test_valid_heartbeat_extends_lease_and_enters_schedulable(client: TestClient) -> None:
    make_node("WORK-T1", 2)
    moment = now_utc()
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1", at=moment, lease=90)]})
    result = heartbeat.ingest()

    assert result["ok"] is True
    assert result["延长租约数"] == 1
    node = node_lease.find_node("WORK-T1")
    assert node is not None and node["status"] == "在线"
    assert node["lease_epoch"] == 1
    view = schedulable.merged_view()
    assert [item["节点编号"] for item in view["条目"]] == ["WORK-T1"]
    assert schedulable.verify()["ok"] is True


def test_concurrent_heartbeats_extend_lease_only_once(client: TestClient) -> None:
    make_node("WORK-T1", 2)
    moment = now_utc()
    heartbeat.collect({"events": [
        heartbeat_event("A", "WORK-T1", at=moment, lease=120),
        heartbeat_event("B", "WORK-T1", at=moment, lease=120),
    ]})
    result = heartbeat.ingest()

    node = node_lease.find_node("WORK-T1")
    assert node is not None
    assert node["lease_epoch"] == 1
    assert result["延长租约数"] == 1
    assert result["合并心跳数"] == 1
    heartbeats = [d for d in node["decisions"] if d["来源"] == "心跳"]
    assert len(heartbeats) == 1


def test_slot_snapshot_failure_removes_node_and_keeps_online_flag_consistent(
    client: TestClient,
) -> None:
    make_node("WORK-T1", 2)
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1", lease=60)]})
    heartbeat.ingest()
    assert schedulable.merged_view()["条目数"] == 1

    heartbeat.collect({"events": [heartbeat_event(
        "B", "WORK-T1", lease=60,
        slots=[{"槽位": 0, "状态": "占用"}, {"槽位": 1, "状态": "未知"}],
    )]})
    result = heartbeat.ingest()

    assert result["拒绝数"] == 1
    view = schedulable.merged_view()
    assert view["条目"] == []
    assert view["可调度节点数"] == 0
    node = node_lease.find_node("WORK-T1")
    assert node is not None and node_lease.online_state(node) == "离线"
    finals = store.rows(node_lease.FINAL_MODULE)
    assert [(row["节点编号"], row["原因码"]) for row in finals] == [
        ("WORK-T1", "slot_snapshot_failed")
    ]
    assert schedulable.verify()["ok"] is True


def test_duplicate_event_is_not_stored_twice(client: TestClient) -> None:
    make_node("WORK-T1", 2)
    event = heartbeat_event("A", "WORK-T1", event="EV-1")
    heartbeat.collect({"events": [event]})
    created, errors = heartbeat.collect({"events": [event]})
    assert created == []
    assert errors and "已采集" in errors[0]
    assert len(store.rows(heartbeat.MODULE)) == 1


def test_schedulable_publish_is_idempotent_per_request_id(client: TestClient) -> None:
    make_node("WORK-T1", 2)
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1")]})
    heartbeat.ingest()

    first = pipeline.refresh(trigger="手动", request_id="REQ-1")
    second = pipeline.refresh(trigger="手动", request_id="REQ-1")
    assert first["版本"] == second["版本"]
    assert second.get("重复提交") is True
    versions = [batch["版本"] for batch in schedulable.batches()]
    assert len(versions) == len(set(versions))


def test_concurrent_ingest_keeps_one_result_per_batch(client: TestClient) -> None:
    make_node("WORK-T1", 2)
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1", lease=60)]})
    results: list[dict] = []

    def run() -> None:
        results.append(heartbeat.ingest())

    threads = [threading.Thread(target=run) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == 4
    assert len(store.rows(heartbeat.MODULE)) == 1
    assert all(event["处理状态"] != heartbeat.STATUS_PENDING for event in store.rows(heartbeat.MODULE))
    versions = {result["批次版本"] for result in results if result.get("批次版本")}
    assert len(versions) == 1


def test_boundary_conflict_resolved_without_two_online_judgments(client: TestClient) -> None:
    make_node("WORK-T1", 2)
    moment = now_utc() - timedelta(seconds=60)
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1", at=moment, lease=30)]})
    heartbeat.ingest()
    batch_before = schedulable.merged_view(at=moment + timedelta(seconds=1))

    # 人工离线、租约到期与心跳在批次边界附近同时到达。
    expiry = moment + timedelta(seconds=30)
    node = node_lease.find_node("WORK-T1")
    assert node is not None
    node_lease.expire_due(expiry)
    WorkerService().run_action(node["id"], "标记离线")
    heartbeat.collect({"events": [heartbeat_event("B", "WORK-T1", at=expiry, lease=60)]})
    heartbeat.ingest()

    latest = schedulable.merged_view()
    assert latest["条目"] == []
    assert latest["当前版本"] > batch_before["当前版本"]
    # 合并视图只保留最近两个批次，边界冲突必须被解释成单一结论。
    assert len(latest["合并版本"]) <= 2
    for conflict in latest["边界冲突"]:
        assert conflict["采用结果"] in ("在线", "离线")
        assert "采用判定序号" in conflict
    # 无论批次边界怎么处理，节点自身只能有一个在线判断。
    assert node_lease.online_state(node) == "离线"
    codes = [row["原因码"] for row in store.rows(node_lease.FINAL_MODULE)
             if row["节点编号"] == "WORK-T1"]
    assert "manual_offline" in codes
    assert codes.count("manual_offline") == 1
    assert codes.count("lease_expired") == 1


def test_late_heartbeat_does_not_rewrite_history(client: TestClient) -> None:
    make_node("WORK-T1", 2)
    moment = now_utc() - timedelta(seconds=60)
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1", at=moment, lease=30)]})
    heartbeat.ingest()
    node = node_lease.find_node("WORK-T1")
    assert node is not None
    assert node_lease.expire_due(moment + timedelta(seconds=30))
    final_before = [dict(row) for row in store.rows(node_lease.FINAL_MODULE)]
    decisions_before = len(node["decisions"])

    heartbeat.collect({"events": [heartbeat_event("B", "WORK-T1", at=moment, lease=120)]})
    result = heartbeat.ingest()

    assert result["拒绝数"] == 1
    assert node["status"] == "离线"
    assert len(node["decisions"]) == decisions_before
    assert [dict(row) for row in store.rows(node_lease.FINAL_MODULE)] == final_before
    assert node_lease.online_state(node) == "离线"


def test_request_is_written_once_and_keeps_domain(client: TestClient) -> None:
    make_node("WORK-T1", 2, register=True)
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1", lease=60)]})
    heartbeat.ingest()

    request = {"请求编号": "Q-1", "目标域名": "news.example.com", "目标URL": "https://news.example.com/1"}
    first = routing.submit_requests({"requests": [request]})
    second = routing.submit_requests({"requests": [request]})
    assert first["已写入"] == 1 and first["重复"] == 0
    assert second["已写入"] == 0 and second["重复"] == 1
    assignments = store.rows(routing.ASSIGNMENT_MODULE)
    assert [row["请求编号"] for row in assignments] == ["Q-1"]
    assert assignments[0]["目标域名"] == "news.example.com"
    assert assignments[0]["节点编号"] == "WORK-T1"
    dispatch_row = next(row for row in store.rows("dispatch") if row["目标域名"] == "news.example.com")
    assert dispatch_row["可调度节点"] == ["WORK-T1"]
    assert dispatch_row["批次版本"] == schedulable.merged_view()["当前版本"]


def test_request_is_transferred_instead_of_dropped_on_node_exit(client: TestClient) -> None:
    make_node("WORK-T1", 2)
    make_node("WORK-T2", 2)
    heartbeat.collect({"events": [
        heartbeat_event("A", "WORK-T1", lease=60),
        heartbeat_event("B", "WORK-T2", lease=60),
    ]})
    heartbeat.ingest()
    routing.submit_requests({"requests": [
        {"请求编号": "Q-1", "目标域名": "news.example.com", "目标URL": "https://news.example.com/1"},
    ]})
    assignment = store.rows(routing.ASSIGNMENT_MODULE)[0]
    original_node = assignment["节点编号"]

    node = node_lease.find_node(original_node)
    assert node is not None
    WorkerService().run_action(node["id"], "标记离线")

    rows = store.rows(routing.ASSIGNMENT_MODULE)
    assert rows[0]["请求编号"] == "Q-1"
    assert rows[0]["节点编号"] == original_node
    assert rows[0]["处置"] == "原节点退出"
    assert rows[-1]["请求编号"] == "Q-1"
    assert rows[-1]["处置"] == "已分配"
    assert rows[-1]["节点编号"] != original_node
    assert routing.active_roster("news.example.com")[0]["节点编号"] != original_node
    assert routing.verify()["ok"] is True


def test_request_stays_pending_when_no_replacement(client: TestClient) -> None:
    make_node("WORK-T1", 2)
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1", lease=60)]})
    heartbeat.ingest()
    routing.submit_requests({"requests": [
        {"请求编号": "Q-1", "目标域名": "news.example.com", "目标URL": "https://news.example.com/1"},
    ]})
    node = node_lease.find_node("WORK-T1")
    assert node is not None
    WorkerService().run_action(node["id"], "标记离线")

    rows = store.rows(routing.ASSIGNMENT_MODULE)
    assert len(rows) == 2
    assert rows[0]["请求编号"] == "Q-1"
    assert rows[0]["节点编号"] == "WORK-T1"
    assert rows[0]["处置"] == "原节点退出"
    assert rows[1]["请求编号"] == "Q-1"
    assert rows[1]["处置"] == "待处理"
    assert "待处理" in rows[1]["说明"]
    assert routing.assignments()[1] == 2  # 历史记录仍保留


def test_lifecycle_is_explained_by_one_node_record(client: TestClient) -> None:
    entry = make_node("WORK-T1", 2)
    service = WorkerService()
    moment = now_utc()
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1", at=moment, lease=30)]})
    heartbeat.ingest()
    assert node_lease.find_node("WORK-T1")["status"] == "在线"

    node, message = service.run_action(entry["id"], "标记离线")
    assert node is not None, message
    node, message = service.run_action(entry["id"], "恢复在线")
    assert node is not None, message
    node, message = service.run_action(entry["id"], "标记离线")
    assert node is not None, message
    node, message = service.run_action(entry["id"], "归档")
    assert node is not None, message

    history = node_lease.find_node("WORK-T1")["history"]
    sequence = [(row["动作"], row["新状态"]) for row in history]
    assert sequence[0] == ("登记", "草稿")
    assert ("提交注册", "注册中") in sequence
    assert ("心跳", "在线") in sequence
    assert ("人工离线", "离线") in sequence
    assert ("离线恢复", "在线") in sequence
    assert sequence[-1] == ("归档", "归档")
    assert [row["序号"] for row in history] == sorted(row["序号"] for row in history)


def test_dataset_verify_detects_tampering(client: TestClient) -> None:
    make_node("WORK-T1", 2)
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1")]})
    heartbeat.ingest()
    batch = schedulable.latest_batch()
    assert batch is not None
    good = schedulable.verify(version=batch["版本"])
    assert good["ok"] is True

    tampered = [dict(item) for item in batch["条目"]]
    tampered[0]["空闲槽位"] = int(tampered[0]["空闲槽位"]) + 1
    bad = schedulable.verify(items=tampered, digest=batch["内容摘要"], against_nodes=False)
    assert bad["ok"] is False
    failed = {item["校验项"] for item in bad["校验项"] if not item["通过"]}
    assert {"内容摘要", "槽位口径一致"} <= failed


def test_routing_sync_applies_once_per_batch(client: TestClient) -> None:
    make_node("WORK-T1", 2)
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1")]})
    heartbeat.ingest()
    batch = schedulable.publish(trigger="测试")
    first = routing.sync(batch)
    second = routing.sync(batch)
    assert first["id"] == second["id"]
    assert second["重复触发"] == 1
    assert len([row for row in routing.sync_logs() if row["批次版本"] == batch["版本"]]) == 1


def test_slot_capacity_is_not_oversold(client: TestClient) -> None:
    make_node("WORK-T1", 1)
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1", lease=60)]})
    heartbeat.ingest()
    result = routing.submit_requests({"requests": [
        {"请求编号": "Q-1", "目标域名": "news.example.com"},
        {"请求编号": "Q-2", "目标域名": "news.example.com"},
    ]})
    assert result["已写入"] == 2
    assert result["待处理"] == 1
    dispositions = [row["处置"] for row in store.rows(routing.ASSIGNMENT_MODULE)]
    assert dispositions.count("已分配") == 1
    assert dispositions.count("待处理") == 1


def test_manual_offline_is_not_overwritten_by_late_heartbeat(client: TestClient) -> None:
    make_node("WORK-T1", 2)
    moment = now_utc() - timedelta(seconds=120)
    heartbeat.collect({"events": [heartbeat_event("A", "WORK-T1", at=moment, lease=60)]})
    heartbeat.ingest()
    node = node_lease.find_node("WORK-T1")
    assert node is not None
    WorkerService().run_action(node["id"], "标记离线")

    heartbeat.collect({"events": [
        heartbeat_event("B", "WORK-T1", at=moment + timedelta(seconds=5), lease=60)
    ]})
    result = heartbeat.ingest()

    assert result["拒绝数"] == 1
    assert node["status"] == "离线"
    assert schedulable.merged_view()["条目"] == []
    assert [row["原因码"] for row in store.rows(node_lease.FINAL_MODULE)] == ["manual_offline"]

    node, message = WorkerService().run_action(node["id"], "恢复在线")
    assert node is not None, message
    heartbeat.collect({"events": [
        heartbeat_event("C", "WORK-T1", at=now_utc(), lease=60)
    ]})
    heartbeat.ingest()
    assert node_lease.online_state(node) == "在线"
    assert [item["节点编号"] for item in schedulable.merged_view()["条目"]] == ["WORK-T1"]


def test_flat_payload_shapes_are_accepted(client: TestClient) -> None:
    created = client.post("/api/worker", json={
        "节点编号": "WORK-FLAT",
        "主机地址": "10.1.0.9:9100",
        "并发槽位数": "2",
    })
    assert created.status_code == 200
    assert created.json()["ok"] is True
    node_id = created.json()["entry"]["id"]
    assert client.post(f"/api/worker/{node_id}/actions", json={"action": "提交注册"}).json()["ok"] is True

    collected = client.post("/api/heartbeat/collect", json={
        "events": [{"事件编号": "FLAT-1", "节点编号": "WORK-FLAT", "租约秒数": 60}],
    })
    assert collected.json()["ok"] is True
    ingested = client.post("/api/heartbeat/ingest", json={"request_id": "FLAT-REQ"})
    assert ingested.json()["ok"] is True
    assert "WORK-FLAT" in [item["节点编号"] for item in client.get("/api/schedulable").json()["items"]]

    routing_response = client.post("/api/routing/requests", json={
        "requests": [{"请求编号": "FLAT-Q", "目标域名": "news.example.com"}],
    })
    assert routing_response.json()["ok"] is True
    assert client.get("/api/routing/verify").json()["ok"] is True
    assert client.get("/api/worker/export").status_code == 200
