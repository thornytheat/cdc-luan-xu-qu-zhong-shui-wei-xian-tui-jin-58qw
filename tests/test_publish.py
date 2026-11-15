"""模块 8 原子发布：全部选中表校验通过一次切换、失败保留旧版、同段重试不推进发布次数、切换前后对照。"""
from __future__ import annotations

import json

import pytest

from app import publish, segments


def seg(source: str, no: int, bad: bool = False) -> list[dict]:
    return segments.sample_events(source, no, bad=bad)


ALL_TABLES = ["orders", "customers", "inventory"]


def upload(client, source: str, no: int, bad: bool = False):
    return client.post("/api/segments",
                       json={"source": source, "segment_no": no, "events": seg(source, no, bad)})


def test_publish_success_switches_and_exports(client):
    response = client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1})
    assert response.status_code == 200
    body = response.json()
    assert body["switched"] is True
    assert body["publish_no"] == 1
    assert body["summary"]["events"] == 180
    assert body["summary"]["watermark"] == "2026-11-10T00:14:00Z"

    # 切换与导出一体：快照随发布可直接下载
    export = client.get("/api/publishes/1/export")
    assert export.status_code == 200
    assert "attachment" in export.headers["content-disposition"]
    snap = json.loads(export.content)
    assert snap["export"]["run_id"] == body["run_id"]
    assert snap["watermark"]["watermark"] == "2026-11-10T00:14:00Z"
    assert set(snap["tables"]) == set(ALL_TABLES)

    status = client.get("/api/publishes").json()
    assert status["current"]["publish_no"] == 1
    assert status["publish_count"] == 1
    assert status["previous"] is None


def test_blocked_gate_keeps_old_version(client):
    client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1})
    response = client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 2})
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["kept_publish_no"] == 1
    assert any("阻断" in reason for reason in detail["reasons"])

    # 失败保留旧版：现网不动、发布次数不推进、失败尝试留痕
    status = client.get("/api/publishes").json()
    assert status["current"]["publish_no"] == 1
    assert status["current"]["schema_version"] == 1
    assert status["publish_count"] == 1
    assert any(not att["ok"] for att in status["attempts"])


def test_quarantined_segment_blocks_publish(client):
    upload(client, "orders", 2)                      # 跳号：缺口 [1]、seg2 隔离
    response = client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1})
    assert response.status_code == 409
    reasons = response.json()["detail"]["reasons"]
    assert any("orders" in reason and "缺口" in reason for reason in reasons)
    assert client.get("/api/publishes").json()["publish_count"] == 0


def test_failed_segment_blocks_publish(client):
    # 坏事件（seg4.bad 的内容作为 seg1 上传）：seg1 失败停住
    client.post("/api/segments", json={"source": "inventory", "segment_no": 1,
                                       "events": seg("inventory", 4, bad=True)})
    response = client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1})
    assert response.status_code == 409
    assert any("失败停住" in reason for reason in response.json()["detail"]["reasons"])
    assert client.get("/api/publishes").json()["publish_count"] == 0


def test_same_content_republish_does_not_advance_count(client):
    first = client.post("/api/publish", json={"tables": ["orders"], "schema_version": 1}).json()
    assert first["switched"] is True
    second = client.post("/api/publish", json={"tables": ["orders"], "schema_version": 1}).json()
    assert second["switched"] is False
    assert second["publish_no"] == 1
    assert "内容未变化" in second["reason"]
    assert "不推进发布次数" in second["message"]
    assert client.get("/api/publishes").json()["publish_count"] == 1


def test_same_segment_retry_adds_no_event_and_keeps_publish_count(client):
    """核心验收：同段重试不新增事件，内容摘要不变的发布不推进发布次数。"""
    first = client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1}).json()
    assert first["publish_no"] == 1

    # 补传 orders seg1 → 内容变化 → 发布 #2
    upload(client, "orders", 1)
    second = client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1}).json()
    assert second["switched"] is True and second["publish_no"] == 2
    assert client.get("/health").json()["events"] == 186

    # 同段重试：不新增事件
    dup = upload(client, "orders", 1).json()
    assert dup["status"] == "duplicate"
    assert client.get("/health").json()["events"] == 186

    # 再发布：内容摘要未变 → 不推进发布次数，现网仍是 #2
    third = client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1}).json()
    assert third["switched"] is False
    status = client.get("/api/publishes").json()
    assert status["publish_count"] == 2
    assert status["current"]["publish_no"] == 2


def test_publish_after_gap_fill_and_before_after_comparison(client):
    client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1})

    # 乱序补传 orders：seg1 确认 → seg3 隔离（此时发布被拒）→ seg2 补齐级联
    upload(client, "orders", 1)
    upload(client, "orders", 3)
    refused = client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1})
    assert refused.status_code == 409
    upload(client, "orders", 2)

    body = client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1}).json()
    assert body["switched"] is True and body["publish_no"] == 2
    assert body["previous_publish_no"] == 1

    # 切换前后对照：orders 行数 13 → 17（3 个新键 + O9002 墓碑复活），水位线越过缺口推进
    comp = client.get("/api/publishes").json()["comparison"]
    assert comp["before_no"] == 1 and comp["after_no"] == 2
    orders = comp["per_table"]["orders"]
    assert orders["before"]["rows"] == 13
    assert orders["after"]["rows"] == 17
    assert orders["before"]["tombstones"] == 1
    assert orders["after"]["tombstones"] == 0
    assert orders["before"]["watermark"] == "2026-11-10T00:14:00Z"
    assert orders["after"]["watermark"] == "2026-11-11T09:01:00Z"
    assert comp["overall"]["events_before"] == 180
    assert comp["overall"]["events_after"] == 198


def test_publish_validation_errors(client):
    assert client.post("/api/publish", json={"tables": ["ghost"], "schema_version": 1}).status_code == 400
    assert client.post("/api/publish", json={"tables": [], "schema_version": 1}).status_code == 400
    assert client.post("/api/publish", json={"tables": ["orders"], "schema_version": 9}).status_code == 422


def test_export_unknown_publish_is_404(client):
    assert client.get("/api/publishes/99/export").status_code == 404


def test_publish_run_is_replayable_via_existing_run_apis(client):
    """发布产生的运行与手工合并的运行同构：结果页 / 快照 / 详情都能看。"""
    body = client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1}).json()
    run_id = body["run_id"]
    assert client.get(f"/result/{run_id}").status_code == 200
    detail = client.get(f"/api/runs/{run_id}").json()
    assert detail["summary"]["events"] == 180
    assert client.get(f"/api/runs/{run_id}/snapshot").status_code == 200


def test_segments_page_publish_sections(client):
    page = client.get("/segments")
    assert page.status_code == 200
    assert "现网还未发布" in page.text

    client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1})
    page = client.get("/segments")
    assert "当前现网 · 发布 #1" in page.text
    assert "切换前后对照" in page.text
    assert "首次发布" in page.text
    assert "发布历史与尝试日志" in page.text


def test_reset_clears_publish_state(client):
    client.post("/api/publish", json={"tables": ALL_TABLES, "schema_version": 1})
    stats = client.post("/api/segments/reset").json()
    assert stats["deleted_publishes"] == 1
    status = client.get("/api/publishes").json()
    assert status["current"] is None
    assert status["publish_count"] == 0
