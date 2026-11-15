"""模块 7 分段补传：连续段号核对、孤立段暂存隔离、缺口补齐级联、坏事件整段停住、同段重试幂等。"""
from __future__ import annotations

import pytest

from app import runner, segments, settings, store


def seg(source: str, no: int, bad: bool = False) -> list[dict]:
    return segments.sample_events(source, no, bad=bad)


def watermark(conn, tables=("orders",)) -> str | None:
    """只读演算当前归档水位线（与页面「实时水位线」同口径）。"""
    events = {t: store.fetch_events(conn, t) for t in tables}
    base, _ = runner.load_schemas(1)
    return runner.analyze(list(tables), events, base, base)["watermark"]["watermark"]


def board_source(conn, source: str) -> dict:
    return next(s for s in segments.board(conn)["sources"] if s["source"] == source)


# --- 连续段号核对与确认 -------------------------------------------------------


def test_in_order_commit_archives_events(conn):
    result = segments.ingest(conn, "orders", 1, seg("orders", 1))
    assert result["status"] == "committed"
    assert result["committed"] == [1]
    assert result["cascaded"] == []
    assert result["next_expected"] == 2
    assert store.total_events(conn) == 186          # 180 + 6
    assert store.count_segment_events(conn, "orders") == 6
    assert board_source(conn, "orders")["committed_segments"] == [1]


def test_unknown_source_and_bad_segment_no_rejected(conn):
    with pytest.raises(segments.SegmentError, match="未知的来源"):
        segments.ingest(conn, "ghost", 1, seg("orders", 1))
    with pytest.raises(segments.SegmentError, match="段号"):
        segments.ingest(conn, "orders", 0, seg("orders", 1))
    with pytest.raises(segments.SegmentError, match="至少"):
        segments.ingest(conn, "orders", 1, [])


# --- 孤立段暂存隔离：不进归档、不推水位线；补齐缺口后级联合并 --------------------


def test_out_of_order_quarantined_then_cascade_after_gap_fill(conn):
    segments.ingest(conn, "orders", 1, seg("orders", 1))
    result = segments.ingest(conn, "orders", 3, seg("orders", 3))

    # 跳号到达 → 隔离待补：事件不进归档，缺口 [2]
    assert result["status"] == "quarantined"
    assert result["gaps"] == [2]
    assert store.total_events(conn) == 186
    assert store.count_segment_events(conn, "orders") == 6

    # 孤立段被挡住：水位线仍是基础水位线（O1009 拖在 00:14）
    assert watermark(conn) == "2026-11-10T00:14:00Z"

    # 补齐缺口 → seg2 确认并级联合并 seg3（同一事务）
    result = segments.ingest(conn, "orders", 2, seg("orders", 2))
    assert result["status"] == "committed"
    assert result["committed"] == [2, 3]
    assert result["cascaded"] == [3]
    assert result["gaps"] == [] and result["quarantined"] == []
    assert store.total_events(conn) == 198
    assert store.committed_upto(conn, "orders") == 3

    # 缺口补齐后水位线才推进到段事件时代
    assert watermark(conn) == "2026-11-11T09:01:00Z"


def test_quarantine_survives_reconnect(conn):
    """重启（重开同一 SQLite 文件）后隔离段仍在隔离区，不会抢先出现在现网。"""
    segments.ingest(conn, "orders", 1, seg("orders", 1))
    segments.ingest(conn, "orders", 3, seg("orders", 3))
    conn.close()

    conn2 = store.connect()
    try:
        store.init_db(conn2)
        orders = board_source(conn2, "orders")
        assert [s["segment_no"] for s in orders["quarantined"]] == [3]
        assert orders["gaps"] == [2]
        assert orders["next_expected"] == 2
        assert store.count_segment_events(conn2, "orders") == 6
        assert store.total_events(conn2) == 186
    finally:
        conn2.close()


def test_quarantined_segment_replaceable_before_commit(conn):
    segments.ingest(conn, "orders", 1, seg("orders", 1))
    segments.ingest(conn, "orders", 3, seg("orders", 3))
    replacement = seg("orders", 3) + [seg("orders", 4)[-1]]   # 多塞一条 O2005 v1 c
    result = segments.ingest(conn, "orders", 3, replacement)
    assert result["status"] == "quarantined"
    assert "替换" in result["message"]
    assert store.get_segment(conn, "orders", 3)["event_count"] == 7

    # 补齐 seg2 后，级联确认的是替换后的内容
    result = segments.ingest(conn, "orders", 2, seg("orders", 2))
    assert result["cascaded"] == [3]
    assert store.count_segment_events(conn, "orders") == 6 + 6 + 7


# --- 坏事件整段停住 ------------------------------------------------------------


def test_bad_event_stalls_whole_segment(conn):
    segments.ingest(conn, "orders", 1, seg("orders", 1))
    with pytest.raises(segments.SegmentInvalid, match="第 3 行"):
        segments.ingest(conn, "orders", 4, seg("orders", 4, bad=True))

    # 整段停住：一条不入库、期望段号不动、failed 留痕带行号
    assert store.total_events(conn) == 186
    assert store.committed_upto(conn, "orders") == 1
    failed = store.list_segments(conn, "orders", store.SEGMENT_FAILED)
    assert [s["segment_no"] for s in failed] == [4]
    assert "第 3 行" in failed[0]["error"]
    assert "op 必须是 c/u/d 之一" in failed[0]["error"]

    # 修复后按同段号重传：先补齐 2/3，seg4 随即确认，failed 记录被确认覆盖
    segments.ingest(conn, "orders", 2, seg("orders", 2))
    segments.ingest(conn, "orders", 3, seg("orders", 3))
    result = segments.ingest(conn, "orders", 4, seg("orders", 4))
    assert result["status"] == "committed"
    assert store.committed_upto(conn, "orders") == 4
    assert store.list_segments(conn, "orders", store.SEGMENT_FAILED) == []
    assert store.total_events(conn) == 186 + 6 + 6 + 5


# --- 同段重试幂等 --------------------------------------------------------------


def test_duplicate_retry_is_noop(conn):
    segments.ingest(conn, "orders", 1, seg("orders", 1))
    again = segments.ingest(conn, "orders", 1, seg("orders", 1))
    assert again["status"] == "duplicate"
    assert "不新增事件" in again["message"]
    assert store.total_events(conn) == 186
    assert store.committed_upto(conn, "orders") == 1


def test_committed_segment_content_conflict_rejected(conn):
    segments.ingest(conn, "orders", 1, seg("orders", 1))
    tampered = seg("orders", 1)
    tampered[0]["after"]["amount"] = 999999
    with pytest.raises(segments.SegmentConflict, match="不可改写"):
        segments.ingest(conn, "orders", 1, tampered)
    assert store.total_events(conn) == 186          # 冲突不落任何事件


# --- 看板与重置 -----------------------------------------------------------------


def test_board_and_reset(conn):
    segments.ingest(conn, "orders", 1, seg("orders", 1))
    segments.ingest(conn, "orders", 3, seg("orders", 3))
    segments.ingest(conn, "customers", 2, seg("customers", 2))   # customers 缺口 [1]
    board = segments.board(conn)
    assert board["totals"]["committed"] == 1
    assert board["totals"]["quarantined"] == 2
    assert board["totals"]["gaps"] == 2             # orders [2] + customers [1]

    stats = segments.reset(conn)
    assert stats["deleted_events"] == 6
    assert stats["deleted_segments"] == 3
    assert stats["total_events"] == 180
    assert segments.board(conn)["totals"] == {"committed": 0, "quarantined": 0, "failed": 0, "gaps": 0}


# --- HTTP 接口 ------------------------------------------------------------------


def test_api_ingest_board_and_error_paths(client):
    ok = client.post("/api/segments", json={"source": "orders", "segment_no": 1, "events": seg("orders", 1)})
    assert ok.status_code == 200 and ok.json()["status"] == "committed"

    quar = client.post("/api/segments", json={"source": "orders", "segment_no": 3, "events": seg("orders", 3)})
    assert quar.json()["status"] == "quarantined"

    board = client.get("/api/segments").json()
    orders = next(s for s in board["sources"] if s["source"] == "orders")
    assert orders["gaps"] == [2]
    assert [s["segment_no"] for s in orders["quarantined"]] == [3]

    # 坏事件 → 422，整段停住；修复后同段号重传 → 确认
    bad = client.post("/api/segments", json={"source": "inventory", "segment_no": 1,
                                             "events": seg("inventory", 4, bad=True)})
    assert bad.status_code == 422
    assert "第 3 行" in bad.json()["detail"]
    fixed = client.post("/api/segments", json={"source": "inventory", "segment_no": 1,
                                               "events": seg("inventory", 1)})
    assert fixed.status_code == 200 and fixed.json()["status"] == "committed"

    # 同段重试 → duplicate；内容改写 → 409；未知来源 → 400；空段 → 400
    dup = client.post("/api/segments", json={"source": "inventory", "segment_no": 1,
                                             "events": seg("inventory", 1)})
    assert dup.json()["status"] == "duplicate"
    tampered = seg("inventory", 1)
    tampered[0]["after"]["on_hand"] = 1
    conflict = client.post("/api/segments", json={"source": "inventory", "segment_no": 1,
                                                  "events": tampered})
    assert conflict.status_code == 409
    assert client.post("/api/segments", json={"source": "ghost", "segment_no": 1,
                                              "events": []}).status_code == 400
    assert client.post("/api/segments", json={"source": "orders", "segment_no": 9,
                                              "events": []}).status_code == 400


def test_api_ingest_jsonl_text_and_syntax_error(client):
    text = (settings.SEGMENTS_DIR / "customers.seg1.jsonl").read_text(encoding="utf-8")
    ok = client.post("/api/segments", json={"source": "customers", "segment_no": 1,
                                            "events_jsonl": text})
    assert ok.status_code == 200 and ok.json()["status"] == "committed"

    bad = client.post("/api/segments", json={"source": "customers", "segment_no": 2,
                                             "events_jsonl": "{not json}"})
    assert bad.status_code == 422
    customers = next(s for s in client.get("/api/segments").json()["sources"]
                     if s["source"] == "customers")
    assert [f["segment_no"] for f in customers["failed"]] == [2]


def test_segments_page_shows_gaps_quarantine_and_watermark(client):
    client.post("/api/segments", json={"source": "orders", "segment_no": 1, "events": seg("orders", 1)})
    client.post("/api/segments", json={"source": "orders", "segment_no": 3, "events": seg("orders", 3)})
    page = client.get("/segments")
    assert page.status_code == 200
    text = page.text
    assert "段状态看板" in text
    assert "隔离待补" in text and "seg3" in text
    assert "缺口" in text and "seg2" in text
    assert "2026-11-10T00:14:00Z" in text            # 实时水位线被孤立段挡住
