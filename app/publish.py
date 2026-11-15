"""模块 8 · 原子发布：全部选中表校验通过才一次切换现网并导出，失败保留旧版。

校验（三条全部通过才允许切换，任一不过即整体保留旧版）：
1. 选中表无缺口 / 隔离段 / 失败段 —— 前后段已统一确认，孤立段不会抢先上现网；
2. 选中表均有归档事件；
3. schema 门禁逐表无「阻断」结论。

切换与导出在同一事务：运行落库 + 发布记录（内含快照 JSON）+ 现网指针一起提交，
任一失败整体回滚，现网保留旧版。发布次数 = publishes 表自增主键，只有成功切换才 +1；
内容摘要未变化的重复发布（如同段重试后再点发布）不推进发布次数。
"""
from __future__ import annotations

import hashlib
import json

from . import runner, segments, settings, snapshot, store


class PublishError(ValueError):
    """校验未通过（路由转成 409）；reasons 逐条列出，现网保留旧版。"""

    def __init__(self, reasons: list[str]):
        super().__init__("；".join(reasons))
        self.reasons = reasons


def content_digest(conn, tables: list[str], schema_version: int) -> str:
    """发布内容摘要：选中表 + schema 版本 + seed 版本 + 各来源已确认段的摘要链。

    同段重试不会产生新的已确认段，因此摘要不变 → 重复发布不推进发布次数。
    """
    parts = {
        "tables": tables,
        "schema_version": int(schema_version),
        "seed_version": store.get_meta(conn, "seed_version"),
        "segments": {
            table: [
                {"no": seg["segment_no"], "digest": seg["digest"]}
                for seg in store.list_segments(conn, table, store.SEGMENT_COMMITTED)
            ]
            for table in tables
        },
    }
    payload = json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _validate(conn, tables: list[str], schema_version: int) -> list[str]:
    """发布前三条校验，返回未通过原因列表（空 = 全部通过）。"""
    reasons: list[str] = []

    # 1) 前后段统一确认：选中表不能还有缺口 / 隔离段 / 失败段
    for source, pending in segments.pending_by_source(conn, tables).items():
        bits = []
        if pending["gaps"]:
            bits.append(f"缺口 {pending['gaps']}")
        if pending["quarantined"]:
            bits.append(f"隔离待补 {pending['quarantined']}")
        if pending["failed"]:
            bits.append(f"失败停住 {pending['failed']}")
        reasons.append(
            f"{source} 还有未统一确认的段（{'、'.join(bits)}）："
            "孤立段未补齐前不允许切换现网，否则部分表会抢先上线、水位越过缺口"
        )

    # 2) 选中表均有归档事件
    for table in tables:
        if store.count_table_events(conn, table) == 0:
            reasons.append(f"{table} 还没有归档事件，请先加载样例数据")

    if reasons:
        return reasons

    # 3) schema 门禁逐表无阻断（无 findings 的表即「无变更、可放行」；未覆盖才按未通过处理）
    base_doc, target_doc = runner.load_schemas(schema_version)
    events_by_table = {table: store.fetch_events(conn, table) for table in tables}
    result = runner.analyze(tables, events_by_table, base_doc, target_doc)
    gate = result["gate"]
    for table in tables:
        if table in gate["skipped_tables"]:
            reasons.append(f"{table} 未被门禁覆盖（schema 缺少表定义），按未通过处理")
            continue
        bucket = gate["tables"].get(table)
        if bucket and bucket["verdict"] == "blocked":
            reasons.append(
                f"{table} 的 schema 门禁有 {bucket['counts']['blocked']} 条阻断"
                "（详见结果页门禁分段），校验未通过"
            )
    return reasons


def publish(conn, tables: list[str], schema_version: int) -> dict:
    """校验并尝试原子切换；校验失败抛 PublishError（旧版保留），成功返回发布结果。"""
    if schema_version not in (1, 2):
        raise runner.RunError(f"schema 版本只支持 1 或 2，收到 {schema_version}")
    ordered = runner.validate_tables(tables)

    reasons = _validate(conn, ordered, schema_version)
    if reasons:
        current = store.current_publish(conn)
        store.record_attempt(conn, ordered, schema_version, False,
                             {"reasons": reasons, "kept_publish_no": current["publish_no"] if current else None})
        raise PublishError(reasons)

    digest = content_digest(conn, ordered, schema_version)
    current = store.current_publish(conn)

    # 同内容重复发布（含同段重试后再点发布）：不新增发布、不推进发布次数
    if current and current["digest"] == digest:
        store.record_attempt(conn, ordered, schema_version, True,
                             {"note": "内容未变化，现网已是最新", "publish_no": current["publish_no"]})
        return {
            "switched": False,
            "reason": "内容未变化，现网已是最新",
            "publish_no": current["publish_no"],
            "run_id": current["run_id"],
            "digest": digest,
            "message": f"内容摘要与现网发布 #{current['publish_no']} 一致："
                       "不新增发布记录、不推进发布次数",
        }

    # 重新分析（校验已跑过一遍，这里取同一口径的完整结果用于落库与导出）
    base_doc, target_doc = runner.load_schemas(schema_version)
    events_by_table = {table: store.fetch_events(conn, table) for table in ordered}
    result = runner.analyze(ordered, events_by_table, base_doc, target_doc)
    run = {
        "run_id": runner.new_run_id(),
        "created_at": store.now_iso(),
        "tables": ordered,
        "schema_version": int(schema_version),
        "summary": result["summary"],
        "result": result,
    }
    snapshot_json = snapshot.dumps(snapshot.build_snapshot(run))

    previous = store.current_publish(conn)
    try:
        store.save_run_tx(conn, run)
        publish_no = store.insert_publish_tx(
            conn, run["run_id"], run["created_at"], ordered, schema_version,
            digest, run["summary"], snapshot_json,
        )
        store.set_meta_tx(conn, "production_publish_no", str(publish_no))
        conn.commit()
    except Exception:
        conn.rollback()  # 任一失败整体回滚：现网保留旧版
        raise

    store.record_attempt(conn, ordered, schema_version, True,
                         {"publish_no": publish_no, "switched": True, "run_id": run["run_id"]})
    return {
        "switched": True,
        "publish_no": publish_no,
        "run_id": run["run_id"],
        "created_at": run["created_at"],
        "tables": ordered,
        "schema_version": int(schema_version),
        "digest": digest,
        "summary": run["summary"],
        "previous_publish_no": previous["publish_no"] if previous else None,
        "export_url": f"/api/publishes/{publish_no}/export",
        "message": f"全部选中表校验通过：已原子切换到发布 #{publish_no}"
                   f"（run {run['run_id']}），快照已随发布一并导出",
    }


def _table_brief(run: dict, table: str) -> dict:
    payload = run["result"]["tables"].get(table)
    if payload is None:
        return {"rows": None, "tombstones": None, "watermark": None, "open_keys": None}
    return {
        "rows": payload["row_count"],
        "tombstones": payload["tombstone_count"],
        "watermark": payload["watermark"]["watermark"],
        "open_keys": payload["watermark"]["open_keys"],
    }


def build_comparison(conn, current: dict, previous: dict | None) -> dict | None:
    """切换前后对照：逐表（行数/墓碑/水位线）+ 全局（事件/水位线/门禁/未闭合键）。"""
    cur_run = store.get_run(conn, current["run_id"])
    if cur_run is None:
        return None
    prev_run = store.get_run(conn, previous["run_id"]) if previous else None
    tables = list(dict.fromkeys(cur_run["tables"] + (prev_run["tables"] if prev_run else [])))
    per_table = {}
    for table in tables:
        cur = _table_brief(cur_run, table)
        prev = _table_brief(prev_run, table) if prev_run else {"rows": None, "tombstones": None, "watermark": None, "open_keys": None}
        per_table[table] = {
            "label": settings.TABLE_LABELS.get(table, table),
            "before": prev,
            "after": cur,
            "rows_delta": (cur["rows"] - prev["rows"]) if cur["rows"] is not None and prev["rows"] is not None else None,
        }
    cur_summary, prev_summary = cur_run["summary"], (prev_run["summary"] if prev_run else {})
    return {
        "before_no": previous["publish_no"] if previous else None,
        "after_no": current["publish_no"],
        "before_run_id": previous["run_id"] if previous else None,
        "after_run_id": current["run_id"],
        "before_created_at": previous["created_at"] if previous else None,
        "after_created_at": current["created_at"],
        "per_table": per_table,
        "overall": {
            "events_before": prev_summary.get("events"),
            "events_after": cur_summary.get("events"),
            "watermark_before": prev_summary.get("watermark"),
            "watermark_after": cur_summary.get("watermark"),
            "open_keys_before": prev_summary.get("open_keys"),
            "open_keys_after": cur_summary.get("open_keys"),
            "gate_before": prev_summary.get("gate_verdict"),
            "gate_after": cur_summary.get("gate_verdict"),
        },
    }


def status(conn) -> dict:
    """发布状态：现网当前版、上一版、切换前后对照、发布历史与尝试日志。"""
    current = store.current_publish(conn)
    previous = store.previous_publish(conn, current["publish_no"]) if current else None
    comparison = build_comparison(conn, current, previous) if current else None
    return {
        "current": current,
        "previous": previous,
        "comparison": comparison,
        "publish_count": store.publish_count(conn),
        "history": store.list_publishes(conn, limit=20),
        "attempts": store.list_attempts(conn, limit=20),
    }


def get_export(conn, publish_no: int) -> tuple[dict, str] | None:
    """取某次发布的导出快照（随切换同事务写入，保证与现网版本一致）。"""
    publish = store.get_publish(conn, publish_no, with_snapshot=True)
    if publish is None:
        return None
    return publish, publish["snapshot_json"]
