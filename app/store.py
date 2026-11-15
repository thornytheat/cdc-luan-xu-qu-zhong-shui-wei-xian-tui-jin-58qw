"""SQLite 访问（薄封装）。

表：
- events           归档事件；主键 (source, line_no) 保证重复启动/重复 seed 不会重复导入
- runs             每次合并运行的元信息 + 完整结果 JSON（结果页、复核页、快照下载共用）
- meta             seed 版本、现网发布指针等键值
- segments         分段补传的段状态（committed/quarantined/failed），隔离段 payload 也在这里
- publishes        成功切换的发布（含导出快照 JSON）；publish_no 即发布次数，只增不减
- publish_attempts 每次发布尝试的审计日志（成功/失败/内容未变化）

约定：带 `_tx` 后缀的函数不在内部 commit，由调用方在同一事务里统一提交/回滚
（原子发布、级联确认段都依赖这一点）；不带后缀的公开函数保持「调用即提交」。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from . import settings
from .archive import Event, load_events_file, parse_ts

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  source         TEXT    NOT NULL,
  line_no        INTEGER NOT NULL,
  table_name     TEXT    NOT NULL,
  pk             TEXT    NOT NULL,
  version        INTEGER NOT NULL,
  ts             TEXT    NOT NULL,
  op             TEXT    NOT NULL,
  after_json     TEXT    NOT NULL,
  schema_version INTEGER NOT NULL,
  PRIMARY KEY (source, line_no)
);
CREATE INDEX IF NOT EXISTS idx_events_table ON events(table_name, line_no);

CREATE TABLE IF NOT EXISTS runs (
  run_id         TEXT PRIMARY KEY,
  created_at     TEXT NOT NULL,
  tables_json    TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  summary_json   TEXT NOT NULL,
  payload_json   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_created ON runs(created_at DESC);

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
  source       TEXT    NOT NULL,
  segment_no   INTEGER NOT NULL,
  status       TEXT    NOT NULL,          -- committed / quarantined / failed
  digest       TEXT    NOT NULL,
  event_count  INTEGER NOT NULL,
  error        TEXT,
  payload_json TEXT    NOT NULL DEFAULT '[]',  -- 隔离/失败段的原始事件（待补/留证）
  created_at   TEXT    NOT NULL,
  committed_at TEXT,
  PRIMARY KEY (source, segment_no)
);
CREATE INDEX IF NOT EXISTS idx_segments_status ON segments(source, status, segment_no);

CREATE TABLE IF NOT EXISTS publishes (
  publish_no     INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id         TEXT NOT NULL,
  created_at     TEXT NOT NULL,
  tables_json    TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  digest         TEXT NOT NULL,
  summary_json   TEXT NOT NULL,
  snapshot_json  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS publish_attempts (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at     TEXT NOT NULL,
  tables_json    TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  ok             INTEGER NOT NULL,
  detail_json    TEXT NOT NULL
);
"""


def connect() -> sqlite3.Connection:
    settings.ensure_data_dir()
    conn = sqlite3.connect(settings.db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- meta ------------------------------------------------------------------


def set_meta_tx(conn: sqlite3.Connection, key: str, value: str) -> None:
    """事务内版本：不 commit，由调用方统一提交。"""
    conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (key, value))


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    set_meta_tx(conn, key, value)
    conn.commit()


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


# --- events ----------------------------------------------------------------


def archive_events_tx(conn: sqlite3.Connection, events: list[Event]) -> dict:
    """事务内版本：幂等归档但不 commit（级联确认段时由调用方统一提交）。"""
    inserted = 0
    for event in events:
        row = event.to_row()
        cursor = conn.execute(
            """INSERT OR IGNORE INTO events
               (source, line_no, table_name, pk, version, ts, op, after_json, schema_version)
               VALUES (:source, :line_no, :table, :pk, :version, :ts, :op, :after_json, :schema_version)""",
            row,
        )
        inserted += cursor.rowcount
    return {"attempted": len(events), "inserted": inserted, "skipped": len(events) - inserted}


def archive_events(conn: sqlite3.Connection, events: list[Event]) -> dict:
    """幂等归档：主键冲突一律忽略（重复启动/重复 seed 不产生脏数据）。"""
    stats = archive_events_tx(conn, events)
    conn.commit()
    return stats


def seed_sample(conn: sqlite3.Connection, events_dir: Path | None = None) -> dict:
    """把 sample_data/events/*.jsonl 逐表幂等导入；返回本次导入统计。"""
    events_dir = events_dir or settings.EVENTS_DIR
    total = {"attempted": 0, "inserted": 0, "skipped": 0, "tables": {}}
    for item in settings.TABLES:
        path = events_dir / item["file"]
        events = load_events_file(path, item["name"])
        stats = archive_events(conn, events)
        total["attempted"] += stats["attempted"]
        total["inserted"] += stats["inserted"]
        total["skipped"] += stats["skipped"]
        total["tables"][item["name"]] = stats
    set_meta(conn, "seed_version", settings.SEED_VERSION)
    set_meta(conn, "seeded_at", now_iso())
    total["seed_version"] = settings.SEED_VERSION
    return total


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        table=row["table_name"],
        line_no=int(row["line_no"]),
        pk=row["pk"],
        version=int(row["version"]),
        ts=parse_ts(row["ts"]),
        op=row["op"],
        after=json.loads(row["after_json"]),
        schema_version=int(row["schema_version"]),
        source=row["source"],
    )


def fetch_events(conn: sqlite3.Connection, table: str) -> list[Event]:
    """按文件行号（= 到达顺序）返回某表归档事件。"""
    rows = conn.execute(
        "SELECT * FROM events WHERE table_name=? ORDER BY source, line_no", (table,)
    ).fetchall()
    return [_row_to_event(row) for row in rows]


def event_counts(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT table_name, COUNT(*) AS n FROM events GROUP BY table_name"
    ).fetchall()
    return {row["table_name"]: int(row["n"]) for row in rows}


def total_events(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()
    return int(row["n"])


def count_table_events(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM events WHERE table_name=?", (table,)
    ).fetchone()
    return int(row["n"])


def count_segment_events(conn: sqlite3.Connection, table: str) -> int:
    """某表已由补传段确认归档的事件数（source 形如 orders.jsonl#seg001）。"""
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM events WHERE table_name=? AND source LIKE '%#seg%'",
        (table,),
    ).fetchone()
    return int(row["n"])


def delete_segment_events(conn: sqlite3.Connection) -> int:
    """重置补传：删除所有由段确认进来的归档事件（基础 seed 的 source 不含 #seg）。"""
    cursor = conn.execute("DELETE FROM events WHERE source LIKE '%#seg%'")
    conn.commit()
    return int(cursor.rowcount)


# --- runs ------------------------------------------------------------------


def save_run_tx(conn: sqlite3.Connection, run: dict) -> None:
    """事务内版本：不 commit（原子发布时与发布记录、现网指针一起提交）。"""
    conn.execute(
        """INSERT OR REPLACE INTO runs
           (run_id, created_at, tables_json, schema_version, summary_json, payload_json)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            run["run_id"],
            run["created_at"],
            json.dumps(run["tables"], ensure_ascii=False),
            int(run["schema_version"]),
            json.dumps(run["summary"], ensure_ascii=False),
            json.dumps(run["result"], ensure_ascii=False),
        ),
    )


def save_run(conn: sqlite3.Connection, run: dict) -> None:
    save_run_tx(conn, run)
    conn.commit()


def get_run(conn: sqlite3.Connection, run_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        return None
    return {
        "run_id": row["run_id"],
        "created_at": row["created_at"],
        "tables": json.loads(row["tables_json"]),
        "schema_version": int(row["schema_version"]),
        "summary": json.loads(row["summary_json"]),
        "result": json.loads(row["payload_json"]),
    }


def list_runs(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = conn.execute(
        "SELECT run_id, created_at, tables_json, schema_version, summary_json FROM runs "
        "ORDER BY created_at DESC, run_id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    out = []
    for row in rows:
        out.append(
            {
                "run_id": row["run_id"],
                "created_at": row["created_at"],
                "tables": json.loads(row["tables_json"]),
                "schema_version": int(row["schema_version"]),
                "summary": json.loads(row["summary_json"]),
            }
        )
    return out


def run_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()
    return int(row["n"])


def event_overview(conn: sqlite3.Connection) -> dict[str, dict]:
    """首页概览：按表统计 op / schema 版本 / 键数 / 时间区间（全部来自归档表）。"""
    out: dict[str, dict] = {}
    for item in settings.TABLES:
        table = item["name"]
        rows = conn.execute(
            "SELECT op, schema_version, pk, ts FROM events WHERE table_name=?", (table,)
        ).fetchall()
        ops: dict[str, int] = {}
        versions: dict[str, int] = {}
        stamps: list[str] = []
        keys: set[str] = set()
        for row in rows:
            ops[row["op"]] = ops.get(row["op"], 0) + 1
            versions[str(row["schema_version"])] = versions.get(str(row["schema_version"]), 0) + 1
            keys.add(row["pk"])
            stamps.append(row["ts"])
        out[table] = {
            "table": table,
            "label": item["label"],
            "desc": item["desc"],
            "file": item["file"],
            "events": len(rows),
            "ops": ops,
            "schema_versions": versions,
            "keys": len(keys),
            "first_ts": min(stamps) if stamps else None,
            "last_ts": max(stamps) if stamps else None,
        }
    return out


# --- segments（模块 7 · 分段补传） --------------------------------------------

SEGMENT_COMMITTED = "committed"
SEGMENT_QUARANTINED = "quarantined"
SEGMENT_FAILED = "failed"


def _row_to_segment(row: sqlite3.Row) -> dict:
    return {
        "source": row["source"],
        "segment_no": int(row["segment_no"]),
        "status": row["status"],
        "digest": row["digest"],
        "event_count": int(row["event_count"]),
        "error": row["error"],
        "payload_json": row["payload_json"],
        "created_at": row["created_at"],
        "committed_at": row["committed_at"],
    }


def upsert_segment_tx(
    conn: sqlite3.Connection,
    source: str,
    segment_no: int,
    status: str,
    digest: str,
    event_count: int,
    error: str | None,
    payload_json: str,
    committed_at: str | None = None,
) -> None:
    """事务内版本：写入/覆盖一个段的状态（同一段号重传会覆盖旧记录）。"""
    conn.execute(
        """INSERT OR REPLACE INTO segments
           (source, segment_no, status, digest, event_count, error, payload_json, created_at, committed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            source,
            int(segment_no),
            status,
            digest,
            int(event_count),
            error,
            payload_json,
            now_iso(),
            committed_at,
        ),
    )


def get_segment(conn: sqlite3.Connection, source: str, segment_no: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM segments WHERE source=? AND segment_no=?", (source, int(segment_no))
    ).fetchone()
    return _row_to_segment(row) if row else None


def list_segments(
    conn: sqlite3.Connection, source: str | None = None, status: str | None = None
) -> list[dict]:
    sql = "SELECT * FROM segments"
    clauses, params = [], []
    if source is not None:
        clauses.append("source=?")
        params.append(source)
    if status is not None:
        clauses.append("status=?")
        params.append(status)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY source, segment_no"
    return [_row_to_segment(row) for row in conn.execute(sql, params).fetchall()]


def committed_upto(conn: sqlite3.Connection, source: str) -> int:
    """该来源已连续确认到的段号（确认只发生在「下一期望段」，因此已确认段恒为 1..N 前缀）。"""
    row = conn.execute(
        "SELECT MAX(segment_no) AS n FROM segments WHERE source=? AND status=?",
        (source, SEGMENT_COMMITTED),
    ).fetchone()
    return int(row["n"]) if row["n"] is not None else 0


def delete_all_segments(conn: sqlite3.Connection) -> int:
    cursor = conn.execute("DELETE FROM segments")
    conn.commit()
    return int(cursor.rowcount)


# --- publishes（模块 8 · 原子发布） -------------------------------------------


def insert_publish_tx(
    conn: sqlite3.Connection,
    run_id: str,
    created_at: str,
    tables: list[str],
    schema_version: int,
    digest: str,
    summary: dict,
    snapshot_json: str,
) -> int:
    """事务内版本：写入一条成功发布，返回 publish_no（= 发布次数，单调递增）。"""
    cursor = conn.execute(
        """INSERT INTO publishes
           (run_id, created_at, tables_json, schema_version, digest, summary_json, snapshot_json)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            run_id,
            created_at,
            json.dumps(tables, ensure_ascii=False),
            int(schema_version),
            digest,
            json.dumps(summary, ensure_ascii=False),
            snapshot_json,
        ),
    )
    return int(cursor.lastrowid)


def _row_to_publish(row: sqlite3.Row, with_snapshot: bool) -> dict:
    out = {
        "publish_no": int(row["publish_no"]),
        "run_id": row["run_id"],
        "created_at": row["created_at"],
        "tables": json.loads(row["tables_json"]),
        "schema_version": int(row["schema_version"]),
        "digest": row["digest"],
        "summary": json.loads(row["summary_json"]),
    }
    if with_snapshot:
        out["snapshot_json"] = row["snapshot_json"]
    return out


def get_publish(conn: sqlite3.Connection, publish_no: int, with_snapshot: bool = False) -> dict | None:
    row = conn.execute(
        "SELECT * FROM publishes WHERE publish_no=?", (int(publish_no),)
    ).fetchone()
    return _row_to_publish(row, with_snapshot) if row else None


def list_publishes(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM publishes ORDER BY publish_no DESC LIMIT ?", (int(limit),)
    ).fetchall()
    return [_row_to_publish(row, False) for row in rows]


def current_publish(conn: sqlite3.Connection) -> dict | None:
    """现网当前版本：meta.production_publish_no 指向的发布。"""
    raw = get_meta(conn, "production_publish_no")
    if raw is None:
        return None
    return get_publish(conn, int(raw))


def previous_publish(conn: sqlite3.Connection, publish_no: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM publishes WHERE publish_no<? ORDER BY publish_no DESC LIMIT 1",
        (int(publish_no),),
    ).fetchone()
    return _row_to_publish(row, False) if row else None


def publish_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM publishes").fetchone()
    return int(row["n"])


def delete_all_publishes(conn: sqlite3.Connection) -> dict:
    publishes = conn.execute("DELETE FROM publishes").rowcount
    attempts = conn.execute("DELETE FROM publish_attempts").rowcount
    conn.execute("DELETE FROM meta WHERE key='production_publish_no'")
    conn.commit()
    return {"publishes": int(publishes), "attempts": int(attempts)}


def record_attempt(
    conn: sqlite3.Connection,
    tables: list[str],
    schema_version: int,
    ok: bool,
    detail: dict,
) -> None:
    """发布尝试审计日志（独立小事务，不进原子切换的临界区）。"""
    conn.execute(
        """INSERT INTO publish_attempts (created_at, tables_json, schema_version, ok, detail_json)
           VALUES (?, ?, ?, ?, ?)""",
        (
            now_iso(),
            json.dumps(tables, ensure_ascii=False),
            int(schema_version),
            1 if ok else 0,
            json.dumps(detail, ensure_ascii=False),
        ),
    )
    conn.commit()


def list_attempts(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM publish_attempts ORDER BY id DESC LIMIT ?", (int(limit),)
    ).fetchall()
    return [
        {
            "id": int(row["id"]),
            "created_at": row["created_at"],
            "tables": json.loads(row["tables_json"]),
            "schema_version": int(row["schema_version"]),
            "ok": bool(row["ok"]),
            "detail": json.loads(row["detail_json"]),
        }
        for row in rows
    ]
