"""FastAPI 路由（薄）：页面渲染 + JSON 端点，业务全部在领域模块里。

页面：/ 首页 · /workbench 工作台 · /result/{run_id} 结果 · /review 复核 · /segments 补传与发布
接口：POST /api/merge · GET /api/runs · GET /api/runs/{id} · GET /api/runs/{id}/snapshot
      POST /api/segments · GET /api/segments · POST /api/segments/reset · GET /api/segments/samples
      POST /api/publish · GET /api/publishes · GET /api/publishes/{no}/export
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from . import archive, publish, runner, schema_gate, segments, settings, snapshot, store
from .archive import EventError

BASE_DIR = settings.BASE_DIR


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = store.connect()
    store.init_db(conn)
    if store.total_events(conn) == 0:
        store.seed_sample(conn)
    conn.close()
    yield


app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "app" / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


class MergeRequest(BaseModel):
    tables: list[str] = Field(default_factory=list, description="要合并的表名")
    schema_version: int = Field(2, ge=1, le=2, description="要评估的目标 schema 版本")


class SegmentUpload(BaseModel):
    source: str = Field(description="来源（表 名）")
    segment_no: int = Field(ge=1, description="段号（按来源从 1 连续递增）")
    events: list[dict] | None = Field(default=None, description="段内事件（JSON 对象列表）")
    events_jsonl: str | None = Field(default=None, description="段内事件（JSONL 文本，二选一）")


class PublishRequest(BaseModel):
    tables: list[str] = Field(default_factory=list, description="要校验并切换的表名")
    schema_version: int = Field(1, ge=1, le=2, description="目标 schema 版本")


def render(request: Request, template: str, **ctx) -> HTMLResponse:
    context = {
        "app_name": settings.APP_NAME,
        "app_short": settings.APP_SHORT,
        "tables_meta": settings.TABLES,
        "kind_labels": settings.KIND_LABELS,
        "verdict_labels": settings.VERDICT_LABELS,
        "change_labels": settings.CHANGE_LABELS,
        "op_labels": settings.OP_LABELS,
        "nav": ctx.pop("nav", ""),
    }
    context.update(ctx)
    return templates.TemplateResponse(request, template, context)


def with_conn():
    conn = store.connect()
    store.init_db(conn)
    return conn


def back_to_segments(notice: str = "", error: str = "") -> RedirectResponse:
    """表单提交后 303 回补传页，带一行提示（看板与发布状态由页面重新渲染）。"""
    query = urlencode({k: v for k, v in (("notice", notice), ("error", error)) if v})
    return RedirectResponse(f"/segments{'?' + query if query else ''}", status_code=303)


# --- 页面 ------------------------------------------------------------------


@app.get("/health", include_in_schema=False)
def health():
    conn = with_conn()
    try:
        current = store.current_publish(conn)
        return {
            "status": "ok",
            "app": settings.APP_SHORT,
            "events": store.total_events(conn),
            "runs": store.run_count(conn),
            "seed_version": store.get_meta(conn, "seed_version"),
            "quarantined_segments": len(store.list_segments(conn, status=store.SEGMENT_QUARANTINED)),
            "failed_segments": len(store.list_segments(conn, status=store.SEGMENT_FAILED)),
            "publish_no": current["publish_no"] if current else 0,
        }
    finally:
        conn.close()


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    conn = with_conn()
    try:
        overview = store.event_overview(conn)
        meta = {
            "seed_version": store.get_meta(conn, "seed_version"),
            "seeded_at": store.get_meta(conn, "seeded_at"),
            "total_events": store.total_events(conn),
            "runs": store.run_count(conn),
        }
        recent = store.list_runs(conn, limit=3)
    finally:
        conn.close()
    stats = {
        "total_events": meta["total_events"],
        "table_count": len(settings.TABLES),
        "runs": meta["runs"],
        "last_run_id": recent[0]["run_id"] if recent else None,
        "last_run_at": recent[0]["created_at"] if recent else None,
        "last_conflicts": recent[0]["summary"]["conflicts"] if recent else 0,
    }
    return render(
        request,
        "index.html",
        page="index",
        nav="index",
        overview=overview,
        meta=meta,
        recent=recent,
        stats=stats,
        embedded=settings.EMBEDDED,
        steps=settings.DEMO_STEPS,
        schema_v1=schema_gate.load_schema(settings.SCHEMAS_DIR / "v1.json"),
        schema_v2=schema_gate.load_schema(settings.SCHEMAS_DIR / "v2.json"),
        changes=schema_gate.structural_changes(
            schema_gate.load_schema(settings.SCHEMAS_DIR / "v1.json"),
            schema_gate.load_schema(settings.SCHEMAS_DIR / "v2.json"),
            settings.TABLE_NAMES,
        ),
    )


@app.get("/workbench", response_class=HTMLResponse)
def workbench(request: Request):
    conn = with_conn()
    try:
        overview = store.event_overview(conn)
        recent = store.list_runs(conn, limit=5)
    finally:
        conn.close()
    return render(
        request,
        "workbench.html",
        page="workbench",
        nav="workbench",
        overview=overview,
        recent=recent,
        schema_v1=schema_gate.load_schema(settings.SCHEMAS_DIR / "v1.json"),
        schema_v2=schema_gate.load_schema(settings.SCHEMAS_DIR / "v2.json"),
    )


@app.get("/result/{run_id}", response_class=HTMLResponse)
def result_page(request: Request, run_id: str):
    conn = with_conn()
    try:
        run = store.get_run(conn, run_id)
    finally:
        conn.close()
    if run is None:
        return render(
            request,
            "not_found.html",
            page="not_found",
            nav="",
            run_id=run_id,
            error=f"没有找到运行 {run_id}，可能已被清理；请到工作台重新执行一次合并。",
        )
    snap = snapshot.build_snapshot(run)
    preview = snapshot.preview(snap)
    result = run["result"]
    ignored = {
        table: set(payload.get("ignored_line_nos", []))
        for table, payload in result["tables"].items()
    }
    change_by_line = {(entry["table"], entry["line_no"]): entry for entry in result["changelog"]}
    late_rows = []
    for item in result["late_events"]:
        row = dict(item)
        entry = change_by_line.get((item["table"], item["line_no"]), {})
        row["after_tombstone"] = item["line_no"] in ignored.get(item["table"], set())
        row["kind"] = entry.get("kind", "")
        row["kind_label"] = entry.get("kind_label", "已处理")
        row["reason"] = entry.get("reason", "")
        late_rows.append(row)
    return render(
        request,
        "result.html",
        page="result",
        nav="",
        run=run,
        summary=run["summary"],
        result=result,
        snapshot=snap,
        preview=preview,
        late_rows=late_rows,
        snapshot_bytes=snapshot.snapshot_size(snap),
        base_schema=schema_gate.load_schema(settings.SCHEMAS_DIR / "v1.json"),
        target_schema=schema_gate.load_schema(
            settings.SCHEMAS_DIR / f"v{run['schema_version']}.json"
        ),
    )


@app.get("/review", response_class=HTMLResponse)
def review(request: Request, run_id: str | None = None, kind: str = Query(default="")):
    conn = with_conn()
    try:
        runs = store.list_runs(conn, limit=50)
        selected = store.get_run(conn, run_id) if run_id else (
            store.get_run(conn, runs[0]["run_id"]) if runs else None
        )
    finally:
        conn.close()
    entries: list[dict] = []
    kinds: list[str] = []
    if selected is not None:
        entries = selected["result"]["changelog"]
        kinds = sorted({entry["kind"] for entry in entries})
        if kind:
            entries = [entry for entry in entries if entry["kind"] == kind]
        entries = entries[:200]
    return render(
        request,
        "review.html",
        page="review",
        nav="review",
        runs=runs,
        selected=selected,
        entries=entries,
        kinds=kinds,
        active_kind=kind,
        run_count=len(runs),
    )


@app.get("/segments", response_class=HTMLResponse)
def segments_page(request: Request, notice: str = "", error: str = ""):
    conn = with_conn()
    try:
        board = segments.board(conn)
        pub = publish.status(conn)
        events_by_table = {table: store.fetch_events(conn, table) for table in settings.TABLE_NAMES}
    finally:
        conn.close()
    # 实时演算当前归档水位线（只读、不落库）：隔离段未归档，因此推不动这条线
    base_doc, _ = runner.load_schemas(1)
    live = runner.analyze(settings.TABLE_NAMES, events_by_table, base_doc, base_doc)
    live_wm = {
        "watermark": live["watermark"]["watermark"],
        "open_keys": live["watermark"]["open_keys"],
        "events": live["counters"]["events"],
    }
    payloads = segments.sample_payloads()
    samples_sources = []
    for item in settings.TABLES:
        source = item["name"]
        buttons = []
        for key, label in (("1", "seg1"), ("2", "seg2"), ("3", "seg3"),
                           ("4", "seg4"), ("4bad", "seg4（坏）")):
            if key in payloads.get(source, {}):
                buttons.append((key, 4 if key == "4bad" else int(key), label, "secondary"))
        samples_sources.append((source, buttons))
    return render(
        request,
        "segments.html",
        page="segments",
        nav="segments",
        notice=notice,
        error=error,
        board=board,
        pub=pub,
        live_wm=live_wm,
        samples_json=json.dumps(payloads, ensure_ascii=False),
        samples_sources=samples_sources,
        segment_status_labels=settings.SEGMENT_STATUS_LABELS,
        demo_hints=settings.SEGMENT_DEMO_HINTS,
    )


# --- 补传与发布：表单端点（303 回页面） ---------------------------------------


@app.post("/segments/upload", include_in_schema=False)
async def segment_upload_form(request: Request):
    form = await request.form()
    source = str(form.get("source", "")).strip()
    try:
        segment_no = int(str(form.get("segment_no", "")).strip())
    except ValueError:
        return back_to_segments(error="段号必须是整数")
    if source not in settings.TABLE_NAMES:
        return back_to_segments(error=f"未知的来源：{source}；可选 {', '.join(settings.TABLE_NAMES)}")
    if segment_no < 1:
        return back_to_segments(error="段号必须是 >=1 的整数")
    text = str(form.get("events_jsonl", ""))
    conn = with_conn()
    try:
        raw_events = segments.parse_jsonl_text(conn, text, source, segment_no)
        result = segments.ingest(conn, source, segment_no, raw_events)
        return back_to_segments(notice=result["message"])
    except segments.SegmentInvalid as exc:
        return back_to_segments(error=f"{source} seg{segment_no} 整段停住：{exc}")
    except segments.SegmentConflict as exc:
        return back_to_segments(error=str(exc))
    except segments.SegmentError as exc:
        return back_to_segments(error=str(exc))
    finally:
        conn.close()


@app.post("/segments/publish", include_in_schema=False)
async def segment_publish_form(request: Request):
    form = await request.form()
    tables = [str(t) for t in form.getlist("tables")]
    try:
        schema_version = int(str(form.get("schema_version", "1")).strip())
    except ValueError:
        return back_to_segments(error="schema 版本非法")
    conn = with_conn()
    try:
        result = publish.publish(conn, tables, schema_version)
    except publish.PublishError as exc:
        current = store.current_publish(conn)
        kept = f"；现网保留发布 #{current['publish_no']}" if current else "；现网仍未发布"
        return back_to_segments(error="校验未通过：" + "；".join(exc.reasons) + kept)
    except runner.RunError as exc:
        return back_to_segments(error=str(exc))
    finally:
        conn.close()
    return back_to_segments(notice=result["message"])


@app.post("/segments/reset", include_in_schema=False)
def segment_reset_form():
    conn = with_conn()
    try:
        stats = segments.reset(conn)
    finally:
        conn.close()
    return back_to_segments(
        notice=f"已重置补传与发布：清理段事件 {stats['deleted_events']} 条、"
               f"段记录 {stats['deleted_segments']} 个、发布 {stats['deleted_publishes']} 次"
               "（基础样例与历史运行保留）"
    )


# --- API -------------------------------------------------------------------


@app.post("/api/merge")
def api_merge(payload: MergeRequest):
    conn = with_conn()
    try:
        run = runner.execute(conn, payload.tables, payload.schema_version)
    except runner.RunError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except schema_gate.SchemaError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except EventError as exc:
        raise HTTPException(status_code=422, detail=f"样例事件解析失败：{exc}") from exc
    finally:
        conn.close()
    return JSONResponse(
        {
            "run_id": run["run_id"],
            "summary": run["summary"],
            "tables": run["tables"],
            "schema_version": run["schema_version"],
            "result_url": f"/result/{run['run_id']}",
            "snapshot_url": f"/api/runs/{run['run_id']}/snapshot",
        }
    )


@app.get("/api/runs")
def api_runs(limit: int = Query(default=20, ge=1, le=100)):
    conn = with_conn()
    try:
        runs = store.list_runs(conn, limit=limit)
    finally:
        conn.close()
    return {"runs": runs, "count": len(runs)}


@app.get("/api/runs/{run_id}")
def api_run(run_id: str):
    conn = with_conn()
    try:
        run = store.get_run(conn, run_id)
    finally:
        conn.close()
    if run is None:
        raise HTTPException(status_code=404, detail=f"运行不存在：{run_id}")
    result = run["result"]
    return {
        "run_id": run["run_id"],
        "created_at": run["created_at"],
        "tables": run["tables"],
        "schema_version": run["schema_version"],
        "summary": run["summary"],
        "watermark": {
            "watermark": result["watermark"]["watermark"],
            "open_keys": result["watermark"]["open_keys"],
            "closed_keys": result["watermark"]["closed_keys"],
            "per_table": {
                table: {
                    "watermark": payload["watermark"]["watermark"],
                    "open_keys": payload["watermark"]["open_keys"],
                    "closed_keys": payload["watermark"]["closed_keys"],
                    "laggard_keys": payload["watermark"]["laggard_keys"],
                }
                for table, payload in result["tables"].items()
            },
        },
        "tables": {
            table: {
                "row_count": payload["row_count"],
                "tombstone_count": payload["tombstone_count"],
                "key_count": payload["key_count"],
                "counters": payload["counters"],
            }
            for table, payload in result["tables"].items()
        },
        "gate": result["gate"],
        "conflicts": [
            {
                "table": item["table"],
                "pk": item["pk"],
                "version": item["version"],
                "diff": item["diff"],
                "first_line": item["first"]["line_no"],
                "second_line": item["second"]["line_no"],
            }
            for item in result["conflicts"]
        ],
        "late_events": result["late_events"],
        "snapshot_url": f"/api/runs/{run['run_id']}/snapshot",
    }


@app.get("/api/runs/{run_id}/snapshot")
def api_snapshot(run_id: str):
    conn = with_conn()
    try:
        run = store.get_run(conn, run_id)
    finally:
        conn.close()
    if run is None:
        raise HTTPException(status_code=404, detail=f"运行不存在：{run_id}")
    payload = snapshot.dumps(snapshot.build_snapshot(run))
    filename = f"cdcwater-snapshot-{run_id}.json"
    return Response(
        content=payload,
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/seed")
def api_seed():
    """幂等重放样例：已归档的事件不会重复插入。"""
    conn = with_conn()
    try:
        stats = store.seed_sample(conn)
        stats["total_events"] = store.total_events(conn)
        stats["counts"] = store.event_counts(conn)
    except EventError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    finally:
        conn.close()
    return stats


@app.get("/api/samples")
def api_samples():
    conn = with_conn()
    try:
        return {"overview": store.event_overview(conn), "total": store.total_events(conn)}
    finally:
        conn.close()


@app.get("/api/schemas/{version}")
def api_schema(version: int):
    if version not in (1, 2):
        raise HTTPException(status_code=400, detail="schema 版本只支持 1 或 2")
    return schema_gate.load_schema(settings.SCHEMAS_DIR / f"v{version}.json")


# --- 分段补传（模块 7） -------------------------------------------------------


@app.post("/api/segments")
def api_segment_ingest(payload: SegmentUpload):
    """补传一个段：连续段号核对 → 确认（含级联）/ 隔离 / 整段停住 / 幂等重试。"""
    if payload.source not in settings.TABLE_NAMES:
        raise HTTPException(
            status_code=400,
            detail=f"未知的来源：{payload.source}；可选 {', '.join(settings.TABLE_NAMES)}",
        )
    conn = with_conn()
    try:
        raw_events = payload.events
        if raw_events is None:
            if not payload.events_jsonl:
                raise HTTPException(status_code=400, detail="events 与 events_jsonl 至少给一个")
            raw_events = segments.parse_jsonl_text(
                conn, payload.events_jsonl, payload.source, payload.segment_no
            )
        result = segments.ingest(conn, payload.source, payload.segment_no, raw_events)
    except segments.SegmentInvalid as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except segments.SegmentConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except segments.SegmentError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        conn.close()
    return result


@app.get("/api/segments")
def api_segments_board():
    """段状态看板：各来源已确认前缀 / 下一期望段 / 缺口 / 隔离段 / 失败段。"""
    conn = with_conn()
    try:
        return segments.board(conn)
    finally:
        conn.close()


@app.post("/api/segments/reset")
def api_segments_reset():
    """演示重置：清空段与发布状态（基础样例与历史运行保留）。"""
    conn = with_conn()
    try:
        return segments.reset(conn)
    finally:
        conn.close()


@app.get("/api/segments/samples")
def api_segment_samples():
    """样例段内容（与页面快捷按钮同源），便于 curl 演示。"""
    out: dict[str, dict[str, list]] = {}
    for source, segs in segments.sample_payloads().items():
        out[source] = {
            key: [json.loads(line) for line in text.splitlines() if line.strip()]
            for key, text in segs.items()
        }
    return out


# --- 原子发布（模块 8） -------------------------------------------------------


@app.post("/api/publish")
def api_publish(payload: PublishRequest):
    """校验全部选中表 → 通过则一次切换现网并导出；失败保留旧版（409）。"""
    conn = with_conn()
    try:
        result = publish.publish(conn, payload.tables, payload.schema_version)
    except publish.PublishError as exc:
        current = store.current_publish(conn)
        raise HTTPException(
            status_code=409,
            detail={
                "reasons": exc.reasons,
                "kept_publish_no": current["publish_no"] if current else None,
            },
        ) from exc
    except runner.RunError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        conn.close()
    return result


@app.get("/api/publishes")
def api_publishes():
    """发布状态：现网当前版 / 上一版 / 切换前后对照 / 历史与尝试日志。"""
    conn = with_conn()
    try:
        return publish.status(conn)
    finally:
        conn.close()


@app.get("/api/publishes/{publish_no}/export")
def api_publish_export(publish_no: int):
    """下载某次发布随切换一并导出的快照 JSON。"""
    conn = with_conn()
    try:
        found = publish.get_export(conn, publish_no)
    finally:
        conn.close()
    if found is None:
        raise HTTPException(status_code=404, detail=f"发布不存在：#{publish_no}")
    pub, snapshot_json = found
    return Response(
        content=snapshot_json,
        media_type="application/json; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="cdcwater-publish-{publish_no}.json"'
        },
    )
