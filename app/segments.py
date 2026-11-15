"""模块 7 · 分段包补传：按来源核对连续段号，孤立段暂存隔离，补齐缺口后级联合并。

口径（对外承诺）：
- 段号按**来源**各自从 1 连续递增；只有「下一期望段」能确认入库，前后段统一确认。
- 坏事件 → **整段停住**：段内任一事件非法，全段不入库，段记为 failed（带行号原因），
  期望段号不动；修复后按同段号重传即可。
- 跳号到达 → **孤立段暂存隔离**（quarantined）：事件只落 segments 表、不进 events 归档，
  因此不参与合并、**不推进水位线**；重启后仍在隔离区，不会抢先出现在现网。
- 补齐缺口 → **级联合并**：确认当前段后，把隔离区里已连续的段按段号依次确认，
  归档与段状态在同一事务里提交。
- 同段重试：内容与已确认段一致 → 幂等 no-op（不新增事件、不推进发布次数）；
  内容不一致 → 409 冲突（已确认的段不可改写）；隔离段未确认前允许同号替换内容。
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from . import archive, settings, store


class SegmentError(ValueError):
    """可预期的段操作错误（路由转成 400）。"""


class SegmentConflict(SegmentError):
    """段号已被确认且内容不一致（路由转成 409）。"""


class SegmentInvalid(SegmentError):
    """段内含坏事件，整段停住（路由转成 422；failed 记录已落库）。"""


def segment_source(source: str, segment_no: int) -> str:
    """段事件的归档 source：排在基础文件（orders.jsonl）之后、按段号字典序有序。"""
    return f"{source}.jsonl#seg{segment_no:03d}"


def content_digest(raw_events: list[dict]) -> str:
    """段内容摘要：规范化序列化后的 SHA-256，用于同段重试的幂等判定。"""
    payload = json.dumps(raw_events, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_jsonl_text(conn, text: str, source: str, segment_no: int) -> list[dict]:
    """把 textarea 里的 JSONL 文本解析成事件对象列表；语法错误按「坏事件整段停住」处理。"""
    label = segment_source(source, segment_no)
    raw_events: list[dict] = []
    for line_no, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            error = f"{label} 第 {line_no} 行：不是合法 JSON（{exc.msg}）"
            _mark_failed(conn, source, segment_no, text, 0, error)
            raise SegmentInvalid(error) from exc
        raw_events.append(obj)
    return raw_events


def _mark_failed(conn, source: str, segment_no: int, payload, event_count: int, error: str) -> None:
    """坏事件整段停住：只记 failed 状态，不归档任何事件（独立提交，期望段号不动）。"""
    digest = hashlib.sha256(
        payload.encode("utf-8") if isinstance(payload, str)
        else json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    store.upsert_segment_tx(
        conn, source, segment_no, store.SEGMENT_FAILED, digest,
        event_count, error,
        payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False),
    )
    conn.commit()


def _validate_events(conn, source: str, segment_no: int, raw_events: list[dict]) -> list[archive.Event]:
    """逐条校验段内事件；任一非法 → 记 failed 并抛 SegmentInvalid（整段停住）。"""
    label = segment_source(source, segment_no)
    events: list[archive.Event] = []
    for line_no, obj in enumerate(raw_events, start=1):
        try:
            events.append(archive.parse_event(obj, source, line_no, label))
        except archive.EventError as exc:
            _mark_failed(conn, source, segment_no, raw_events, len(raw_events), str(exc))
            raise SegmentInvalid(str(exc)) from exc
    return events


def gaps_for(conn, source: str) -> list[int]:
    """缺口段号：在 (已确认前缀, 已知最大段号] 内、既未确认也未在隔离/失败名单里的段号。"""
    upto = store.committed_upto(conn, source)
    known = {
        seg["segment_no"]
        for seg in store.list_segments(conn, source)
        if seg["status"] in (store.SEGMENT_QUARANTINED, store.SEGMENT_FAILED)
    }
    if not known:
        return []
    return [no for no in range(upto + 1, max(known) + 1) if no not in known]


def pending_by_source(conn, sources: list[str]) -> dict[str, dict]:
    """发布校验用：各来源仍未统一确认的段（缺口 / 隔离 / 失败）。"""
    out: dict[str, dict] = {}
    for source in sources:
        gaps = gaps_for(conn, source)
        quarantined = [s["segment_no"] for s in store.list_segments(conn, source, store.SEGMENT_QUARANTINED)]
        failed = [s["segment_no"] for s in store.list_segments(conn, source, store.SEGMENT_FAILED)]
        if gaps or quarantined or failed:
            out[source] = {"gaps": gaps, "quarantined": quarantined, "failed": failed}
    return out


def ingest(conn, source: str, segment_no: int, raw_events: list[dict]) -> dict:
    """补传一个段：核对连续段号 → 隔离或确认（含级联），返回看板需要的全部状态。"""
    if source not in settings.TABLE_NAMES:
        raise SegmentError(f"未知的来源：{source}；可选 {', '.join(settings.TABLE_NAMES)}")
    if isinstance(segment_no, bool) or not isinstance(segment_no, int) or segment_no < 1:
        raise SegmentError(f"段号必须是 >=1 的整数，收到 {segment_no!r}")
    if not raw_events:
        raise SegmentError("段内至少要有 1 条事件")

    digest = content_digest(raw_events)

    # 1) 同段重试：已确认且内容一致 → 幂等 no-op（不新增事件、不推进发布次数）
    existing = store.get_segment(conn, source, segment_no)
    if existing and existing["status"] == store.SEGMENT_COMMITTED:
        if existing["digest"] == digest:
            return _result(conn, source, segment_no, "duplicate", digest, len(raw_events),
                           message=f"seg{segment_no} 与已确认内容一致：本次为重试，"
                                   "不新增事件、不推进任何计数")
        raise SegmentConflict(
            f"{source} seg{segment_no} 已确认入库，本次内容摘要不一致"
            f"（已确认 {existing['digest'][:12]}… ≠ 本次 {digest[:12]}…）："
            "已确认的段不可改写，请核对上游后按新段号补传"
        )

    # 2) 坏事件 → 整段停住（failed 记录落库，期望段号不动）
    events = _validate_events(conn, source, segment_no, raw_events)

    upto = store.committed_upto(conn, source)
    next_expected = upto + 1

    # 3) 跳号到达 → 孤立段暂存隔离：事件只落 segments 表，不进归档、不推进水位线
    if segment_no > next_expected:
        replaced = existing is not None and existing["status"] == store.SEGMENT_QUARANTINED
        store.upsert_segment_tx(
            conn, source, segment_no, store.SEGMENT_QUARANTINED, digest,
            len(events), None,
            json.dumps(raw_events, ensure_ascii=False),
        )
        conn.commit()
        gaps = gaps_for(conn, source)
        note = "已替换同号隔离段内容；" if replaced else ""
        return _result(conn, source, segment_no, "quarantined", digest, len(events),
                       message=f"seg{segment_no} 越过缺口段 {gaps[0] if gaps else next_expected} 到达："
                               f"{note}已暂存隔离（{len(events)} 条事件未归档，不参与合并与水位线），"
                               f"待缺口 {gaps} 补齐后按序合并")

    # 4) 正是下一期望段 → 确认本段，并级联合并隔离区里已连续的段（同一事务）
    #    每项 = (段号, 事件, 内容摘要)；级联段沿用其隔离时登记的摘要（内容未变）
    to_commit: list[tuple[int, list[archive.Event], str]] = [(segment_no, events, digest)]
    cascaded_from_quarantine: list[int] = []
    follow = segment_no
    while True:
        nxt = store.get_segment(conn, source, follow + 1)
        if nxt is None or nxt["status"] != store.SEGMENT_QUARANTINED:
            break
        follow += 1
        stored_raw = json.loads(nxt["payload_json"])
        to_commit.append((follow, _validate_events(conn, source, follow, stored_raw), nxt["digest"]))
        cascaded_from_quarantine.append(follow)

    try:
        archived = 0
        for no, seg_events, seg_digest in to_commit:
            stats = store.archive_events_tx(conn, seg_events)  # (source,line_no) 幂等，双保险
            archived += stats["inserted"]
            store.upsert_segment_tx(
                conn, source, no, store.SEGMENT_COMMITTED, seg_digest,
                len(seg_events), None, "[]",
                committed_at=store.now_iso(),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    committed_nos = [no for no, _, _ in to_commit]
    if cascaded_from_quarantine:
        message = (f"seg{segment_no} 已确认入库（{len(events)} 条事件），"
                   f"缺口补齐，隔离段 {cascaded_from_quarantine} 已按序级联合并；"
                   f"共归档 {archived} 条事件")
    else:
        message = f"seg{segment_no} 已确认入库（{len(events)} 条事件），下一期望段 {follow + 1}"
    result = _result(conn, source, segment_no, "committed", digest, len(events), message=message)
    result["committed"] = committed_nos
    result["cascaded"] = cascaded_from_quarantine
    result["archived"] = archived
    return result


def _result(conn, source: str, segment_no: int, status: str, digest: str,
            event_count: int, message: str) -> dict:
    return {
        "status": status,
        "source": source,
        "segment_no": segment_no,
        "event_count": event_count,
        "digest": digest,
        "next_expected": store.committed_upto(conn, source) + 1,
        "gaps": gaps_for(conn, source),
        "quarantined": [s["segment_no"] for s in store.list_segments(conn, source, store.SEGMENT_QUARANTINED)],
        "failed": [s["segment_no"] for s in store.list_segments(conn, source, store.SEGMENT_FAILED)],
        "message": message,
    }


def board(conn) -> dict:
    """段状态看板：按来源列出已确认前缀、下一期望段、缺口、隔离段、失败段与归档计数。"""
    sources: list[dict] = []
    totals = {"committed": 0, "quarantined": 0, "failed": 0, "gaps": 0}
    for item in settings.TABLES:
        source = item["name"]
        upto = store.committed_upto(conn, source)
        quarantined = store.list_segments(conn, source, store.SEGMENT_QUARANTINED)
        failed = store.list_segments(conn, source, store.SEGMENT_FAILED)
        gaps = gaps_for(conn, source)
        totals["committed"] += upto
        totals["quarantined"] += len(quarantined)
        totals["failed"] += len(failed)
        totals["gaps"] += len(gaps)
        sources.append({
            "source": source,
            "label": item["label"],
            "committed_upto": upto,
            "next_expected": upto + 1,
            "committed_segments": list(range(1, upto + 1)),
            "gaps": gaps,
            "quarantined": [
                {
                    "segment_no": seg["segment_no"],
                    "event_count": seg["event_count"],
                    "digest": seg["digest"][:12],
                    "created_at": seg["created_at"],
                }
                for seg in quarantined
            ],
            "failed": [
                {
                    "segment_no": seg["segment_no"],
                    "event_count": seg["event_count"],
                    "error": seg["error"],
                    "created_at": seg["created_at"],
                }
                for seg in failed
            ],
            "archived_events": store.count_table_events(conn, source),
            "segment_events": store.count_segment_events(conn, source),
        })
    return {"sources": sources, "totals": totals}


def reset(conn) -> dict:
    """演示重置：清空段状态与段归档事件、清空发布记录与现网指针（基础 seed 与历史运行保留）。"""
    deleted_events = store.delete_segment_events(conn)
    deleted_segments = store.delete_all_segments(conn)
    deleted = store.delete_all_publishes(conn)
    return {
        "deleted_events": deleted_events,
        "deleted_segments": deleted_segments,
        "deleted_publishes": deleted["publishes"],
        "deleted_attempts": deleted["attempts"],
        "total_events": store.total_events(conn),
    }


def sample_events(source: str, segment_no: int, bad: bool = False,
                  segments_dir: Path | None = None) -> list[dict]:
    """读取样例段内容（解析成事件对象列表）；测试与 /api/segments/samples 共用。"""
    segments_dir = segments_dir or settings.SEGMENTS_DIR
    suffix = ".bad" if bad else ""
    path = segments_dir / f"{source}.seg{segment_no}{suffix}.jsonl"
    if not path.exists():
        raise SegmentError(f"缺少样例段文件：{path}")
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def sample_payloads(segments_dir: Path | None = None) -> dict[str, dict[str, str]]:
    """页面快捷按钮用：{来源: {段号或 '4bad': JSONL 原文}}。"""
    segments_dir = segments_dir or settings.SEGMENTS_DIR
    out: dict[str, dict[str, str]] = {}
    for item in settings.TABLES:
        source = item["name"]
        out[source] = {}
        for no in (1, 2, 3, 4):
            path = segments_dir / f"{source}.seg{no}.jsonl"
            if path.exists():
                out[source][str(no)] = path.read_text(encoding="utf-8")
        bad = segments_dir / f"{source}.seg4.bad.jsonl"
        if bad.exists():
            out[source]["4bad"] = bad.read_text(encoding="utf-8")
    return out
