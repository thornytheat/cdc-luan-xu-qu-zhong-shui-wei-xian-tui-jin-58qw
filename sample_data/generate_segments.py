#!/usr/bin/env python3
"""确定性生成分段包补传样例（模块 7/8 的演示素材）。

运行：python3 sample_data/generate_segments.py

产物（固定，不进随机数）：
- sample_data/segments/{orders,customers,inventory}.seg{1,2,3,4}.jsonl   每表 6/6/6/5 条
- sample_data/segments/{orders,customers,inventory}.seg4.bad.jsonl       seg4 的坏事件变体（第 3 行 op 非法）

设计口径（与 app/segments.py 的演示脚本一致）：
- 段事件全部落在 2026-11-11（基础样例之后一天），段内按 ts 升序，不引入新的迟到。
- seg1 只更新部分「拖水位」键：确认后水位线仍停在 2026-11-10T00:14:00Z（P1009 未动）。
- seg3 才更新 P1009 等最后一批旧 ts 键：把 seg3 隔离时水位线被挡住；
  补齐 seg2 触发级联合并后，水位线才一次性推进到 2026-11-11T09:01:00Z。
- orders / inventory 的 seg2 含墓碑键 P9002 的 v4 复活事件（c）；customers 的 C9002 无墓碑，为 v3 更新。
- seg4.bad 与 seg4 逐行一致，仅第 3 行 op 写成 "x"：演示「坏事件整段停住 → 修复重传」。
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from generate_samples import (  # noqa: E402
    TABLES as BASE_TABLES,
    customer_row,
    inventory_row,
    order_row,
)

SEGMENTS_DIR = ROOT / "segments"

BASE_TS = datetime(2026, 11, 11, 0, 0, tzinfo=timezone.utc)
# 各段的起始分钟：seg1 09:00 / seg2 10:00 / seg3 11:00 / seg4 12:00
SEG_MINUTE = {1: 9 * 60, 2: 10 * 60, 3: 11 * 60, 4: 12 * 60}
SEG_COUNTS = {1: 6, 2: 6, 3: 6, 4: 5}

BUILDERS = {"orders": order_row, "customers": customer_row, "inventory": inventory_row}
PREFIX = {"orders": "O", "customers": "C", "inventory": "I"}

# 基础样例里特殊键的 ident（必须保持一致，身份字段才不会漂移）
IDENT = {"1999": 40, "9001": 60, "9002": 61, "9003": 62, "9004": 63}
NEW_IDENT = {"2001": 70, "2002": 71, "2003": 72, "2004": 73, "2005": 74}


def ts(minute: int) -> str:
    return (BASE_TS + timedelta(minutes=minute)).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_event(table: str, pk: str, version: int, minute: int, op: str, ident: int, step: int) -> dict:
    row = BUILDERS[table](pk, ident, step, minute, 2)
    row["updated_at"] = ts(minute)  # generate_samples 的 ts 基准是 11-10，这里覆盖成段内时间
    return {"pk": pk, "version": version, "ts": ts(minute), "op": op, "after": row, "schema_version": 2}


def build_segments(table: str) -> dict[int, list[dict]]:
    p = PREFIX[table]

    def ev(suffix: str, version: int, seg: int, offset: int, op: str, ident: int, step: int) -> dict:
        return make_event(table, f"{p}{suffix}", version, SEG_MINUTE[seg] + offset, op, ident, step)

    # seg1：只清 00:14 的 1001/1005 与 00:19 的 1002/1006；1009 不动 → 水位线不前进
    seg1 = [
        ev("2001", 1, 1, 0, "c", NEW_IDENT["2001"], 1),
        ev("2002", 1, 1, 1, "c", NEW_IDENT["2002"], 1),
        ev("1001", 6, 1, 2, "u", 0, 6),
        ev("1002", 6, 1, 3, "u", 1, 6),
        ev("1005", 6, 1, 4, "u", 4, 6),
        ev("1006", 6, 1, 5, "u", 5, 6),
    ]
    # seg2：清 00:24 / 00:29 的普通键；orders/inventory 复活墓碑键 9002（v4 c），customers 为 v3 更新
    has_tombstone = table != "customers"
    seg2 = [
        ev("2001", 2, 2, 0, "u", NEW_IDENT["2001"], 2),
        ev("1003", 6, 2, 1, "u", 2, 6),
        ev("1004", 6, 2, 2, "u", 3, 6),
        ev("1007", 6, 2, 3, "u", 6, 6),
        ev("1008", 6, 2, 4, "u", 7, 6),
        ev("9002", 4 if has_tombstone else 3, 2, 5,
           "c" if has_tombstone else "u", IDENT["9002"], 4 if has_tombstone else 3),
    ]
    # seg3：清掉最后一批旧 ts 键（1009/1999/9001/9003/9004）→ 确认后水位线跳到 09:01
    seg3 = [
        ev("2003", 1, 3, 0, "c", NEW_IDENT["2003"], 1),
        ev("1009", 6, 3, 1, "u", 8, 6),
        ev("1999", 4, 3, 2, "u", IDENT["1999"], 4),
        ev("9001", 3, 3, 3, "u", IDENT["9001"], 3),
        ev("9003", 4, 3, 4, "u", IDENT["9003"], 4),
        ev("9004", 4, 3, 5, "u", IDENT["9004"], 4),
    ]
    # seg4：补传收尾（P1001 v7、P9004 v5、两个新键）；.bad 变体把第 3 行 op 写成 "x"
    seg4 = [
        ev("2002", 2, 4, 0, "u", NEW_IDENT["2002"], 2),
        ev("2004", 1, 4, 1, "c", NEW_IDENT["2004"], 1),
        ev("1001", 7, 4, 2, "u", 0, 7),
        ev("9004", 5, 4, 3, "u", IDENT["9004"], 5),
        ev("2005", 1, 4, 4, "c", NEW_IDENT["2005"], 1),
    ]
    return {1: seg1, 2: seg2, 3: seg3, 4: seg4}


def dump(events: list[dict], path: Path) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for e in events:
            fh.write(json.dumps(e, ensure_ascii=False) + "\n")


def base_max_versions() -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for table in BASE_TABLES:
        name = table["name"] if isinstance(table, dict) else table
        versions: dict[str, int] = {}
        with (ROOT / "events" / f"{name}.jsonl").open(encoding="utf-8") as fh:
            for line in fh:
                e = json.loads(line)
                versions[e["pk"]] = max(versions.get(e["pk"], 0), e["version"])
        out[name] = versions
    return out


def main() -> int:
    SEGMENTS_DIR.mkdir(parents=True, exist_ok=True)
    base_versions = base_max_versions()
    total = 0
    for table in PREFIX:
        segs = build_segments(table)
        for no, events in segs.items():
            assert len(events) == SEG_COUNTS[no], f"{table} seg{no} 条数 {len(events)} != {SEG_COUNTS[no]}"
            minutes = [json.loads(json.dumps(e))["ts"] for e in events]
            assert minutes == sorted(minutes), f"{table} seg{no} 段内 ts 未升序"
            for e in events:
                assert e["ts"] >= "2026-11-11T09:00:00Z", f"{table} seg{no} 出现 11-10 的 ts"
                base_max = base_versions[table].get(e["pk"], 0)
                if e["pk"][-4:] in NEW_IDENT:
                    assert base_max == 0, f"新键 {e['pk']} 不应出现在基础样例"
                    assert e["version"] in (1, 2)
                else:
                    assert e["version"] > base_max, f"{e['pk']} v{e['version']} 未超过基础最大版本 v{base_max}"
            dump(events, SEGMENTS_DIR / f"{table}.seg{no}.jsonl")
            total += len(events)

        # 坏事件变体：与 seg4 逐行一致，仅第 3 行 op 非法
        bad = [dict(e) for e in segs[4]]
        bad[2]["op"] = "x"
        dump(bad, SEGMENTS_DIR / f"{table}.seg4.bad.jsonl")
        bad_lines = sum(1 for e in bad if e["op"] not in ("c", "u", "d"))
        assert bad_lines == 1, f"{table} seg4.bad 坏行数 {bad_lines} != 1"

        counts = " / ".join(f"seg{no}:{len(evts)}" for no, evts in segs.items())
        print(f"{table:10s} {counts}  （seg4.bad 第 3 行 op='x'）")
    print(f"合计 {total} 条段事件（每表 23 条）+ 每表 1 个坏事件变体")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
