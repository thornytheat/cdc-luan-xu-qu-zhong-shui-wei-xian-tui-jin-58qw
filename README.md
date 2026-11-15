# cdcwater · CDC 乱序去重与水位线推进

面向数据集成组的 CDC 合并工具：把订单 / 客户 / 库存三库的变更事件（乱序、重复、迟到）
合并成可信的最终行，给出可复现的水位线，并在放行 schema 变更前逐条评估影响。
分段包补传按来源核对连续段号，孤立段暂存隔离、补齐缺口后级联合并，
全部选中表校验通过才一次切换现网并导出。

- 形态：单服务 FastAPI + Jinja2 服务端模板 + 原生 JS，SQLite 命名卷持久化。
- 端口：宿主机 `18213`（容器内 8000）。
- 样例：3 张表 × 60 条事件，故意埋 8 条迟到、3 条同版本冲突、2 条墓碑后迟到、1 条同内容重复；
  另有 3 来源 × 4 段补传包（每段 6/6/6/5 条）+ 每来源 1 个坏事件段变体。

## 能力

1. **事件归档**：JSONL 逐行解析、字段校验，事件保留「文件 + 行号」作为所有结论的定位依据；导入幂等（唯一键 source + line_no）。
2. **同键合并**：同 pk 保留最大 `version`；同版本同内容静默去重；同版本不同内容判 `conflict` 且不覆盖先到内容。
3. **墓碑处理**：`op=d` 且版本更高 → 键判删除，墓碑保留；墓碑后低版本 c/u 忽略并计入 `ignored_after_tombstone`；只有严格更高版本才复活。
4. **乱序与水位线**：按到达顺序判定迟到（`ts < 当前已处理最大 ts`），迟到仍按版本合并；`watermark = min(每个键已处理事件的最大 ts)`，输出全局/分表水位线、未闭合键数与推进轨迹。
5. **Schema 门禁**：对比 v1 → v2，逐条给「可放行 / 放行+回填 / 阻断」，附列位置与理由。
6. **快照导出**：每表最终行 + 墓碑 + 冲突明细 + 迟到位置 + 门禁结论 + 全部变更日志，JSON 可下载。
7. **分段包补传**：段号按来源从 1 连续递增，只有「下一期望段」能确认入库；坏事件让整段停住（记 `failed` 带行号）；跳号段进隔离区（`quarantined`，事件不进归档、不推水位线、重启不丢失）；补齐缺口时期望段与隔离区里已连续的段在同一事务里级联确认；同段重试内容一致幂等 no-op，内容不一致判 409。
8. **原子发布**：选中表无缺口/隔离/失败段 + 均有归档事件 + 门禁逐表无阻断，三条全过才一次切换现网并导出（运行、发布记录、现网指针、快照同事务提交）；任一不过保留旧版；内容摘要未变的重复发布不推进发布次数；页面逐表对照切换前后。

## 核心规则口径

| 规则 | 口径 |
|---|---|
| 最大版本 | 同 pk 只保留 `version` 最大的事件；冲突时保留**先到**内容 |
| 同版本 | `after` 按字段排序后逐字比较：一致 → 静默去重（不计冲突）；不一致 → 冲突（列出两条事件与差异字段） |
| 墓碑 | 删除事件写入后不可丢；`version < 墓碑版本` 的事件一律忽略，且不改状态 |
| 迟到 | 文件行序处理，`event.ts < 当前已处理最大 ts`（相等不算迟到），记录表名 + 行号 + 当时最大 ts |
| 水位线 | `min(每个键已处理事件的最大 ts)`；`键最大 ts <= 水位线` 为已闭合键，其余为未闭合键 |
| 门禁 | 新增 nullable → `compatible`；新增带默认值 → `compatible_with_backfill`；删列 / 数值收窄 / 类型族不兼容 / 新增非空无默认 / 可空收紧为非空无默认 → `blocked`；总判定取最严 |
| 连续段号 | 按来源各自核对：`next_expected = 已确认最大段号 + 1`；等于它才确认，大于它进隔离区，小于等于它按内容摘要比对（一致幂等 / 不一致 409） |
| 孤立段 | 只写 segments 表、不进 events 归档 → 不参与合并、不推进水位线；重启后仍在隔离区；未确认前允许同号替换内容 |
| 坏事件 | 段内任一事件非法 → 整段不入库、期望段号不动，段记 `failed`（带行号原因），修复后同号重传 |
| 级联合并 | 确认当前段后，隔离区里已连续的段按段号依次确认，归档与段状态同一事务提交 |
| 原子发布 | 三条校验全过才切换：无缺口/隔离/失败段、表均有事件、门禁逐表无阻断；运行 + 发布 + 现网指针 + 快照导出同事务，失败整体回滚保留旧版 |
| 发布次数 | `publishes` 自增主键，只有成功切换才 +1；内容摘要（表 + schema 版本 + seed + 已确认段摘要链）未变的发布不推进 |

## 启动

```bash
cd batch-20261114b/projects/gb-624
cp .env.example .env            # 可选，默认端口 18213
docker compose up -d --build
docker compose ps               # 等 healthy
curl -s http://127.0.0.1:18213/health
```

打开 <http://127.0.0.1:18213/>。SQLite 落在命名卷 `cdcwater-data:/app/data`；
重建数据：`docker compose down -v` 后重新 `up`（启动时幂等 seed，重复启动不会重复导入事件）。

## 演示步骤

1. 首页看样例概览与埋点说明（8 迟到 / 3 冲突 / 2 墓碑后迟到 / 1 重复）。
2. 工作台勾选 3 张表、目标 schema 选 v2，点「执行合并」，看进度与即时摘要。
3. 结果页：每表合并行数、冲突逐条（两条事件行号 + 差异字段）、迟到位置、水位线、门禁逐条结论、快照预览。
4. 结果页或复核页下载快照 JSON；复核页可按结论筛选 180 条变更日志。
5. 「补传与发布」页演示分段包补传：
   - **乱序**：orders 点 seg1（确认）→ seg3（隔离待补、缺口 [2]、水位线不动）→ seg2（缺口补齐，seg2/seg3 级联确认，水位线跳到 2026-11-11）。
   - **坏事件**：inventory 点 seg4（坏）→ 第 3 行 op 非法、整段停住记失败；再点 seg4（修复版）重传。
   - **幂等**：同段同内容重传 → 不新增事件；再发布 → 内容未变化，发布次数不推进。
   - **原子发布**：三表段补齐后选 v1「校验并发布」→ 一次切换现网并导出；改选 v2 → 门禁阻断、保留旧版；「切换前后对照」逐表比较行数 / 墓碑 / 水位线。

## API / 页面

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查（含事件数、运行数、隔离/失败段数、当前发布号） |
| GET | `/` `/workbench` `/result/{run_id}` `/review` `/segments` | 5 个页面 |
| POST | `/api/merge` | `{"tables":["orders"],"schema_version":2}` → run_id + 摘要 |
| GET | `/api/runs` `/api/runs/{run_id}` | 历史运行 / 单次运行详请 |
| GET | `/api/runs/{run_id}/snapshot` | 下载快照 JSON（attachment） |
| POST | `/api/seed` | 幂等重放样例（已归档事件不重复插入） |
| GET | `/api/samples` `/api/schemas/{1\|2}` | 样例概览 / schema 文档 |
| POST | `/api/segments` | 补传一个段：`{"source":"orders","segment_no":3,"events":[...]}`（或 `events_jsonl` 文本）→ committed/quarantined/duplicate；坏事件 422、内容冲突 409 |
| GET | `/api/segments` | 段状态看板：各来源已确认前缀 / 下一期望段 / 缺口 / 隔离段 / 失败段 |
| POST | `/api/segments/reset` | 演示重置：清段事件、段记录与发布（基础样例与历史运行保留） |
| GET | `/api/segments/samples` | 样例段内容（与页面快捷按钮同源） |
| POST | `/api/publish` | `{"tables":[...],"schema_version":1}` → 校验通过一次切换并导出；校验失败 409 保留旧版；内容未变不推进发布次数 |
| GET | `/api/publishes` | 现网当前版 / 上一版 / 切换前后对照 / 发布历史与尝试日志 |
| GET | `/api/publishes/{no}/export` | 下载某次发布随切换导出的快照 JSON |

curl 验收：

```bash
RUN=$(curl -s -X POST http://127.0.0.1:18213/api/merge -H 'Content-Type: application/json' \
  -d '{"tables":["orders","customers","inventory"],"schema_version":2}' | python -c 'import json,sys;print(json.load(sys.stdin)["run_id"])')
curl -s http://127.0.0.1:18213/api/runs/$RUN | python -m json.tool
curl -s -o /tmp/cdcwater-snapshot.json http://127.0.0.1:18213/api/runs/$RUN/snapshot
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:18213/api/merge \
  -H 'Content-Type: application/json' -d '{"tables":["ghost"],"schema_version":2}'   # 400
```

curl 验收（补传与发布）：

```bash
B=http://127.0.0.1:18213
# 乱序补传：seg1 确认 → seg3 隔离（缺口 [2]）→ seg2 级联合并
for n in 1 3 2; do
  python3 - "$n" <<'EOF
import json, sys, urllib.request
n = sys.argv[1]
events = [json.loads(l) for l in open(f"sample_data/segments/orders.seg{n}.jsonl", encoding="utf-8") if l.strip()]
req = urllib.request.Request("http://127.0.0.1:18213/api/segments",
    data=json.dumps({"source": "orders", "segment_no": int(n), "events": events}).encode(),
    headers={"Content-Type": "application/json"})
print(n, json.loads(urllib.request.urlopen(req).read())["status"])
EOF
done
curl -s $B/api/segments | python -m json.tool                       # 看板：缺口 / 隔离段
curl -s -X POST $B/api/publish -H 'Content-Type: application/json' \
  -d '{"tables":["orders","customers","inventory"],"schema_version":1}' | python -m json.tool
curl -s $B/api/publishes | python -m json.tool                      # 切换前后对照
curl -s -o /tmp/cdcwater-publish-1.json $B/api/publishes/1/export   # 导出快照
```

## 测试

```bash
python -m pytest -q                               # 本地
docker compose run --rm app python -m pytest -q    # 容器内
```

- `tests/test_merge.py`：去重、同版本冲突/同内容、墓碑、墓碑后忽略、复活、迟到仍按版本合并、样例埋点对齐。
- `tests/test_watermark.py`：迟到判定与位置、水位线公式、未闭合键、跨表合并、推进轨迹单调性。
- `tests/test_schema_gate.py`：三类结论矩阵 + 样例 v1→v2 全量（3 可放行 / 1 回填 / 5 阻断）。
- `tests/test_api.py`：成功、400/404/422、端到端（样例 → 合并 → 结论 → 快照）。
- `tests/test_segments.py`：连续段号核对、孤立段隔离不推水位线、缺口补齐级联、坏事件整段停住、同段重试幂等、409 冲突、重启后隔离段仍在隔离区、看板与重置。
- `tests/test_publish.py`：校验通过一次切换并导出、门禁阻断/隔离段/失败段保留旧版、同内容发布与同段重试不推进发布次数、切换前后对照、发布运行可回看。

## 目录

```text
app/
├── main.py          # FastAPI 路由（薄）
├── settings.py      # 路径、表清单、文案映射
├── archive.py       # 模块1 事件归档（解析/校验/幂等导入）
├── merge.py         # 模块2 同键合并
├── tombstone.py     # 模块3 墓碑/版本判定表（纯函数）
├── watermark.py     # 模块4 迟到与水位线
├── schema_gate.py   # 模块5 schema 门禁
├── snapshot.py      # 模块6 快照导出与页面预览
├── segments.py      # 模块7 分段补传（连续段号/隔离暂存/级联合并）
├── publish.py       # 模块8 原子发布（三条校验/一次切换/前后对照）
├── runner.py        # 编排 + 落库
├── store.py         # SQLite（events / runs / meta / segments / publishes / publish_attempts）
├── templates/       # base/index/workbench/result/review/segments/not_found
└── static/          # app.js / style.css（原生，无框架）
sample_data/
├── generate_samples.py   # 确定性生成器（末尾自检 8 条迟到）
├── generate_segments.py  # 补传段生成器（自检版本连续性与坏行数）
├── events/{orders,customers,inventory}.jsonl
├── segments/{表}.seg{1..4}.jsonl + {表}.seg4.bad.jsonl
└── schemas/{v1,v2}.json
tests/                      # pytest
```

## 已知限制

- 事件只支持 JSONL 落盘后的批量合并，不做实时流接入；重复跑同一批事件会得到同一结果（幂等）。
- 冲突只判定「同版本不同内容」，不做字段级自动合并；裁决后需人工重放修正事件。
- 水位线按到达顺序的前缀计算，未做事件时间的乱序窗口缓冲（迟到只标注不阻塞）。
- 类型门禁覆盖 string/int/long/decimal/timestamp/boolean 的收窄与族不兼容，不做列重命名识别。
- 段号按来源各自连续，不做跨来源的全局段序；发布校验不含冲突裁决（冲突仍走人工重放流程）。
- 单进程单 SQLite：原子切换靠单事务保证，不覆盖多实例并发发布的场景。
- 队列/上游 connector、鉴权、多租户不在本演示范围。
