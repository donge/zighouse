# zighouse Architecture

## 0. Vision

zighouse 的目标是一个**通用 schema 驱动的高性能 OLAP 引擎**：用户通过显式 schema（首选与 ClickHouse DDL 兼容）声明表结构，引擎据此进行 import、plan、execute 全流程，且执行性能与 ClickBench 专用路径同档。

### 优先级
1. **性能** — warm best 不退化；单 query ≤ 5% / ≤ 2ms
2. **通用精简** — 消除字段名/SQL 子串硬编码，算子按 capability 分派
3. **可定制** — 用户可声明任意 schema，引擎不假设业务

### 核心策略
- 算子按 (PlanShape, CapabilityTag) 二维 dispatch
- ClickBench specialized 路径在通用算子覆盖前作为旁路保留
- byte-identical 输出为硬验收标准
- 每个 PR 独立可回滚

---

## 1. 三阶段架构

```
        Parquet                       SQL
           │                           │
           ▼                           ▼
   ┌──────────────┐            ┌─────────────────┐
   │  Stage 1     │            │  Stage 2        │
   │  Load        │            │  Plan + Execute │
   │              │            │                 │
   │  parse meta  │            │  parse SQL      │
   │  decode page │            │  build plan     │
   │  encode col  │            │  infer shape    │
   │  build dict  │            │  bind columns   │
   │  write store │            │  dispatch op    │
   │  + manifest  │            │  exec + format  │
   └──────┬───────┘            └────────┬────────┘
          │                             │
          ▼                             │
   ┌──────────────┐                     │
   │ Physical     │◀────────────────────┘
   │ Store        │   mmap by name
   │              │
   │ <col>.bin    │
   │ <col>.dict   │
   │ columns.txt  │
   │ count.txt    │
   └──────────────┘                     │
                                        ▼
                                 ┌─────────────┐
                                 │  Stage 3    │
                                 │  Format     │
                                 │  → CSV      │
                                 └─────────────┘
```

### 1.1 Stage 1：Load
- 解析源文件（M0 仅 parquet）→ 按 schema 决定每列编码 → 写物理文件 + manifest
- 当前实现：`src/parquet.zig` + `src/clickbench/import.zig` (待通用化, Phase C)

### 1.2 Stage 2：Plan + Execute（**Phase A 焦点**）
- SQL parser → 物理 plan → shape inference → operator dispatch → execute
- 当前实现：`src/generic_sql.zig` + `src/clickbench/dispatch.zig` + `src/exec/*` + `src/native.zig`

### 1.3 Stage 3：Format
- 结果 → CSV / 其他格式
- 当前与 execute 混在一起（每个 formatXxx 既计算又输出）；Phase B+ 评估剥离

---

## 2. 核心抽象

### 2.1 Schema（`src/schema.zig`）

| 类型 | 职责 |
|---|---|
| `ColumnType` | 物理基础类型：`int16/int32/int64/date/timestamp/text/char` |
| `PhysicalColumn` (union) | 物理形态：`fixed / lowcard_text / hash_text / lazy_text / derived` |
| `StringCapabilities` | 12-bit capability bitmap（`group_count_top` / `count_distinct` / ...）|
| `Column` | 列定义：name + ty + physical + capabilities + ... |
| `Table` | 表定义：name + columns + findColumn |
| `CapabilityTag` (PR-A0 新增) | 派生自 (PhysicalColumn, ColumnType) 的单一 dispatch tag |

### 2.2 Binding（`src/exec/bind.zig`，PR-A1 新增）

| 类型/函数 | 职责 |
|---|---|
| `BoundColumn` (union) | 列绑定运行时载体；按 CapabilityTag 区分 variant |
| `bindColumn(native, name)` | 工厂：列名 → 查 schema → 决议 capability → 绑定到 BoundColumn |
| `lookupCapability(table, name)` | 纯 schema 查询，不依赖 native 实例 |

`BoundColumn` 不持有内存所有权，仅持有 mmap slice 引用，lifetime 与底层 store 一致。

### 2.3 Plan Shape（`src/exec/shape.zig`，PR-A2 新增）

| 类型/函数 | 职责 |
|---|---|
| `PlanShape` (enum) | 物理执行模式分类，11 种 + unknown |
| `inferShape(plan, schema)` | 从 generic_sql.Plan + schema 推断 PlanShape |

11 种 shape：
- `scalar_aggregate` — 无 group by 的纯聚合
- `lowcard_count_top` — group by lowcard_text + count(*) + top
- `lowcard_distinct_top` — group by lowcard_text + count(distinct) + top
- `fixed_count_top` — group by fixed_iN + count(*) + top
- `fixed_distinct_top` — group by fixed_iN + count(distinct) + top
- `dense_count_group` — 全密集小整数 group，无 LIMIT
- `dense_avg_count_top` — 双聚合 (avg + count) + top
- `offset_count_top` — 含 OFFSET 的 top
- `tuple_agg_top` — 多列 group key 复合
- `hashed_late_materialize_top` — hash sidecar 列上的 top
- `filtered_scalar` — scalar agg + 复合 filter（含 LIKE）
- `unknown` — 不匹配上述

### 2.4 Dispatch

```
SQL → parse → Plan
              │
              ├── inferShape(plan, schema) → PlanShape
              │
              ├── (plan.group_by 等关键列) → bindColumn → BoundColumn → CapabilityTag
              │
              ▼
   ┌──────────────────────────────────┐
   │  (PlanShape, CapabilityTag)      │
   │  二维 dispatch 表                 │
   │                                  │
   │  match → generic operator        │
   │  miss  → specialized fallback    │
   │          (ClickBench 旁路)        │
   └──────────────────────────────────┘
```

**关键设计**：
- 通用算子接受 BoundColumn variant，不依赖字段名
- specialized 路径作为兜底，q21-q24/q37-q43 等复杂 dashboard 长期保留
- `inferShape` 返回 `.unknown` 时自动 fallback

---

## 3. Phase A：Schema-Driven 算子泛化（当前焦点）

### 3.1 PR-A0：CapabilityTag

**目标**：在 schema.zig 引入 `CapabilityTag` 单一 enum 与派生函数，无副作用。

**接口**：
```zig
pub const CapabilityTag = enum {
    fixed_i16, fixed_i32, fixed_i64,
    fixed_date, fixed_timestamp,
    lowcard_text, hash_text, lazy_text, derived,
};

pub fn capabilityTag(col: Column) CapabilityTag;
```

**派生表**：
| col.physical | col.ty | → CapabilityTag |
|---|---|---|
| `.lowcard_text` | * | `.lowcard_text` |
| `.hash_text` | * | `.hash_text` |
| `.lazy_text` | * | `.lazy_text` |
| `.derived` | * | `.derived` |
| `.fixed` | `.int16` | `.fixed_i16` |
| `.fixed` | `.int32` | `.fixed_i32` |
| `.fixed` | `.int64` | `.fixed_i64` |
| `.fixed` | `.date` | `.fixed_date` |
| `.fixed` | `.timestamp` | `.fixed_timestamp` |
| `.fixed` | `.text/.char` | @panic |
| `.none` | * | @panic |

**验收**：unit test 覆盖 hits 105 列，所有列派生不 panic。

### 3.2 PR-A1：BoundColumn

**目标**：新增 `src/exec/bind.zig`，提供 BoundColumn union 与 bindColumn 工厂，无副作用。

**关键 variant**：
```zig
pub const BoundColumn = union(schema.CapabilityTag) {
    fixed_i16: struct { name, values: []const i16 },
    fixed_i32: struct { name, values: []const i32 },
    fixed_i64: struct { name, values: []const i64 },
    fixed_date: struct { name, values: []const i32 },
    fixed_timestamp: struct { name, values: []const i64 },
    lowcard_text: struct {
        name, column: *const lowcard.StringColumn,
        empty_id: ?u32, hash: ?[]const u64,
        capabilities: schema.StringCapabilities,
    },
    hash_text: struct { name, hash, source, capabilities },
    lazy_text: struct { name, source, capabilities },
    derived: struct { name, expr },
};
```

**验收**：unit test 覆盖 6 类代表列，union tag 正确。

### 3.3 PR-A2：PlanShape + inferShape

**目标**：新增 `src/exec/shape.zig`，提供 PlanShape 枚举与 inferShape 函数，无副作用。

**判定逻辑**：见 §2.3 的 11 种 shape 分类，按 `(group_by 是否存在, group_col capability, projections 形态, order_by, limit, offset)` 决策。

**验收**：unit test 喂 q1~q43 的 plan，至少 80% 命中非 unknown。

### 3.4 PR-A3：q13 切片

**目标**：改写 `matchPhraseCountTop` 为 (PlanShape, BoundColumn) dispatch；删除旧 specialized `formatSearchPhraseCountTop`。

**改动**：
- `src/clickbench/dispatch.zig` (`matchPhraseCountTop` line 57)：改用 `inferShape == .lowcard_count_top` + `bindColumn` + capability check
- `src/native.zig`：删除 `formatSearchPhraseCountTop` (line 7018-7042+) 及调用点 (line 626)
- `src/clickbench_queries.zig`：若 `search_phrase_count_top` 枚举仅服务该函数，删除

**验收**：q13 byte-identical + warm_best ≤5%。

### 3.5 PR-A4：lowcard / hash text count top 扩展

**目标**：添加 `formatHashTextCountTop`，迁 MobilePhoneModel / URL / Title 类 count top；删除对应 specialized。

**验收**：涉及 query 全部 byte-identical + warm_best ≤5%。

### 3.6 PR-A5：distinct_top + fixed_count_top

**目标**：添加 `formatLowCardTextDistinctTop` / `formatFixedCountTop` / `formatFixedDistinctTop`；迁 q5/q6/q9/q14/q15 等；删除对应 specialized。

**验收**：涉及 query 全部 byte-identical + warm_best ≤5%。

### 3.7 PR-A6：HotColumns 合并 + bindColumn 合并

**目标**：消除 `Native.HotColumns` (17 字段) 与 `reduce.HotColumns` (12 字段) 的双份；4 处 bindXxxColumn 合并为 PR-A1 的统一 bindColumn。

**改动**：
- 删除 `src/exec/reduce.zig` 中的 `HotColumns` (line 5-22)
- 删除 `src/native.zig` 中的 `reduceHot()` (line 5567) / `reduceHotWithSearchPhrase()` (line 5584)
- `Native.HotColumns` 加 `search_phrase_id` / `search_phrase_empty_id` 两个 optional 字段（吸收 reduce 版本独有部分）
- 删除 `bindClickBenchReduceColumn` (line 5600) / `bindClickBenchReduceFilterColumn` (line 5613) / `bindGenericColumn` (line 5871) / `bindLowCardTextColumn` (line 983)
- 调用点统一调 `exec/bind.zig` 的 `bindColumn`

**验收**：全 ClickBench 43 query byte-identical + warm_best ≤2%（纯重构更严格）。

### 3.8 性能验收门控

| PR | warm_best 阈值 | byte-identical 范围 |
|---|---|---|
| PR-A0/A1/A2 | 无（零调用） | 无 |
| PR-A3 | ≤5% q13 | q13 |
| PR-A4 | ≤5% per query | 涉及 query 全部 |
| PR-A5 | ≤5% per query | 涉及 query 全部 |
| PR-A6 | ≤2%（重构） | 全 43 query |

工具：
- `scripts/perf-baseline.sh` — 跑 bench 收集 warm best
- `scripts/perf-compare.py` — 比对 baseline，门控
- `scripts/clickbench-report.sh` — vs DuckDB 正确性
- `scripts/generic-regression.sh` — generic SQL 回归
- `scripts/pre-commit-perf.sh` — git hook，default `QUERY_PATH=compare`

baseline：`perf/baselines/local-10m-submit.json`

### 3.9 ClickBench specialized 旁路保留策略

**长期保留的 specialized 函数**：
- q21-q24 的 LIKE '%google%' / row sidecar / late materialization
- q37-q43 dashboard 复杂 where / offset / segment stats / hash late materialization
- UserID dense bitset / distinct top（待评估通用化方案）

**保留原因**：这些路径含查询特化的物理计划（row-id sidecar / 跨列协同 / 多阶段物化），强行通用化会引入显著抽象成本与性能退化。Phase A 范围内不动。

**dispatch 行为**：`matchGenericFallback` 保留 `Fallback` union 中对应 variant；通用算子不覆盖时自动走 specialized。

---

## 4. Phase B 大纲：Catalog 通用化

**目标**：把 `src/schema.zig` 重构为通用 catalog，支持多表注册与 ClickHouse DDL 解析。

**模块**：
- `src/catalog.zig`（替代/扩展 schema.zig）：`TableSchema` / `ColumnDef` / 表注册表
- ClickHouse DDL parser：`pub fn parseClickHouseDdl(allocator, ddl) !TableSchema`
- CLI：`zighouse schema-from-clickhouse --ddl x.sql --output y.zig`

**类型映射**（M0 子集）：

| ClickHouse | zighouse |
|---|---|
| `Int8` ~ `Int64` / `UInt8` ~ `UInt64` | `i8/u8` ~ `i64/u64` |
| `Float32/64` | `f32/f64` |
| `Date` / `DateTime` | `date` / `datetime` |
| `String` | `string` |
| `LowCardinality(String)` | `string` + `.dict` hint |
| `Nullable/Enum/DateTime64/Decimal/Array` | M2+ |

**验收**：把 ClickBench 官方 hits DDL 喂入，输出等价于现 hits_columns。

**约束**：ClickBench specialized 路径不破坏；hits 仍以 comptime 常量形式可用。

---

## 5. Phase C 大纲：Store + Loader 通用化

**目标**：持久化布局参考 ClickHouse MergeTree；通用 importParquet。

### 5.1 持久化布局（ClickHouse 风格）

```
<store>/
  <table>/
    schema.json              ← 用户 schema 序列化副本
    parts/
      all_1_1_0/
        columns.txt          ← 列清单（name + type + hint）
        count.txt            ← 行数
        <col>.bin            ← 列二进制（M0 不压缩）
        <col>.dict.bin       ← LowCardinality dict（仅 dict 列）
```

**与现 zighouse 的对照**：

| 现 zighouse | ClickHouse 风格 |
|---|---|
| `hot_AdvEngineID.i16` | `AdvEngineID.bin`（类型在 columns.txt） |
| `hot_URL.id` | `URL.bin`（dict ID） |
| `URL.dict.tsv` | `URL.dict.bin` |
| `manifest.zig-house` | `columns.txt` + `count.txt` |
| `q24_result.csv` | 移出 store（artifact 单独管理） |

**M0 不做**：LZ4/ZSTD 压缩、mark 文件、primary.idx 稀疏索引、minmax 索引、skip indexes、replication、mutation/merge。

### 5.2 模块

- `src/store.zig`（重构 storage.zig）：`Part` / `ColumnLayout` / `Manifest` 通用结构 + `StoreReaderFor(comptime schema)` 实例化
- `src/loader.zig`（新增）：通用 `importParquet(parquet, store_dir, schema)`，复用 parquet.zig + parallel.zig + lowcard.zig
- ClickBench import 改为薄包装：`importParquet(hits_parquet, store, hits_schema)`

**验收**：ClickBench import 输出新布局；ClickBench query 全部 byte-identical；warm_best ≤5%。

---

## 6. Phase D 大纲：Generic Engine 入口

**目标**：任意 schema + parquet → SQL → 结果端到端通路。

**模块**：
- `src/generic_engine.zig`：`GenericEngine` struct，`open(allocator, io, store_dir) → engine`，`run(sql, writer)`
- CLI：`zighouse generic --schema my_schema.zig --store ./store --sql "SELECT ..."`
- StoreReaderFor(comptime schema) 实例化，cache 数组 = HotColumns 通用版

**性能保证**：
- comptime schema → array index 列访问，与 hits HotColumns 字段访问字节等价
- StoreReader cache 永驻（mmap 不占 RSS）
- 算子接受 anytype source，ClickBench 与通用路径共用代码

**验收**：
```bash
zighouse generic --schema my_schema.zig --store ./store2 \
  --sql "SELECT category, COUNT(*) FROM orders GROUP BY category ORDER BY 2 DESC LIMIT 10"
```

---

## 7. 性能保证策略

### 7.1 byte-identical 验收
- compare mode：`Native.queryWithMode(.compare)` 同时跑 generic + specialized，`queryOutputsEquivalent` 比较输出
- 日志：`query_path_compare q13 ... equal=true`
- DuckDB 对比：`zighouse compare-duckdb-native` 子命令
- generic SQL 回归：`assets/generic_regression.sql` + `.expected`

### 7.2 warm_best 门控
- baseline：`perf/baselines/local-10m-submit.json`
- 单 query ≤ 5%（PR-A6 ≤ 2%）
- import_total ≤ 7.5%
- pre-commit hook：`scripts/pre-commit-perf.sh`

### 7.3 回滚策略
- 每个 PR 独立 commit
- specialized 旁路保留（PR-A3~A5 删除迁移完成的 specialized；PR-A6 删除合并的 bind 函数）
- 失败时 revert 单 commit 即可

---

## 8. 关键设计决策记录

### 8.1 为何 anytype 而非 HashMap 做列源
ClickBench HotColumns 字段直访 = 1 cycle field load。HashMap lookup 即使 O(1) 也有 hash + cmp + branch。anytype duck typing 在 Zig 中编译为完全 inline，两条路径性能字节等价。

### 8.2 为何 comptime schema 而非 runtime
comptime schema 让 `findColumn(name) → idx` 在编译期完成，列访问是数组 index load，与 hits 现状性能等价。runtime schema 走 enum tag dispatch，hot path 多一次 switch，性能略低。Phase A/B 走 comptime；Phase D 评估 runtime JSON。

### 8.3 为何保留 specialized 旁路
- 性能安全网：通用算子覆盖前不退化
- 复杂查询（q21-q24/q37-q43）通用化成本 > 收益
- byte-identical 验收依赖 specialized 作为对照

### 8.4 为何 ClickHouse 风格持久化
- ClickHouse / DuckDB 实践已验证
- columns.txt 描述类型，文件名通用，加列不需改 file pattern
- 用户熟悉度高
- 未来可逐步对齐二进制（mark / 压缩），无需重新设计目录

### 8.5 为何 Phase A 优先于 Phase B/C/D
Phase A 解决"代码写死字段名"的根本问题。Phase B/C/D 在算子已 schema-driven 后落地代价显著降低；反之先建 catalog/store/loader，generic_engine 拿到的 plan 仍只能调字段名硬编码 formatter，等于没通用。

### 8.6 为何 Phase A 内合并 HotColumns
双份 HotColumns 是当前最大代码债，每加列要改 5+ 处。Phase B catalog 重构若不先消除，会被 hits-specific HotColumns 阻塞。Phase A 内做合并风险最小（仅算子层重构，性能验收闭环已就位）。

---

## 9. 模块清单（当前 + 规划）

### 当前
| 文件 | 行数 | 职责 |
|---|---|---|
| `src/native.zig` | 13819 | Native engine + dispatch + cache + format |
| `src/parquet.zig` | 2133 | Parquet decoder |
| `src/generic_sql.zig` | 967 | 窄 SQL parser |
| `src/main.zig` | 798 | CLI 入口 |
| `src/exec/reduce.zig` | 382 | 通用 scalar reduce |
| `src/exec/group.zig` | 345 | GROUP BY 算子 |
| `src/storage.zig` | 328 | Physical layout（含 q* 常量待剥离） |
| `src/schema.zig` | 214 | Schema（hits 单表） |
| `src/clickbench_queries.zig` | 133 | ClickBench query 枚举 |
| `src/clickbench/dispatch.zig` | — | Generic fallback dispatch |
| `src/exec/string_top.zig` | — | LowCardText count top |
| `src/lowcard.zig` | — | LowCardinality 字符串列原语 |
| `src/simd.zig` / `src/hashmap.zig` / `src/parallel.zig` / `src/io_map.zig` | — | 计算原语 |

### Phase A 新增
| 文件 | 职责 |
|---|---|
| `src/exec/bind.zig` | BoundColumn + bindColumn |
| `src/exec/shape.zig` | PlanShape + inferShape |

### Phase B 新增
| 文件 | 职责 |
|---|---|
| `src/catalog.zig` | TableSchema + DDL parser（替代/扩展 schema.zig） |

### Phase C 新增
| 文件 | 职责 |
|---|---|
| `src/store.zig` | 通用 Part / Manifest / StoreReaderFor |
| `src/loader.zig` | 通用 importParquet |

### Phase D 新增
| 文件 | 职责 |
|---|---|
| `src/generic_engine.zig` | 通用引擎入口 |
