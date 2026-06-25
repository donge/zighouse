# DuckDB 架构与性能设计，以及对 ZigHouse 的借鉴

本文主要提炼 Greybeam 的 [DuckDB Internals: Why is DuckDB Fast? (Part 1)](https://www.greybeam.ai/blog/duckdb-internals-part-1)，并结合 DuckDB 官方的 [Internals Overview](https://duckdb.org/docs/current/internals/overview)、[Execution Format](https://duckdb.org/docs/stable/internals/vector)、[Storage Format](https://duckdb.org/docs/stable/internals/storage.html) 和 [Indexing](https://duckdb.org/docs/current/guides/performance/indexing.html)，对照当前 ZigHouse 实现给出可落地的演进方向。

本文关注架构机制，不把 DuckDB 的嵌入式形态原样套到 ZigHouse。ZigHouse 的目标包含 ClickHouse 协议、MergeTree 文件和服务端持续写入，因此应借鉴 DuckDB 的执行契约，而不是照搬其部署模型。

## 1. 核心结论

DuckDB 快，不主要因为某一个 SIMD kernel，而是因为从 SQL 到存储形成了一条连续、可组合的低开销路径：

```text
SQL
  -> AST
  -> typed/bound logical plan
  -> rule-based + cost-aware optimization
  -> physical operators
  -> pipelines
  -> vectorized DataChunk execution
  -> local sink state / parallel combine / finalize
  -> column + row-group selective storage reads
```

其中最重要的设计原则是：

1. **尽早确定语义**：binding 阶段完成名称解析、类型解析和隐式转换，执行器不再猜测列或类型。
2. **优化规则正交可组合**：filter pushdown、unused columns、limit pushdown、TopN、join order 等是独立 pass，而不是散落在执行器中的 query-shape 判断。
3. **统一批处理契约**：所有常规算子消费和产生 `DataChunk/Vector`，默认一批 2048 行。
4. **物理表示延迟展开**：constant、dictionary、sequence 等向量可以保持压缩，通过统一向量视图被 kernel 消费。
5. **pipeline 与 breaker 明确分离**：filter/project/join probe 是 streaming operator；group by/order by/join build 是 sink。
6. **并行状态局部化**：每个线程维护 local state，随后并行 combine，避免热路径共享锁。
7. **存储减少无效工作**：列裁剪、row-group zonemap、压缩块读取与 LIMIT/谓词下推共同减少 I/O 和解压。
8. **性能可解释**：optimizer、planner、physical planner 和每个 operator 都有 profile 指标。

ZigHouse 已经具备其中不少关键积木：2048 行 `DataChunk`、`SelectionVector`、morsel source、persistent worker pool、planner 选择聚合策略、compact/wide part mark range reader、sort-key range。当前主要差距是这些能力尚未形成统一执行契约，很多路径仍在 `DataChunk`、`RowList`、raw slice fast path 和 shape-specific executor 之间切换。

## 2. DuckDB 从 SQL 到执行的架构

### 2.1 Parse、Bind、Optimize、Physical Plan 各负其责

DuckDB 将前端分成几个边界清晰的阶段：

| 阶段 | 输入 | 输出 | 主要职责 |
|---|---|---|---|
| Parser | SQL text | AST | 只表达语法结构 |
| Binder | AST + catalog | typed logical tree | 表/列/函数解析、类型检查、隐式转换、歧义报错 |
| Optimizer | bound logical tree | optimized logical tree | 等价重写、基数传播、下推、join reorder |
| Physical planner | logical operators | physical operators | 根据统计和能力选择 hash/merge/index、TopN/sort 等算法 |
| Pipeline builder | physical tree | pipelines + dependencies | 切分 source/operator/sink，建立执行依赖 |

这几个层次解决不同问题：

- logical plan 描述“算什么”；
- optimizer 在语义等价前提下改变计算顺序；
- physical plan 描述“用什么算法算”；
- pipeline scheduler 描述“何时由哪些线程算”。

DuckDB 的 optimizer 是一组小型 pass。文章列出的典型规则包括 expression rewrite、filter pullup/pushdown、CTE filter pushdown、unused columns、statistics propagation、common subexpression、limit pushdown、row-group pruning、TopN、join order 和 late materialization。单个 pass 可以观察或禁用，这让性能行为更容易定位。

值得注意的三个优化机制：

- **Filter pushdown**：先整理谓词，再尽量推近 scan，减少后续算子输入。
- **Subquery unnesting**：将相关子查询改写成 join，避免逐外层行重复执行子查询。
- **Dynamic join filter**：hash join build 完成后，从实际 key 产生 min/max 或小型 IN 集合，反向过滤 probe 侧 row groups。

### 2.2 Physical operator 与 pipeline

physical operator 并不等于线程任务。DuckDB 会把 operator tree 切成 pipeline：

```text
Pipeline 1: scan -> filter -> project -> GROUP BY sink
Pipeline 2: group source -> expression -> ORDER BY sink
Pipeline 3: sorted source -> limit -> result
```

算子分为三类角色：

- **Source**：产生 chunk，例如 table scan、hash aggregate 的结果扫描。
- **Streaming operator**：输入一个 chunk 即可产生输出，例如 filter、project、hash join probe。
- **Sink / pipeline breaker**：必须收集状态，例如 aggregate、sort、hash join build。

sink 的生命周期统一为：

1. `sink`：线程消费 chunk，写入 thread-local state。
2. `combine`：将 local state 合并到 global state；合并本身也可并行。
3. `finalize`：完成全局状态，使其成为后续 pipeline 的 source。

这个模型的价值不是术语，而是让所有 breaker 共享调度、取消、资源管理和 profile 机制。新增一种聚合 hash table 不需要新增一套 query executor，只需要提供相同生命周期下的另一种 local/global state。

### 2.3 Vectorized execution

DuckDB 的执行载体是 `DataChunk`，每列是一个 `Vector`，默认 chunk 大小为 2048 行。算子以 chunk 为单位工作，降低虚函数、分支、类型判断和函数调用的逐行成本，同时保持数据在 CPU cache 中可管理。

Vector 不只有 flat buffer：

| 形式 | 表示 | 性能价值 |
|---|---|---|
| Flat | 连续值数组 | 常规 SIMD/批处理 |
| Constant | 单值 | 常量表达式只算一次 |
| Dictionary | child vector + selection | filter、字典压缩后避免拷贝 |
| Sequence | start + increment | range/row id 无需物化 |

DuckDB 用 Unified Vector Format 给通用 kernel 提供统一的 `data + selection + validity` 视图，避免为每种向量形式组合编写专用函数。能识别 constant/dictionary 的 kernel 可以进一步特化，普通 kernel 也有稳定 fallback。

复杂类型仍保持列式：List 是 `offset/length + child vector`，Map 是 `LIST<STRUCT<key,value>>`。这比把 Array/Map 编码成字符串 blob 更容易继续做 filter、arrayJoin 和聚合。

### 2.4 Morsel-driven parallelism

一个 pipeline 内，多线程从共享 morsel source 领取小块输入，每个线程运行完整 operator chain。这样既能动态负载均衡，也不需要给每个 operator 单独建立线程和队列。

关键点是“并行状态局部化”：

- scan/filter/project 基本无共享可变状态；
- aggregate/sort/join build 使用 thread-local state；
- combine 阶段再合并；
- pipeline 之间通过显式 dependency 顺序执行。

因此，morsel 是调度单位，`DataChunk` 是算子交换单位，row group 是存储并行与裁剪单位。三者职责不同，但边界相互配合。

### 2.5 存储与执行协同

DuckDB 原生存储是列式、分 row group 的。文章给出的典型 row group 上限为 122,880 行，列在 row group 内进一步切成通常映射到固定块的 segment。row group 同时承担：

- 并行 scan 切分；
- 压缩编码选择；
- zonemap 统计边界；
- predicate pruning 单位。

每个常规类型列自动维护 zonemap，包括 min、max 和 null count。scan 在读取和解压前判断 row group 是否可能匹配谓词。数据越接近按查询列排序，min/max 范围越窄，裁剪效果越好。

Parquet 路径复用同一思想：先读 footer 和 row-group statistics，再只读取命中的列块。远端文件也按 byte range 拉取，而不是先下载整文件。

因此，DuckDB 的存储性能不是单纯“解压快”，而是：

```text
unused column elimination
  x predicate / limit pushdown
  x row-group pruning
  x compressed representation
  x vectorized decoding
```

任意一层漏掉，都可能把本应很小的查询放大成全列、全 row group 的工作。

### 2.6 In-process 的意义及其边界

文章首先强调 DuckDB in-process：它消除了网络传输和逐 value 的 ODBC/JDBC 序列化开销，并可通过 replacement scan 直接读取 DataFrame/Arrow buffer。

ZigHouse 是 ClickHouse-compatible server，不能消除 wire protocol，但仍可借鉴两点：

1. 内部执行与协议序列化必须以 column/chunk 交接，避免先转成 row `Value` 再编码。
2. 为同机应用提供 Native block、Arrow C Data 或嵌入式 SourceIface，可把 server 形态之外的零拷贝能力保留下来。

## 3. ZigHouse 当前架构映射

### 3.1 已经做对的部分

| DuckDB 机制 | ZigHouse 当前能力 | 评价 |
|---|---|---|
| 2048-row DataChunk | `src/core/chunk.zig` 的 `CHUNK_SIZE = 2048` | 已对齐 |
| Selection vector | `SelectionVector` + `evalExprSelection` | 已有，但未贯穿下游 |
| Morsel parallelism | `parallel.MorselSource` + persistent pool | 基础良好 |
| Physical strategy | `HashAggNode.strategy` | 已能自动命中特化聚合 |
| Column pruning | planner + `SourceIface.setNeededCols` | 已有，且对真实宽表很重要 |
| Range pruning | `setRowRange`、primary key granule range | 已能减少 compact/wide part 读取 |
| Lazy part read | `columnReaderRange()` 按 mark/granule 读 | 大 part 能力方向正确 |
| Local aggregation | 多个 parallel aggregate executor 使用 per-worker context | 已体现 local state 思路 |
| Typed expression IR | `core/exec/plan.zig::Expr` | 已覆盖主要标量/聚合表达式 |

这些能力解释了为什么 ZigHouse 在 ClickBench 的特定扫描聚合上可以接近或快于 DuckDB：raw mmap slice、专门 hash table、morsel 并行和 TopK 融合已经能够形成很短的热循环。

### 3.2 当前最主要的结构差距

#### A. Planner 同时承担 binding、rewrite 和 physical strategy 选择

`generic_sql.Plan -> planner.plan_query() -> PhysicalNode` 基本是一步完成。`inferHashAggStrategy()` 直接根据 key 数量、字符串 key 和 distinct 形状选择 `single_int_count_topk`、`pair_count`、`string_key` 等物理策略。

优点是短小直接，缺点是：

- binding/type coercion 失败与“不支持的物理实现”都表现为 `null` fallback；
- filter、projection、order、aggregate 的重写散落在单次建树流程；
- 策略选择难以使用 cardinality、distinct estimate、内存预算和排序信息；
- logical 等价变换与 benchmark fast path 容易耦合。

#### B. `pipeline.zig` 同时是 scheduler、operator executor 和 kernel selector

当前 `pipeline.zig` 约 1.37 万行。`executeNode()` 递归 materialize `RowList`，而多个 scannable fast path 又绕过通用 tree walk，直接读取 SourceIface/raw column。聚合策略失败后还会依次探测其他策略，再回到 chunked generic aggregation。

这带来三个风险：

- operator 生命周期不统一，取消、内存限制、profile 和错误语义难以统一实现；
- 同一 column pruning、range reset、fallback guard 在多个 executor 中重复；
- 新增通用能力时容易扩大 strategy 分支，而不是扩大所有查询可用的 operator 能力。

#### C. DataChunk 已存在，但 Value/RowList 仍是常见中间格式

`DataChunk.readRow()` 会为每行构造 `[]?Value`；`evalExprBatch()` 对简单比较和 AND/OR 有 batch path，其他表达式仍逐行调用 `evalExpr()`。filter 后虽然可以产生 SelectionVector，但 `compactSelection()` 仍是兼容桥，很多下游不直接消费 selection。

子查询的 `ChunkSource` 也先接收已物化 `ResultSet`，再逐 value 拷回新 DataChunk。这不是语义错误，但对大 CTE、subquery 和 join 中间结果会放大内存和复制。

#### D. Vector 只有 typed flat buffers，没有统一物理表示层

当前 `ColumnData` 是 typed slices，NULL 有 bitmap，这是很好的基础；但 constant、dictionary、sequence、list child vector 尚不是一等表示。ClickHouse LowCardinality/Array/Map 在 storage reader 中被解码或适配，执行层不能自然保持压缩。

结果是：

- 常量可能按行重复；
- filter 选择往往转为 compact copy；
- LowCardinality 的 code-level group/filter 优势不能通用复用；
- Array/Map 需要额外 blob/string-like 兼容逻辑。

#### E. Storage pruning 仍偏 sort-key 特化，缺少通用 statistics contract

PartScanBridge 已可根据 primary index 缩小 granule range，也能按 needed columns 做 lazy range read。但目前没有统一的 per-part/per-granule `ColumnStats` 与 predicate evaluator，难以自动支持：

- 任意列 min/max zonemap；
- null count；
- multi-column statistics；
- runtime join filter；
- bloom/token skip index；
- EXPLAIN 中展示“为什么跳过/没有跳过”。

#### F. 可观测性不足，fallback 会掩盖机制缺口

当前已有 warning，但还缺少稳定的 per-query profile：各 operator 输入/输出行数、scan bytes、granules skipped、vectorized/scalar rows、strategy 命中/拒绝原因、local combine 时间、peak memory。

没有这些指标时，一个查询变慢后很难快速判断是 I/O 放大、selection 被 materialize、batch kernel fallback，还是 hash table/merge 成本。

## 4. 对 ZigHouse 的具体借鉴

### P0：先统一执行契约，不重写已有快路径

#### 4.1 增加轻量 Bound/Logical 层和独立 optimizer pass

不需要复制 DuckDB 的 30 多个 optimizer。先引入最小四层：

```text
AST
  -> BoundPlan       名称、类型、函数、隐式 cast 均已确定
  -> LogicalPlan     scan/filter/project/agg/join/order/limit
  -> OptimizedPlan   依次运行小型 rewrite passes
  -> PhysicalPlan    选择 hash table、TopK、scan implementation
```

第一批 pass 只需要：

1. `predicate_pushdown`
2. `unused_columns`
3. `limit_pushdown`
4. `topn_rewrite`：`ORDER BY + LIMIT -> TopK`
5. `common_expression`：避免 post-project 重复求值
6. `statistics_propagation`

现有 `inferHashAggStrategy()` 保留，但从 planner 内部函数移动到 physical planner，并让输入变为：key physical types、estimated groups、order/limit、source capabilities、memory budget。这样现有性能不丢，同时策略不再与 SQL shape 直接绑定。

#### 4.2 将 pipeline 抽成统一 Source/Operator/Sink 生命周期

先不拆所有 1.37 万行代码，可从 hash aggregation 和 sort 两个 breaker 开始定义接口：

```zig
const Sink = struct {
    initLocal: fn (...) LocalState,
    sink: fn (*LocalState, DataChunk, SelectionView) !void,
    combine: fn (*GlobalState, *LocalState) !void,
    finalize: fn (*GlobalState) !Source,
};
```

现有 compact int/string/distinct hash table 继续作为不同 `AggLayout` 或 `AggTable` implementation。`PhysicalStrategy` 只选择 implementation，不再选择一整套 `executeHashAggParallelXxx` 查询流程。

迁移顺序：

1. 通用 scalar aggregate；
2. compact int grouped aggregate；
3. string grouped aggregate；
4. distinct state；
5. TopK 作为 agg result source 后的 sink，保留可融合实现。

每迁移一个 implementation，都用现有 10M gate 对比，不要求一次性替换全部 fast path。

#### 4.3 让 SelectionView 贯穿 filter -> project -> aggregate/TopK

`SelectionVector` 已经存在，下一步不是再增加 mask 类型，而是统一算子签名：

```text
ChunkView = DataChunk + optional SelectionVector
```

- filter 只更新 selection；
- projection 只计算 selected rows，constant expression 只计算一次；
- aggregate 遍历 selection，不 `compactSelection()`；
- 只有 serializer、blocking materialization 或不支持 selection 的兼容算子才 compact。

`i16 mask` 可继续作为 SIMD kernel 内部临时格式，但 operator 边界统一用 selection 或 bitset，避免每层传播 2 bytes/row mask。

### P1：建立 Vector 物理表示和 storage statistics

#### 4.4 在 ColumnData 外增加 VectorView，而不是立即重做类型系统

建议渐进支持：

```text
VectorView = flat | constant | dictionary | sequence
```

并提供类似 Unified Vector Format 的只读视图：

```text
data + validity + optional selection
```

第一阶段覆盖 int/float/date/bool/string；Array/Map 后续再改成 list/struct child vector。SourceIface 可直接产生 dictionary view，使 ClickHouse LowCardinality 在 filter/group by 中保持 code 表示。

#### 4.5 建立通用 ColumnStats 和 PrunablePredicate

在 part metadata 层提供：

```text
ColumnStats { min, max, null_count, row_count }
GranuleStats { columns[] }
PrunablePredicate { eq, range, in, is_null, and, or }
```

optimizer 将可下推谓词编译为 `PrunablePredicate`，scan 负责判断 part/granule 是否可能匹配。已有 primary index range 是其中一个更精确的实现，不应被替换。

建议写入新 part 时自动生成 min/max；接管老 ClickHouse part 时优先读取已有 primary/minmax/skip index metadata。这样 scan pruning 对 generic 与 compact/wide source 使用相同能力接口。

#### 4.6 将 source capability 显式化

当前 SourceIface 通过大量 optional function pointer 表达能力。可逐步收敛为：

```text
SourceCapabilities {
  random_range_read,
  raw_fixed_vector,
  dictionary_vector,
  primary_key_range,
  zonemap_pruning,
  parallel_partitions,
}
```

physical planner 基于 capability 选算法，executor 不再通过“调用后返回 null”反复探测。列集合也应使用动态 bitset/column id，而不是多个 executor 内固定容量的 name buffer。

### P2：补齐并行调度和可观测性

#### 4.7 从 parallelFor 提升为 pipeline scheduler

现有 persistent pool 可以保留。新增 scheduler 只需管理：

- pipeline dependency；
- morsel/source partition；
- query cancellation/deadline；
- memory budget 与 spill trigger；
- local state combine/finalize task；
- 多查询公平性。

不必一开始实现跨 pipeline 并发。DuckDB 的核心经验正是“一个 pipeline 内充分并行，依赖 pipeline 顺序推进”，这比全局复杂 DAG 调度更适合当前代码量目标。

#### 4.8 增加 EXPLAIN ANALYZE / profile contract

最小指标集：

| 层 | 指标 |
|---|---|
| Planning | parse/bind/optimizer/physical plan time，各 pass time |
| Scan | requested/read bytes、parts/granules scanned/skipped、columns read |
| Vector | input/output rows、selected rows、scalar fallback rows |
| Aggregate | strategy、estimated/actual groups、table capacity/load、combine time |
| Sort/TopK | input rows、heap size、spill bytes |
| Query | wall/cpu time、peak memory、rows returned、cancellation reason |

profile 不只是运维功能，它是控制架构复杂度的工具：每个 fallback 都应有计数器和原因，避免“结果正确但悄悄走慢路径”。

## 5. 不建议直接照搬的部分

1. **不要把 ZigHouse 改成纯 in-process**：ClickHouse wire/server compatibility 是产品目标。应优化内部 chunk 交接和 Native/Arrow 输出。
2. **不要立即实现完整 cost-based optimizer**：当前 join 复杂度和统计基础不足。先建立 typed logical boundary 与 statistics propagation。
3. **不要一次删除现有 specialized path**：它们已经证明性能价值。先把它们变成统一 sink/operator 下的 implementation，再删除重复 orchestration。
4. **不要先做 JIT**：在 selection、batch expression、compressed vector、pruning 尚未贯穿前，JIT 只会加速局部 CPU 工作，无法消除复制和 I/O 放大。
5. **不要只追求 SQL conformance 数量**：替代 ClickHouse 的优先级应是读取正确、错误不静默、核心类型/函数正确、查询可诊断，再扩展低频 DDL/事务语法。

## 6. 推荐实施路线

### Milestone A：可解释的统一执行基础

- 增加 query/operator profile 和 fallback counters。
- 把列引用改为稳定 column id/dynamic bitset，消除固定 name buffer。
- 定义 `ChunkView(DataChunk + SelectionVector?)`，迁移 filter/project/通用 aggregate。
- 为当前 physical strategy 增加明确 guard/拒绝原因。
- 性能 gate：43 query 总和不回退，单 query 不超过 DuckDB 2x 的既有要求。

### Milestone B：Planner 与 physical strategy 解耦

- 建立 BoundPlan/LogicalPlan 最小结构。
- 将 unused columns、limit、TopN、predicate pushdown 做成独立 pass。
- physical planner 使用类型、统计、limit/order 和 source capability 选择聚合实现。
- 保持已有 compact/string/distinct hash table，不重写热 kernel。

### Milestone C：统一 sink 与并行 combine

- 先迁移 scalar/grouped aggregate 到 local/sink/combine/finalize。
- 将 TopK、sort、join build 接入相同 breaker 生命周期。
- CTE/subquery 改为 chunk source 或明确 materialized sink，避免 `ResultSet -> Value -> DataChunk` 往返。

### Milestone D：存储级工作规避

- 新 part 自动维护 per-granule min/max/null count。
- scan 接收可判定的 predicate IR，统一 generic/compact/wide pruning。
- LowCardinality 以 dictionary vector 进入执行层。
- 增加 dynamic join filter；之后再评估 bloom/token index、异步预读和全局 block cache。

## 7. 近期最值得做的三个改动

如果只选择三个高收益、低失控风险的任务：

1. **profile + fallback counters**：先让慢在哪里、为何 fallback 可见，为后续所有优化建立证据。
2. **SelectionView 贯穿通用 filter/project/aggregate**：减少 row Value、arena 分配和过滤后复制，扩大所有查询的向量化收益。
3. **把聚合策略变成统一 AggSink 的 implementation**：保留现有快表和 TopK 融合，同时开始缩小 `pipeline.zig` 的分支与重复控制流。

存储侧紧随其后的是通用 granule zonemap。对于真实 ClickHouse 宽表和持续增长的数据，它通常比再优化一个 SIMD 算子更能稳定降低延迟。

## 8. 验证原则

每个架构迁移都应同时验证正确性、性能和代码收敛：

```text
zig build test --summary all
bash scripts/compact-part-query-test.sh
bash scripts/vprobe-compat-test.sh
scripts/pre-commit-perf.sh
```

另外建议新增：

- selection 0%/1%/10%/50%/100% microbench；
- flat/constant/dictionary vector kernel 对比；
- local combine 随线程数和 group cardinality 的扩展曲线；
- zonemap 命中率与实际 read bytes 测试；
- 大 CTE/subquery 的 peak RSS 测试；
- 每次提交记录总 `warm_best_sum`、最慢 query、scalar fallback rows 和 bytes read。

最终判断标准不是“是否长得像 DuckDB”，而是 ZigHouse 是否形成了同样清晰的性能闭环：planner 能表达决策，operator 有统一契约，storage 能避免无效读取，profile 能解释实际行为，而已有手写热循环仍能作为物理实现被自动选择。
