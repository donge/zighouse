# 模块与目录划分

## 产品定位

```
ZigDB     — 单机，无持久化，内存/mmap 临时 store，查完即走
ZigHouse  — 单机，持久化，ClickHouse MergeTree 兼容存储
ZigLake   — 集群（规划中，基于同一底座扩展）
ZigCloud  — SaaS（规划中）
```

## 模块归属

| 模块 | ZigDB | ZigHouse | 说明 |
|------|:-----:|:--------:|------|
| `parquet.zig` | ✓ | ✓ | Parquet 解码 |
| `schema.zig` | ✓ | ✓ | 列类型、表定义 |
| `schema_infer.zig` | ✓ | ✓ | 从 Parquet 推断 schema |
| `catalog.zig` | ✓ | ✓ | 表注册、manifest |
| `generic_sql.zig` | ✓ | ✓ | SQL 解析 |
| `generic_executor.zig` | ✓ | ✓ | 查询执行（Source union） |
| `exec/` | ✓ | ✓ | 算子层（bind / shape / group / reduce） |
| `simd.zig` / `hashmap.zig` / `parallel.zig` / `agg.zig` | ✓ | ✓ | 计算原语 |
| `lowcard.zig` / `io_map.zig` | ✓ | ✓ | 存储原语 |
| `generic_store.zig` | ✓ | — | 无压缩 mmap store，ZigDB 专用 |
| `native.zig` | ✓ | ✓ | specialized 高性能路径（ClickBench 共用）* |
| `clickbench/` | ✓ | ✓ | ClickBench benchmark，跨产品共用* |
| `clickhouse_format/` | — | ✓ | MergeTree part 读写，ZigHouse 专用 |
| `loader.zig` | — | ✓ | Parquet → MergeTree import |
| `storage.zig` | — | ✓ | 物理布局 |

\* ClickBench benchmark 入口跨两个产品共用，长期归属 ZigDB 层。

## 未来目录规划

当前所有模块平铺在 `src/`，下一步按产品边界分组（不影响现有代码，迁移时逐步进行）：

```
src/
  core/        ← 共享层
                 parquet.zig, schema.zig, schema_infer.zig, catalog.zig
                 generic_sql.zig, generic_executor.zig
                 exec/
                 simd.zig, hashmap.zig, parallel.zig, agg.zig
                 lowcard.zig, io_map.zig

  db/          ← ZigDB 专有
                 generic_store.zig   — 无压缩 mmap store
                 native.zig          — specialized 高性能路径
                 clickbench/         — ClickBench benchmark

  house/       ← ZigHouse 专有
                 clickhouse_format/  — MergeTree part 读写
                 loader.zig          — Parquet → MergeTree import
                 storage.zig         — 物理布局
```
