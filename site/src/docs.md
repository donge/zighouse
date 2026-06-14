# Reference

## CLI Reference

```
zighouse serve [<data_dir>] [--port=<port>]  [--schemas=<file>]
zighouse import <parquet>    [--db=<db>] [--table=<t>] [--pk=<col>] [--format=<fmt>]
zighouse bench               [--store=<dir>] [--query=<N>] [--from=<N>] [--limit=<N>]
zighouse compact             [--once] [--interval=<secs>] [--data-dir=<dir>]
zighouse query <store> <table> <sql>
zighouse inspect <parquet>
zighouse info [<data_dir>]
```

### Subcommands

| Command | Description |
|---------|-------------|
| `serve` | Start HTTP + TCP server |
| `import` | Import a Parquet file into the store |
| `bench` | Run ClickBench queries for performance measurement |
| `compact` | Merge small MergeTree parts into larger ones |
| `query` | Execute a single SQL query and print results as CSV |
| `inspect` | Show Parquet file metadata (schema, row groups, columns) |
| `info` | Show store manifest and configuration |

## SQL Coverage

### DDL

| Statement | Support |
|-----------|---------|
| `CREATE DATABASE` | ✅ (no-op) |
| `CREATE TABLE ... ENGINE = MergeTree()` | ✅ |
| `CREATE TABLE ... ENGINE = ReplacingMergeTree(ver)` | ✅ |
| `CREATE VIEW ... AS SELECT` | ✅ |
| `CREATE OR REPLACE FUNCTION` | ✅ |
| `CREATE DICTIONARY` | ✅ (no-op) |
| `ALTER TABLE ADD/DROP COLUMN` | ✅ |
| `DROP TABLE` | ✅ |
| `TRUNCATE TABLE` | ✅ |
| `DESCRIBE TABLE` | ✅ |

### DML

| Statement | Support |
|-----------|---------|
| `SELECT` | ✅ |
| `INSERT INTO ... VALUES` | ✅ |
| `INSERT INTO ... FORMAT RowBinary` | ✅ |
| `INSERT INTO ... FORMAT RowBinaryWithNamesAndTypes` | ✅ |
| `INSERT INTO ... FORMAT Native` | ✅ |
| `INSERT INTO ... FORMAT CSV` | ✅ |
| `INSERT INTO ... FORMAT JSONEachRow` | ✅ |

### SQL Features

| Feature | Support |
|---------|---------|
| `WHERE` with AND/OR conditions | ✅ |
| `GROUP BY` | ✅ |
| `HAVING` | ✅ |
| `ORDER BY` / `LIMIT` / `OFFSET` | ✅ |
| `JOIN` (hash join) | ✅ |
| `UNION ALL` | ✅ |
| `WITH` (CTE) | ✅ |
| `ARRAY JOIN` | ✅ |
| `CASE WHEN ... THEN ... ELSE ... END` | ✅ |
| `CAST(expr AS type)` | ✅ |
| `IN` / `BETWEEN` / `LIKE` | ✅ |
| Subqueries | ✅ |

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `count(*)` / `count()` / `count(1)` | Row count |
| `countIf(cond)` | Conditional count |
| `count(DISTINCT col)` | Distinct count |
| `sum(col)` | Sum |
| `avg(col)` | Average |
| `min(col)` / `max(col)` | Minimum / maximum |
| `minIf(col, cond)` / `maxIf(col, cond)` | Conditional min/max |
| `uniqExact(col)` | Exact distinct count |
| `uniqExactIf(col, cond)` | Conditional distinct count |
| `groupUniqArray(col)` | Array of distinct values |
| `any(col)` | First non-null value |
| `sumArray(arr)` | Sum of array elements |
| `sumArrayIf(arr, cond)` | Conditional sumArray |

### Scalar Functions

#### String Functions

| Function | Description |
|----------|-------------|
| `lower(str)` / `upper(str)` | Case conversion |
| `lowerUTF8(str)` / `upperUTF8(str)` | UTF-8 case conversion |
| `toString(val)` | Convert to string |
| `length(str)` | String length |
| `trim(str)` / `ltrim(str)` / `rtrim(str)` | Whitespace trimming |
| `concat(a, b, ...)` | String concatenation |
| `substring(str, pos, len)` | Substring |
| `splitByChar(sep, str)` | Split string into array |
| `startsWith(str, prefix)` | Prefix check |
| `positionCaseInsensitive(str, substr)` | Case-insensitive search |
| `regexp_replace(str, pattern, repl)` | Regex replacement |

#### Numeric Functions

| Function | Description |
|----------|-------------|
| `abs(n)` | Absolute value |
| `floor(n)` / `ceil(n)` / `round(n)` | Rounding |
| `greatest(a, b)` / `least(a, b)` | Min/max of two values |
| `sqrt(n)` | Square root |
| `intDiv(a, b)` | Integer division |
| `modulo(a, b)` | Modulo |

#### Date/Time Functions

| Function | Description |
|----------|-------------|
| `now()` | Current timestamp |
| `today()` | Current date |
| `toDate(val)` | Convert to date |
| `toStartOfHour(ts)` | Truncate to hour |
| `toStartOfDay(ts)` | Truncate to day |
| `toYYYYMMDD(ts)` | Date as integer |
| `date_part(unit, ts)` / `date_trunc(unit, ts)` | Date part extraction |
| `toHour(ts)` / `toMinute(ts)` / `toSecond(ts)` | Time components |
| `toYear(ts)` / `toMonth(ts)` / `toDayOfMonth(ts)` | Date components |

#### IP Functions

| Function | Description |
|----------|-------------|
| `isIPv4String(str)` | Check if string is valid IPv4 |
| `isIPv6String(str)` | Check if string is valid IPv6 |
| `IPv4StringToNumOrDefault(str)` | IPv4 string to integer |
| `IPv6StringToNumOrDefault(str)` | IPv6 string to binary |
| `IPv4NumToString(n)` | Integer to IPv4 string |

#### Dictionary Functions

| Function | Description |
|----------|-------------|
| `dictHas('dict', key)` | Check if key exists in dictionary |
| `dictGetOrDefault('dict', 'attr', key, default)` | Get attribute with default |

#### Conditional Functions

| Function | Description |
|----------|-------------|
| `if(cond, then, else)` | Ternary conditional |
| `multiIf(cond1, val1, cond2, val2, ..., else)` | Multi-way conditional |

#### Array Functions

| Function | Description |
|----------|-------------|
| `arrayConcat(arr1, arr2, ...)` | Concatenate arrays |
| `arrayDistinct(arr)` | Remove duplicates |
| `arrayFlatten(arr)` | Flatten nested arrays |
| `arraySlice(arr, offset, len)` | Slice array |
| `arrayMap(x -> expr, arr)` | Map over array |
| `arrayFilter(x -> cond, arr)` | Filter array |
| `arrayExists(x -> cond, arr)` | Check if any element matches |
| `arrayMax(arr)` / `arrayMin(arr)` | Max/min element |
| `arrayJoin(arr)` | Row expansion |
| `has(arr, val)` / `hasAny(arr, vals)` / `hasAll(arr, vals)` | Membership tests |

#### Map Functions

| Function | Description |
|----------|-------------|
| `mapKeys(map)` | Get map keys |
| `mapValues(map)` | Get map values |

## Data Types

| Type | ClickHouse | zighouse | Notes |
|------|-----------|----------|-------|
| `Int8` / `Int16` / `Int32` / `Int64` | ✅ | ✅ | |
| `UInt8` / `UInt16` / `UInt32` / `UInt64` | ✅ | ✅ | |
| `Float32` / `Float64` | ✅ | ✅ | |
| `String` | ✅ | ✅ | |
| `Date` | ✅ | ✅ | |
| `DateTime64(3)` | ✅ | ✅ | Stored as Int64 ms |
| `IPv4` / `IPv6` | ✅ | ✅ | IPv6 stored as 16-byte binary |
| `Array(String)` | ✅ | ✅ | |
| `Array(Int64)` | ✅ | ✅ | |
| `Map(String, Float64)` | ✅ | ✅ | |
| `Map(String, String)` | ✅ | ✅ | |
| `LowCardinality(String)` | ✅ | ✅ | Stored as String |
| `Nullable(T)` | ✅ | ✅ | |

## Configuration

### Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir=<path>` | — | Data storage directory |
| `--port=<port>` | `8123` | HTTP server port (TCP = port+1) |
| `--schemas=<file>` | — | Pre-seed schema definitions |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `ZIGHOUSE_IMPORT_TRACE` | Print import phase timings |

## HTTP Protocol

zighouse implements the ClickHouse HTTP wire protocol, compatible with
the `clickhouse-go/v2` driver in HTTP mode.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/?query=...` | Execute SQL, return TSV |
| POST | `/?query=...` | Execute SQL with body data |
| GET | `/ping` | Health check |

### Response Formats

| Format | When |
|--------|------|
| **TSV** (Tab Separated) | GET requests via `?query=` |
| **Native Block** | POST requests (clickhouse-go driver) |
| **JSON** | When `FORMAT JSON` is specified |
