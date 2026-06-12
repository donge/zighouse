# Known Bugs

## FIXED: macOS ARM64 `preadv` EINVAL on 1.5GB+ files

**Fix:** `src/clickhouse_format/part.zig:1166` — replaced `readFileAlloc` (Threaded Io) with `std.posix.mmap` for `data.bin`. The Threaded Io implementation on macOS ARM64 calls `preadv` which returns `EINVAL` for file sizes exceeding ~1 GB due to macOS kernel limits on `preadv` I/O vector size.

## FIXED: `parallel.zig:138` — `n_busy -= 1` integer underflow

**Fix:** Added `if (n_busy > 0)` guard. Race condition where a background worker thread decrements `n_busy` after the main thread has already exited the dispatch loop. In ReleaseFast this silently wraps (causing deadlock on next dispatch); in Debug it panics.

## FIXED: `RowList.toResultSet()` — `metas.len != columns.len` when 0 rows

**Fix:** `src/core/exec/pipeline.zig:1403` — when `num_rows == 0` but `num_cols > 0`, the old code returned `.columns = &.{}` (empty) while `.metas` had entries, causing `for (rs.metas, rs.columns)` to panic in Debug or silently produce truncated output in ReleaseFast. Now allocates empty per-type Column entries so `metas.len == columns.len`.

## 1. `DISTINCT` keyword returns repeated first value

**Query:** `SELECT DISTINCT CounterID FROM default.hits LIMIT 10`
**Expected:** 10 different CounterID values
**Actual:** 10 copies of `38`

Root cause suspected: the DISTINCT implementation in the hash aggregation pipeline writes the first match into all output rows instead of collecting unique values.

## 2. `count(DISTINCT col)` returns wrong result

**Query:** `SELECT count(DISTINCT CounterID) FROM default.hits` (1M rows)
**Expected:** > 10 (at least 11 visible in top-N GROUP BY)
**Actual:** `10`

Likely the same underlying issue as Bug #1 — `DISTINCT` implementation broken.

## 3. `LIMIT` not applied in GROUP BY for Int64 group keys

**Query:** `SELECT UserID, count() AS c FROM default.hits GROUP BY UserID ORDER BY c DESC LIMIT 5`
**Expected:** 5 rows with top UserIDs
**Actual:** All rows (83808+) returned

Affected columns: `UserID` (Int64), `RegionID` (Int32)
Works correctly for: `CounterID` (Int32), `EventDate` (Date)

Possible cause: the hash aggregation pipeline's LIMIT pushdown depends on the key type or hash table variant, and some variants don't implement the `TopK` / bounded heap optimization.

## 4. `ORDER BY` on string columns produces corrupt data

**Query:** `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10`
**Expected:** Normal SearchPhrase strings
**Actual:** Raw binary data (`\x00\x00\x00...`) mixed with some readable strings

Affects ORDER BY paths that sort scanned rows (not GROUP BY results). GROUP BY + ORDER BY on strings works correctly (Q7-Q19). The corruption suggests the string column's internal offset-based storage format is misinterpreted during the sort-and-limit path when there's no hash aggregation involved.

## 5. `POSITION('x' IN y)` standard syntax not working

**Query:** `SELECT POSITION('foo' IN 'bar')` — returns ParseFailed.
Functional form `position(a, b)` works. Standard form enters parser's `parsePositionFunc` but plan_builder → planner chain fails. Parser correctly produces `func("position", [haystack, needle])` AST but planner integration incomplete.
