# Parallel framework (E-stage)

Status: shipped. Powers Q11, Q12, Q19, Q26 (no parallelism but shares
helpers), Q31, Q36. Without it those queries lose to DuckDB-MT by 1.4–1.5×;
with it they win by 0.3–0.6×.

This document is the contract between query implementations in
`src/native.zig` and the primitives in `src/parallel.zig` /
`src/hashmap.zig`. Read it before adding a new parallel query so you
inherit the same trade-offs and don't reinvent edge cases.

## Why morsel-driven parallelism

Each ClickBench query is a one-shot CLI invocation against
mmap'ed columnar files. There is no query planner, no operator tree, no
persistent runtime — just `fn formatXxx(...)` functions that read columns
and write CSV. We pick parallelism that fits this shape:

- **No work-stealing queue.** A single atomic cursor (`MorselSource`) hands
  out fixed-size row chunks. Workers loop `next()` until exhaustion.
  Load balancing is automatic because slow workers naturally claim fewer
  morsels — exactly the property Leis et al. (2014) describe.
- **No persistent thread pool.** `parallelFor` spawns N-1 threads, runs
  the last context inline on the main thread, then `join`s. Spawn cost
  (~50 µs) is below morsel-source granularity; not worth a pool.
- **No pipelining.** Queries are scan-aggregate-emit. Pipelining would
  require an operator framework we're explicitly avoiding.

## Threading model

`parallel.defaultThreads()`:
- Apple Silicon: returns `hw.perflevel0.physicalcpu` (P-cores only).
  E-cores are ~3× slower per core under our compute-bound workloads;
  including them stretches wall-clock by the slowest-finisher penalty.
  This was measured: T=10 (all cores) was 15% slower than T=4 (P-only)
  on Q11.
- Other OS: `std.Thread.getCpuCount()`.
- Capped at 16. Q12's per-thread bitset is 140 MB; T=16 → 2.2 GB. We
  bound this regardless of platform.

`parallel.availableMemoryMiB()` reports physical RAM via `hw.memsize`.
Q12 uses it to downscale T when per-thread bitsets would exceed budget.

## Morsel size

`default_morsel_size = 122_880` rows = 60 × 2048. Matches DuckDB's
`STANDARD_VECTOR_SIZE` × default vectors-per-row-group. At 100M rows
this gives 813 morsels — ~200 morsels per worker at T=4. Empirically
this yields <2% wall-clock variance across runs.

Smaller morsels (e.g. 4096) increase contention on the atomic cursor
without improving balance. Larger morsels (e.g. 1M) cause stragglers
on the last few morsels — the tail dominates.

## Hash aggregation

Two implementations in `src/hashmap.zig`:

### `HashU64Count` (sequential)
Open-addressing linear-probe table. Key = `u64`, value = `u32` count.
Empty sentinel = `0xFFFF_FFFF_FFFF_FFFF`. Used by Q17 (24M groups,
warm best 0.4 s sequential — already fast enough that Pass1
parallelism wouldn't pay back the merge cost).

**Pitfall:** `assert(key != empty_key)` is a no-op in ReleaseFast.
A key collision with the sentinel causes silent infinite probe.
Pack carefully: encode keys so that 0xFFFF... is unreachable. Q19
reserves only the lower 54 bits for this reason.

### `PartitionedHashU64Count` (parallel-friendly)
- `partition_bits = 6` → 64 partitions.
- `partition_shift = 58` → top 6 bits of the key select the partition.
- Each partition is a private `HashU64Count`.
- `bump(key)` is lock-free for the **owner thread** (each worker has its
  own table). It is *not* thread-safe.

**Pipeline (Q31, Q36):**
1. Allocate one `PartitionedHashU64Count` per worker, sized
   `expected_total / n_threads + 1`.
2. `parallelFor` morsel scan: each worker bumps into its own table.
3. `mergePartition(p)` combines partition `p` from all workers into a
   single `HashU64Count`. Run in parallel via `parallelIndices`.
4. Each merge worker keeps a per-partition top-K heap.
5. Final sequential reduce of `n_workers` per-worker top-K into a
   global top-K.

**Why partitioning works:** the top 6 bits of the key are a coarse
hash. Two keys that go to the same partition in worker A also go to
the same partition in worker B. Therefore merging partition `p`
across workers needs no synchronization with merging partition `q`.

### `HashU64Tuple3Count` and `PartitionedHashU64Tuple3Count`
Same shape as the count tables but value is a 3-tuple
`(count u32, sum_a u32, sum_b u64)`. Used by Q31 to aggregate
`COUNT(*)`, `SUM(IsRefresh)`, `SUM(ResolutionWidth)` simultaneously.

## Sizing partitioned tables

The `expected_total` parameter sets the per-partition capacity.
Underestimating → load factor > 0.5 → linear-probe degenerates to
worst-case → table appears to hang during pass1.

Q31 hit this: initial estimate 1.5M; actual cardinality 6M.
Pass1 took >60 s in `bump()` because every probe hit a full chain.
Lesson: when in doubt, *over*-estimate. Memory cost is `16 B/key
× expected_total / partition_count × n_threads`. For 12M expected
total at T=4: 12M × 16 / 64 × 4 = 12 MB per partition × 4 workers =
48 MB total. Cheap.

## Top-K reduction

Inline insertion-sort into a fixed `[K]Row`. K is always 10 or 100
in ClickBench. Comparator is hand-written per query (`q19Before`,
`q31Before`, etc) because the tie-break order matters for byte-equal
output against DuckDB.

**Tie-break observation:** when count is tied, DuckDB's order is
implementation-defined (hash-table iteration order). For Q17 and Q19
DuckDB happens to yield UserID-ascending; for Q31 it yields some
other order we couldn't predict. The bench harness times only — it
doesn't verify output — so we accept divergent ordering when the
underlying multiset is identical.

## Per-query parallel patterns

Three patterns in production:

### A. Pass1 dense count + Pass2 fan-out (Q11, Q12)
- Pass1: per-thread local `[dict_size]u32` counters → reduce to global.
- Pass2: pick top-N candidate keys from global counts.
- Pass3: per-thread bitset fill for each candidate, OR-merged in chunks.
- Counts COUNT(DISTINCT) by popcount of the OR-merged bitset.

Q11 dict_size = 7M (MobilePhoneModel). Pass1 was the hidden
bottleneck — sequential it took 0.1 s out of a 0.21 s total. Going
parallel dropped the whole query to 0.080 s.

### B. Partitioned hash agg + parallel merge (Q19, Q31, Q36)
- `PartitionedHashU64Count` (or `Tuple3` variant) pass1.
- `parallelIndices` fan-out over 64 partitions, each merge worker
  builds its own top-K.
- Final reduce of n_workers top-K.

This is the workhorse pattern. Memory bounded, scales linearly with
P-core count, deterministic up to tie-break.

### C. Sequential lex-min top-K (Q26)
Q26 sorts by SearchPhrase string. The string compare is the hot loop;
parallelizing it adds merge cost without removing the comparison cost.
Sequential is fine: 0.149 s warm best vs DuckDB 0.374 s.

## Tests

`build.zig` adds `parallel_tests` and `hashmap_tests` to the `test`
step. They cover:
- MorselSource non-overlapping coverage and total=0.
- `parallelFor` sums a known sequence at T=4.
- `defaultThreads()` returns ∈ [1, 16].
- `HashU64Count` insert/lookup/iterator.
- `HashU64Tuple3Count` basic add and merge across two tables.
- `PartitionedHashU64Count` partition assignment and `mergePartition`.

Run: `zig build test`.

## DuckDB divergence catalog

Cases where native intentionally differs from DuckDB:

| Query | Divergence | Reason |
|-------|-----------|--------|
| Q31 | 10th row ClientIP differs (count=1058 has 2 candidates with same count, mine returns the smaller, DuckDB returns the larger) | DuckDB iteration order is hash-table-dependent; bench doesn't verify |
| Q19 | None observed | Tie-breaks happen to align (UserID asc) |
| Q26 | None observed | Lex order is well-defined |
| Q11/Q12/Q36 | None observed | Top-N counts have no ties at the boundary in this dataset |

When a divergence appears in a future port: first verify the multiset
is identical (group/aggregate the rows); if so, document the tie-break
delta here and move on. Do *not* contort the implementation to match
DuckDB's hash order.

## Adding a new parallel query

1. Identify the aggregation pattern (A / B / C above).
2. Estimate post-aggregation cardinality (`COUNT(DISTINCT ...)` via
   DuckDB on the existing data) and oversize the table by 2×.
3. Pack the group key into u64; reserve top 6 bits for partition,
   ensure key never equals `0xFFFF...` (avoid all-ones in any field).
4. Write the comparator. Match DuckDB's apparent tie-break if cheap;
   otherwise document the divergence.
5. Add an `isXxx` strict-string matcher at top of `Native.runQuery`,
   and a `formatXxx` impl following the partitioned-agg template
   (`formatClientIpTop10` for pattern B is the cleanest reference).
6. Verify with `diff` against `--backend duckdb`.
7. Bench with `bench-one`; record warm-best in `/tmp/native_bench_v2.txt`.
8. Update `docs/query_families.md` headline counts.
