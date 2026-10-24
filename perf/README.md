# Performance Baselines

Use these baselines before and after refactors. A change should not regress the
current ClickBench-oriented path unless the trade-off is intentional and reviewed.

## Local 10M Baseline

Generate a candidate result:

```sh
scripts/perf-baseline.sh data/hits.parquet /tmp/zighouse-perf-store /tmp/zighouse-candidate.json 10000000
```

The runner repeats import+query three times by default and records medians.
Override with `PERF_REPEATS=5` when investigating borderline changes.

Compare against the checked-in baseline:

```sh
scripts/perf-compare.py perf/baselines/local-10m-submit.json /tmp/zighouse-candidate.json
```

Default gates:

- All 43 ClickBench queries must execute successfully and match the stored row
  count. Deterministic queries must also match their stored sorted checksum.
- Query `warm_best_sum`: max `5%` regression.
- Import total time: max `7.5%` regression, using the internal importer
  `import_phase total` metric when present. Wall time is still recorded for
  context but is more sensitive to local IO and scheduler noise.
- Per-query warm best: max `20%` regression and at least `2ms` absolute increase.

If a generality improvement intentionally regresses performance, record the
candidate JSON and make the trade-off explicit before merging the refactor.

Run the Q20 zero-copy filter regression separately against an imported store:

```sh
ZIGHOUSE_Q20_REPEATS=100 scripts/q20-regression.sh /tmp/zighouse-precommit-ir-store
```

This executes the point lookup with 1, 8, and 16 workers and fails on a crash or
missing result. Set `--require-all-checksums` on
`scripts/verify-result-baseline.py` once every query has a deterministic
checksum baseline.
