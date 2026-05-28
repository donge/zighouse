-- Test that a parquet-imported table is queryable via serve.
SELECT count() FROM hits;
SELECT WatchID FROM hits LIMIT 1;
