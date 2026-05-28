-- Test that the compactor merges parts and the table remains queryable.
-- After compaction, count() should be 2x the single-import count (2 copies imported).
SELECT count() FROM hits;
SELECT WatchID FROM hits LIMIT 1;
