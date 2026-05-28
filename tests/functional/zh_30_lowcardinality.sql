-- Test LowCardinality(String) column: CREATE, INSERT, SELECT, compaction.
DROP TABLE IF EXISTS zh_lc_test;
CREATE TABLE zh_lc_test (id UInt32, cat LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_lc_test VALUES (1, 'apple'), (2, 'banana'), (3, 'apple'), (4, 'cherry');
SELECT id, cat FROM zh_lc_test ORDER BY id;
SELECT cat, count() FROM zh_lc_test GROUP BY cat ORDER BY cat;
DROP TABLE zh_lc_test;
