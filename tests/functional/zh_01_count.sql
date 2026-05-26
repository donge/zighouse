DROP TABLE IF EXISTS zh_count;
CREATE TABLE zh_count (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_count VALUES (1, 10), (2, 20), (3, 30);
SELECT count() FROM zh_count;
DROP TABLE zh_count;
