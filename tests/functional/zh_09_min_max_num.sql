DROP TABLE IF EXISTS zh_minmax;
CREATE TABLE zh_minmax (id UInt64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_minmax VALUES (1, 30), (2, 10), (3, 20);
SELECT min(val), max(val) FROM zh_minmax;
DROP TABLE zh_minmax;
