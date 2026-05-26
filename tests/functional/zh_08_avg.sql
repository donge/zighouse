DROP TABLE IF EXISTS zh_avg;
CREATE TABLE zh_avg (id UInt64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_avg VALUES (1, 10), (2, 20), (3, 30);
SELECT avg(val) FROM zh_avg;
DROP TABLE zh_avg;
