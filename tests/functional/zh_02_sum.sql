DROP TABLE IF EXISTS zh_sum;
CREATE TABLE zh_sum (id UInt64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_sum VALUES (1, 100), (2, -50), (3, 25);
SELECT sum(val) FROM zh_sum;
DROP TABLE zh_sum;
