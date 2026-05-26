DROP TABLE IF EXISTS zh_float;
CREATE TABLE zh_float (id UInt64, val Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_float VALUES (1, 1.5), (2, 2.5), (3, 3.0);
SELECT sum(val), avg(val) FROM zh_float;
DROP TABLE zh_float;
