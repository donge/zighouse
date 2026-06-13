DROP TABLE IF EXISTS zh_between;
CREATE TABLE zh_between (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_between VALUES (1, 5), (2, 15), (3, 25), (4, 35), (5, 45);
SELECT id, val FROM zh_between WHERE val BETWEEN 15 AND 35 ORDER BY id;
DROP TABLE zh_between;
