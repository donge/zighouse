DROP TABLE IF EXISTS zh_where;
CREATE TABLE zh_where (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_where VALUES (1, 10), (2, 20), (3, 30), (4, 40);
SELECT id FROM zh_where WHERE val > 15 ORDER BY id;
DROP TABLE zh_where;
