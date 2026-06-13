DROP TABLE IF EXISTS zh_union1;
DROP TABLE IF EXISTS zh_union2;
CREATE TABLE zh_union1 (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE zh_union2 (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_union1 VALUES (1, 'a'), (2, 'b');
INSERT INTO zh_union2 VALUES (3, 'c'), (4, 'd');
SELECT id FROM zh_union1 UNION ALL SELECT id FROM zh_union2 ORDER BY id;
DROP TABLE zh_union1;
DROP TABLE zh_union2;
