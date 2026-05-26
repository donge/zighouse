DROP TABLE IF EXISTS zh_sub;
CREATE TABLE zh_sub (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_sub VALUES (1,10),(2,20),(3,30),(4,40);
SELECT id FROM (SELECT id, val FROM zh_sub WHERE val > 15) ORDER BY id;
DROP TABLE zh_sub;
