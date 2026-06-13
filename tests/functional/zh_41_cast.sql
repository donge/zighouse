DROP TABLE IF EXISTS zh_cast;
CREATE TABLE zh_cast (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_cast VALUES (1, 100), (2, 200);
SELECT id, CAST(val AS String) FROM zh_cast ORDER BY id;
DROP TABLE zh_cast;
