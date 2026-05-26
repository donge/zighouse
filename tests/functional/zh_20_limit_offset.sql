DROP TABLE IF EXISTS zh_offset;
CREATE TABLE zh_offset (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_offset VALUES (1),(2),(3),(4),(5);
SELECT id FROM zh_offset ORDER BY id LIMIT 2 OFFSET 2;
DROP TABLE zh_offset;
