DROP TABLE IF EXISTS zh_limit;
CREATE TABLE zh_limit (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_limit VALUES (1), (2), (3), (4), (5);
SELECT id FROM zh_limit ORDER BY id LIMIT 2;
DROP TABLE zh_limit;
