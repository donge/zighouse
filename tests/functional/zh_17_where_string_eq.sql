DROP TABLE IF EXISTS zh_wstr;
CREATE TABLE zh_wstr (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_wstr VALUES (1, 'alice'), (2, 'bob'), (3, 'alice');
SELECT id FROM zh_wstr WHERE name = 'alice' ORDER BY id;
DROP TABLE zh_wstr;
