DROP TABLE IF EXISTS zh_string;
CREATE TABLE zh_string (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_string VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
SELECT id, name FROM zh_string ORDER BY id;
DROP TABLE zh_string;
