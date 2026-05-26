DROP TABLE IF EXISTS zh_tostr;
CREATE TABLE zh_tostr (id UInt64, val Int64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_tostr VALUES (1, 10, 'alice'), (2, 20, 'bob'), (3, 30, 'alice');
SELECT concat(name, ':', toString(val)) FROM zh_tostr ORDER BY id;
DROP TABLE zh_tostr;
