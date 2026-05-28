-- Test FORMAT JSON output for a simple table
DROP TABLE IF EXISTS zh_fmtjson;
CREATE TABLE zh_fmtjson (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_fmtjson VALUES (1, 'alpha'), (2, 'beta');
SELECT id, val FROM zh_fmtjson FORMAT JSON;
DROP TABLE zh_fmtjson;
