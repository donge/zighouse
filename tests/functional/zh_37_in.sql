DROP TABLE IF EXISTS zh_in;
CREATE TABLE zh_in (id UInt64, val UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_in VALUES (1, 10, 'alice'), (2, 20, 'bob'), (3, 30, 'carol'), (4, 40, 'alice');
SELECT id, name FROM zh_in WHERE id IN (1, 3) ORDER BY id;
SELECT id, name FROM zh_in WHERE id NOT IN (2, 4) ORDER BY id;
SELECT id, val FROM zh_in WHERE val IN (10, 30, 50) ORDER BY id;
DROP TABLE zh_in;
