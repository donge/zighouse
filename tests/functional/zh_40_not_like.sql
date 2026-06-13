DROP TABLE IF EXISTS zh_not_like;
CREATE TABLE zh_not_like (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_not_like VALUES (1, 'alice'), (2, 'bob'), (3, 'andrew'), (4, 'carol');
SELECT id, name FROM zh_not_like WHERE name NOT LIKE 'a%' ORDER BY id;
DROP TABLE zh_not_like;
