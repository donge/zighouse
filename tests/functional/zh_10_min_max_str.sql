DROP TABLE IF EXISTS zh_minmax_s;
CREATE TABLE zh_minmax_s (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_minmax_s VALUES (1, 'banana'), (2, 'apple'), (3, 'cherry');
SELECT min(name), max(name) FROM zh_minmax_s;
DROP TABLE zh_minmax_s;
