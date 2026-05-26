DROP TABLE IF EXISTS zh_if;
CREATE TABLE zh_if (id UInt64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_if VALUES (1, 10), (2, -5), (3, 0), (4, 7);
SELECT id, if(val > 0, 'pos', 'non-pos') FROM zh_if ORDER BY id;
DROP TABLE zh_if;
