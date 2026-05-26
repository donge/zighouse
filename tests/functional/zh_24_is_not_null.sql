DROP TABLE IF EXISTS zh_isnull;
CREATE TABLE zh_isnull (id UInt64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_isnull VALUES (1, 10), (2, 20), (3, 30);
SELECT id FROM zh_isnull WHERE val IS NOT NULL ORDER BY id;
DROP TABLE zh_isnull;
