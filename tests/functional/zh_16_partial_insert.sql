DROP TABLE IF EXISTS zh_partial;
CREATE TABLE zh_partial (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO zh_partial (a, c) VALUES (1, 100), (2, 200);
SELECT a, b, c FROM zh_partial ORDER BY a;
DROP TABLE zh_partial;
