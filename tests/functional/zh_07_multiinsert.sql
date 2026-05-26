DROP TABLE IF EXISTS zh_multirow;
CREATE TABLE zh_multirow (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_multirow VALUES (1, 100), (2, 200), (3, 300);
INSERT INTO zh_multirow VALUES (4, 400), (5, 500);
SELECT count(), sum(val) FROM zh_multirow;
DROP TABLE zh_multirow;
