-- Test DateTime('Asia/Shanghai') timezone parameter is parsed and ignored gracefully
DROP TABLE IF EXISTS zh_dt_tz;
CREATE TABLE zh_dt_tz (id UInt32, ts DateTime('Asia/Shanghai'), name LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_dt_tz VALUES (1, '2024-01-15 10:00:00', 'alice'), (2, '2024-06-01 08:30:00', 'bob'), (3, '2023-12-31 23:59:59', 'carol');
SELECT id, name FROM zh_dt_tz ORDER BY id;
DROP TABLE zh_dt_tz;
