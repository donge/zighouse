CREATE TABLE t27 (id UInt64, dt DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t27 VALUES (1,'2024-01-15 10:30:45'),(2,'2023-06-01 00:00:00'),(3,'2022-12-31 23:59:59');
SELECT id, toYear(dt), toMonth(dt), toDayOfMonth(dt), toHour(dt), toMinute(dt), toSecond(dt) FROM t27 ORDER BY id;
