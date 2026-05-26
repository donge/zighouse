DROP TABLE IF EXISTS zh_date;
CREATE TABLE zh_date (id UInt64, d Date) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_date VALUES (1, '2024-01-01'), (2, '2024-06-15'), (3, '2023-12-31');
SELECT id, d FROM zh_date ORDER BY d;
DROP TABLE zh_date;
