-- Test ALTER TABLE DROP PARTITION returns OK (no-op)
DROP TABLE IF EXISTS zh_drop_part;
CREATE TABLE zh_drop_part (id UInt32, val String) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_drop_part VALUES (1, 'a'), (2, 'b');
ALTER TABLE zh_drop_part DROP PARTITION '20240101';
SELECT id, val FROM zh_drop_part ORDER BY id;
DROP TABLE zh_drop_part;
