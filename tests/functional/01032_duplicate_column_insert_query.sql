DROP TABLE IF EXISTS t_dup_col;
CREATE TABLE t_dup_col (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_dup_col (a, b, c) VALUES (1, 2, 3);
SELECT a, b, c FROM t_dup_col;
DROP TABLE t_dup_col;
