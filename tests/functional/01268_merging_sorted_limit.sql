DROP TABLE IF EXISTS t_sorted_limit;
CREATE TABLE t_sorted_limit (n UInt32) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_sorted_limit VALUES (5), (3), (1), (4), (2);
SELECT n FROM t_sorted_limit ORDER BY n LIMIT 3;
DROP TABLE t_sorted_limit;
