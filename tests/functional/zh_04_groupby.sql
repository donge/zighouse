DROP TABLE IF EXISTS zh_group;
CREATE TABLE zh_group (grp UInt64, val UInt64) ENGINE = MergeTree ORDER BY grp;
INSERT INTO zh_group VALUES (1, 10), (1, 20), (2, 5), (2, 15);
SELECT grp, sum(val) FROM zh_group GROUP BY grp ORDER BY grp;
DROP TABLE zh_group;
