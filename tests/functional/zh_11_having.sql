DROP TABLE IF EXISTS zh_having;
CREATE TABLE zh_having (grp UInt64, val UInt64) ENGINE = MergeTree ORDER BY grp;
INSERT INTO zh_having VALUES (1,1),(1,2),(1,3),(2,1),(2,2),(3,1);
SELECT grp, count() FROM zh_having GROUP BY grp HAVING count() > 2 ORDER BY grp;
DROP TABLE zh_having;
