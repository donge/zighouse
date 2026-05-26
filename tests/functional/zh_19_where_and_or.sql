DROP TABLE IF EXISTS zh_andor;
CREATE TABLE zh_andor (id UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_andor VALUES (1,10,5),(2,20,3),(3,5,8),(4,15,6);
SELECT id FROM zh_andor WHERE a > 10 AND b < 7 ORDER BY id;
DROP TABLE zh_andor;
