DROP TABLE IF EXISTS zh_cdist;
CREATE TABLE zh_cdist (id UInt64, cat UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_cdist VALUES (1,10),(2,20),(3,10),(4,30),(5,20);
SELECT count(distinct cat) FROM zh_cdist;
DROP TABLE zh_cdist;
