DROP TABLE IF EXISTS zh_order_desc;
CREATE TABLE zh_order_desc (id UInt64, grp UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_order_desc VALUES (1, 10, 100), (2, 10, 50), (3, 20, 75), (4, 20, 25);
SELECT id FROM zh_order_desc ORDER BY id DESC;
SELECT id, val FROM zh_order_desc ORDER BY grp DESC, val ASC;
DROP TABLE zh_order_desc;
