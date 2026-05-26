DROP TABLE IF EXISTS zh_casestr;
CREATE TABLE zh_casestr (id UInt64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_casestr VALUES (1, 10), (2, -5), (3, 0), (4, 7);
SELECT id, CASE WHEN val > 5 THEN 'big' WHEN val > 0 THEN 'small' ELSE 'non-pos' END FROM zh_casestr ORDER BY id;
DROP TABLE zh_casestr;
