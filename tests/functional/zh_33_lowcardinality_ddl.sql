-- Test LowCardinality(String) DDL correctly preserves LC semantics (ty=low_card)
DROP TABLE IF EXISTS zh_lc_ddl;
CREATE TABLE zh_lc_ddl (id UInt32, org LowCardinality(String), method LowCardinality(String), url String) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_lc_ddl VALUES (1, 'acme', 'GET', '/api/users'), (2, 'acme', 'POST', '/api/login'), (3, 'beta', 'GET', '/health');
SELECT org, method, count() FROM zh_lc_ddl GROUP BY org, method ORDER BY org, method;
DROP TABLE zh_lc_ddl;
