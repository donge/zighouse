-- Materialized view test: setup done in pretest, just query and cleanup here.
SELECT id, val_doubled FROM zh_mv_dst ORDER BY id;
DROP TABLE zh_mv_src;
DROP TABLE zh_mv_dst;
