DROP TABLE IF EXISTS zh_like;
CREATE TABLE zh_like (id UInt64, url String) ENGINE = MergeTree ORDER BY id;
INSERT INTO zh_like VALUES (1, 'http://google.com'), (2, 'http://bing.com'), (3, 'https://google.com/search');
SELECT id FROM zh_like WHERE url LIKE '%google%' ORDER BY id;
DROP TABLE zh_like;
