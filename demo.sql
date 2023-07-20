DROP EXTENSION IF EXISTS quria cascade;
CREATE EXTENSION quria;
ALTER TABLE my_table ADD COLUMN my_column quria.fulltext;

BEGIN
   FOR i IN 1..1000 LOOP
      INSERT INTO my_table (my_column) VALUES (CAST('tepid text turtle ' || i AS quria.fulltext));
   END LOOP;
END;

CREATE INDEX my_index
ON my_table
USING quria_fts
(my_column);

INSERT INTO my_table (my_column) VALUES (CAST('tepid text turtle 3 10' AS quria.fulltext));

SELECT quria.ft_score(ctid, '{"query": "tepid text 10"}'::quria.query) from my_table WHERE my_column ~> '{"query": "tepid turtle"}' ORDER BY ft_score DESC LIMIT 10;
