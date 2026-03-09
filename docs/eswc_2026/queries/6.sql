WITH RECURSIVE related(start_node, current_node) AS (
  SELECT t2.s AS start_node, t2.o AS current_node
  FROM triples AS t2
  JOIN iris AS i2 ON i2.hash = t2.p AND i2.value = '<https://dblp.org/rdf/schema#relatedStream>'
  UNION
  SELECT r.start_node, t3.o AS current_node
  FROM related AS r
  JOIN triples AS t3 ON t3.s = r.current_node
  JOIN iris AS i3 ON i3.hash = t3.p AND i3.value = '<https://dblp.org/rdf/schema#relatedStream>'
)
SELECT COUNT(*) AS count
FROM triples AS t1
JOIN iris AS i1 ON i1.hash = t1.p AND i1.value = '<https://dblp.org/rdf/schema#publishedInStream>'
JOIN related AS r ON r.start_node = t1.o