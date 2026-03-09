SELECT SUM(LENGTH(cat)) AS sum
FROM (
  SELECT STRING_AGG(lit.value, ' ') AS cat
  FROM triples AS t1
  JOIN iris AS i1 ON i1.hash = t1.p AND i1.value = '<https://dblp.org/rdf/schema#signatureDblpName>'
  JOIN literals AS lit ON lit.hash = t1.o
  GROUP BY t1.s
) AS subq