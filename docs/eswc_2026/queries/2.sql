SELECT COUNT(*) AS count
FROM triples AS t1
JOIN iris AS i1 ON i1.hash = t1.p AND i1.value = '<https://dblp.org/rdf/schema#signaturePublication>'
JOIN triples AS t2 ON t2.s = t1.o
JOIN iris AS i2 ON i2.hash = t2.p AND i2.value = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'
WHERE EXISTS (
  SELECT 1
  FROM triples AS t3
  JOIN iris AS i3 ON i3.hash = t3.p AND i3.value = '<http://www.w3.org/2000/01/rdf-schema#subClassOf>'
  WHERE t3.s = t2.o
)