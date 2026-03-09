SELECT COUNT(DISTINCT t1.o) AS count
FROM triples AS t1
JOIN iris AS i1 ON i1.hash = t1.p AND i1.value = '<https://dblp.org/rdf/schema#hasSignature>'