SELECT COUNT(*) AS count
FROM triples AS t1
JOIN iris AS i1 ON i1.hash = t1.p AND i1.value = '<http://www.w3.org/2000/01/rdf-schema#label>'
JOIN literals AS lit ON lit.hash = t1.o
WHERE regexp_matches(lit.value, 'c.m')