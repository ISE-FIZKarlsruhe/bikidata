# Bikidata

Queries over Wikidata dumps. Some background info [in these slides](FIZ-ISE_seminar_20230816.pdf).

Wikidata is an invaluable resource in our research activities. But in many cases we need to do complex queries, or extract large swathes of data. This is an example on how we can convert [offline dumps of Wikidata](https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format) into a format that we can query in a structured manner, or run extractions on.

## Summary

We download a ["truthy" dump in NT format](https://dumps.wikimedia.org/wikidatawiki/entities/latest-truthy.nt.bz2). Split the uncompressed file into smaller chunks. Index the RDF structure using a [fast hash-algorithm](https://xxhash.com/) into [parquet files](https://parquet.apache.org/) of 3 unsigned 64-bit columns (subject-predicate-object). Index the IRIs and Literals into another parquet file of two columns, (hash, literal/IRI) and query the resulting files using [DuckDB](https://duckdb.org/).

### Download and Split the files

The downloaded RDF dump files are very large, so we first split them into smaller chunks. This can be done using command in a unix shell like:  
`bzcat latest-truthy.nt.bz2 | split -u --lines=500000000`

In a dump from June 2023, this results in 16 file chunks, each containing 500 million lines of text (except for the last chunk which is obviously smaller)
The text file chunks are named xaa,xab ... xap by the split command.

### Index the RDF structure

For each chunked text file we run the command `python index.py xaa` etc. where xaa are the chunked names. The `index.py` command calculates a hash of each part of the triple, which is a large integer. A [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) is used to track if a given triple has been indexed or not. A triple is only added to the output parquet file the first time it is seen.

For each chunked file, a separate `xa?.parquet` is produced.

### Map the IRI and Literals

For each chunked text file, we also run the command `python map.py xaa` etc. This also uses a Bloomfilter to check if a given IRI or Literal has been seen, and only adds new items. But, as a given Literal or IRI can be used multiple times, only a single file `index.parquet` is produced and read into the Bloomfilter at the start of running the command. This means that the map part can not be run in parallel, and must be run sequentially for each file.

### Querying the data

Once the index and map steps have been done, we can query the data using [Duckdb](https://duckdb.org/). Running the Duckdb CLI interface, we can for example do commands like:

```SQL
select count(*) from 'xa?.parquet';
   7494368474
```

```SQL
select * from 'index.parquet' where hash = 12746726515823639617;
   <http://www.wikidata.org/entity/Q53592>
```

Or the request from [Q30078997](https://www.wikidata.org/wiki/Q30078997), to find similar items to the above book:

```SQL
WITH Q53592_po AS (SELECT p,o FROM 'xa?.parquet' WHERE s = 12746726515823639617)
SELECT p_cnt, (SELECT iri FROM 'index.parquet' WHERE hash = s)
  FROM (SELECT t.s, count(t.p) p_cnt FROM 'xa?.parquet' t
   INNER JOIN Q53592_po ON t.p = Q53592_po.p AND t.o = Q53592_po.o
   GROUP BY t.s
   ORDER BY count(t.p) DESC)
WHERE p_cnt > 10;
```

which is an interpretation of:

```sparql
SELECT ?book (COUNT(DISTINCT ?o) as ?score)
WHERE {
  wd:Q53592 ?p ?o .
  ?book wdt:P31 wd:Q571 ;
        ?p ?o .
} GROUP BY ?book
ORDER BY DESC(?score)
```

(which has a timeout when you run it on the [WDQS](https://query.wikidata.org/))

## TODO List

- [ ] Add some explanation on: "Why not just use HDT?"
- [ ] Sort the tri table to improve speed of queries. Solve OOM problem, sort has to be on-disk.
- [ ] Make smaller extracts, like (P31 Q5)
- [ ] A quick Property lookup, with labels
- [ ] A "labels" service
- [x] A bikidata package on PYPI
- [ ] Index literals using embeddings and a HNSW (SBERT + FAIS?)
- [ ] Make a fast membership index for the large P31 sets using a Bloomfilter, and add it as a UDF to bikidata package
- [ ] Add a SPARQL translation engine 🤓 Hah, ambitious.

## Also see

[qEndpoint for Wikidata](https://github.com/the-qa-company/qEndpoint#qacompanyqendpoint-wikidata)

[Triplestore Benchmarks for Wikidata](https://github.com/SINTEF-9012/rdf-triplestore-benchmark/tree/main/Queries/wikidata_queries)

[Python HDT](https://pypi.org/project/hdt/)
