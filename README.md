# bikiDATA

## Developer-friendly Queries over RDF triples

Using this tool you can hit the ground running, and make some useful queries over RDF data using simple JSON calls.

### Getting started TL;DR

You have a n-triple file called `myfile.nt`.

Install bikidata by saying `pip install bikidata`

Import it into bikidata with: `python -m bikidata myfile.nt`

And now, in a python prompt, you can query things, for example:

```python
import bikidata

r = bikidata.query({
    "filters": [
        {"p":"fts",
         "o":"something"
        }
    ]
})
```

or

```python
r = bikidata.query({
    "filters": [
        { "p":"id",
          "o":"<http://example.com/id/123>"
        }
    ]
})
```

or

```python
r = bikidata.query({
    "filters": [
        {"p":"fts",
         "o":"something"
        },
        { "op":"not",
          "p":"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
          "o":"<https://swapi.co/vocabulary/Species>"
        }
    ]
})
```

For more examples, see the file: [examples.ipynb](examples.ipynb)

# Full-text search

The `fts` filters are answered by a [Tantivy](https://github.com/quickwit-oss/tantivy-py) index, that is built
together with the database, in a directory next to it (`bikidata.duckdb.fts`, or set `BIKIDATA_FTS_PATH`).
The index has one document per entity, with:

- its labels (`rdfs:label`, `skos:prefLabel`, `schema:name`, ...), weighted highest
- all its other literals
- the labels of the IRIs it links to, weighted lowest
- the literals of the blank nodes it references, unrolled up to 3 levels deep. So a painting can be found by the
  name of its artist, even when the production event in between is a blank node.

All the terms of a query have to match, and the [Tantivy query syntax](https://docs.rs/tantivy/latest/tantivy/query/struct.QueryParser.html)
can be used, for example `"night watch"` for a phrase or `rembrandt OR vermeer`. The filter `"p": "fts 1"` returns
the entities that link to the matches, `"p": "fts <iri>"` only searches the literals of property `<iri>`.
The results are sorted by score.

To (re)build the index of an existing database, for example one that was built with an earlier version of bikidata
that used the DuckDB FTS extension:

```python
import bikidata
bikidata.build_fts(stemmer="porter")  # or "german", "french", ..., or "none"

bikidata.fts_search("rembrandt", size=10)  # [(entity, score), ...]
```

Note that the index is not updated by `insert_async` and `delete_async`, rebuild it with `build_fts` after changes.

# SPARQL queries

Next to the JSON queries, the data can also be queried with [SPARQL 1.1](https://www.w3.org/TR/sparql11-query/).
The SPARQL query is translated into a single SQL query that runs on the same DuckDB tables, no extra index is needed.

```python
import bikidata

r = bikidata.sparql("""
PREFIX schema: <http://schema.org/>
SELECT ?person ?name WHERE {
    ?person a schema:Person ;
            schema:name ?name .
    FILTER(LANG(?name) = "en")
}
ORDER BY ?name
LIMIT 10
""")

for row in r["results"]["bindings"]:
    print(row["person"]["value"], row["name"]["value"])
```

- SELECT queries return a dict in the [SPARQL 1.1 Query Results JSON Format](https://www.w3.org/TR/sparql11-results-json/).
- ASK queries return `{"head": {}, "boolean": True}`.
- CONSTRUCT and DESCRIBE queries return a list of `(s, p, o)` tuples, each term encoded as in N-Triples.

Optional parameters are `db_path` (defaults to the `BIKIDATA_DB` database), `base` for resolving relative IRIs
and `union_default_graph`. Triples loaded from a .trig file are in named graphs, that you can query with `GRAPH`.
By default the default graph only contains the triples that are not in a named graph, pass `union_default_graph=True`
to use the union of all the graphs as the default graph.

To see the SQL a SPARQL query is translated into, use `bikidata.sparql_engine.to_sql(query)`.

The implementation is tested with the W3C SPARQL 1.0 and 1.1 query test suites, see `tests/test_w3c.py`, where
the few tests that do not pass are listed in `tests/w3c_known_failures.txt`. Not supported are SERVICE (federated queries),
SPARQL Update, and entailment regimes.

# SPARQL over HTTP

The SPARQL queries can also be served over HTTP with a [FastAPI](https://fastapi.tiangolo.com/) based endpoint,
that implements the [query operation of the W3C SPARQL 1.1 Protocol](https://www.w3.org/TR/sparql11-protocol/).

The query is sent to the `/sparql` endpoint with either the HTTP GET or the HTTP POST method, and the format of the
query results is selected with the `Accept` header, as required by the protocol:

- The query is sent in the `query` parameter: as a query string parameter for GET requests, as `application/x-www-form-urlencoded`
  form fields, or as the unencoded body of a POST request with content type `application/sparql-query` for POST requests.
- Optional `default-graph-uri` and `named-graph-uri` parameters select the RDF dataset, and a `base` parameter sets the base IRI
  for relative IRIs.
- The response format is the [SPARQL 1.1 Query Results JSON Format](https://www.w3.org/TR/sparql11-results-json/) (`application/sparql-results+json`,
  the default) for SELECT and ASK queries, `application/sparql-results+xml` for SELECT, or `text/tab-separated-values` for SELECT;
  CONSTRUCT and DESCRIBE queries are answered with an RDF serialization (`application/n-triples`, `text/turtle`, `application/rdf+xml`
  or `application/ld+json`).
- A query that is not legal gives a `400 Bad Request` response, a query that fails during its execution gives a `500 Internal Server Error`.

Start the server with [uvicorn](https://www.uvicorn.org/):

```bash
uvicorn bikidata.server:app --host 0.0.0.0 --port 8000
```

or with the bikidata command line interface:

```bash
python -m bikidata serve
```

and query it, for example with curl:

```bash
curl -X POST http://localhost:8000/sparql \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode 'query=PREFIX schema: <http://schema.org/> SELECT ?person ?name WHERE { ?person a schema:Person ; schema:name ?name . } LIMIT 10'
```

# Redis support

When querying non-trivial datasets of a few million triples, or handling many concurrent users, we do not want to open a new database connection for each query, and cache the results in memory.
By using [Redis](https://redis.io/) we can scale the number of bikidata workers horizontally, and share the cache between them.
The `query_async` function can now be awaited in an async context. This function places the query in a Redis queue, and awaits the result.

To use this, you need to have a Redis server running, and install the `redis` python package:

```bash
pip install redis
```

And then you need to run the bikidata worker in a separate terminal:

```bash
python -m bikidata worker
```

It is possible to run multiple workers, and they will share the load.

Then, in your code, you can await the `query_async` function in stead of the regular `query` function:
