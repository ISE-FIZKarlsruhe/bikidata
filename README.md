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
