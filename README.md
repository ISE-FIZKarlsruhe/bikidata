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
