# Bikidata

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
