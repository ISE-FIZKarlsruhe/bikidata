"Helpers to load RDF data into a fresh bikidata database for testing"

import os
import rdflib
from rdflib import Dataset, Graph

import bikidata.main as bikidata_main
from bikidata.sparql_engine import term_to_nt

SCHEMA = """
create table if not exists literals (hash ubigint, value varchar);
create table if not exists iris (hash ubigint, value varchar);
create table if not exists triples (s ubigint, p ubigint, o ubigint, g ubigint);
"""


def build_db(db_path, quads, fts=False):
    """Build a bikidata database at db_path with the bikidata build process.
    quads are (s, p, o, g) tuples of N-Triples encoded terms, g is "" for the
    default graph. The full-text index is only built when fts is True, as it
    takes a few seconds even for tiny databases."""
    quads = list(quads)
    workdir = os.path.dirname(db_path)
    old = (
        bikidata_main.DB_PATH,
        os.environ.get("BIKIDATA_TRIPLE_PATH"),
        os.environ.get("BIKIDATA_MAP_PATH"),
    )
    bikidata_main.DB_PATH = db_path
    os.environ["BIKIDATA_TRIPLE_PATH"] = os.path.join(workdir, "triples.tsv")
    os.environ["BIKIDATA_MAP_PATH"] = os.path.join(workdir, "maps.tsv")
    try:
        if quads:
            bikidata_main.build_from_iterator(iter(quads), fts=fts)
        else:
            import duckdb

            con = duckdb.connect(db_path)
            con.execute(SCHEMA)
            con.close()
    finally:
        bikidata_main.DB_PATH = old[0]
        for key, value in zip(("BIKIDATA_TRIPLE_PATH", "BIKIDATA_MAP_PATH"), old[1:]):
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def dataset_quads(path):
    "The quads of a TriG or N-Quads file, the default graph gets the name \"\""
    normalize = rdflib.NORMALIZE_LITERALS
    rdflib.NORMALIZE_LITERALS = False
    try:
        ds = Dataset()
        ds.parse(path, format=rdflib.util.guess_format(str(path)))
    finally:
        rdflib.NORMALIZE_LITERALS = normalize
    for s, p, o, g in ds.quads():
        name = "" if g is None or g == rdflib.graph.DATASET_DEFAULT_GRAPH_ID else term_to_nt(g)
        yield term_to_nt(s), term_to_nt(p), term_to_nt(o), name


def graph_quads(graph: Graph, graph_name: str = ""):
    for s, p, o in graph:
        yield term_to_nt(s), term_to_nt(p), term_to_nt(o), graph_name


def parse_graph(data: str = None, path: str = None, format: str = None, base=None):
    "Parse RDF without normalizing literals, so lexical forms are kept"
    normalize = rdflib.NORMALIZE_LITERALS
    rdflib.NORMALIZE_LITERALS = False
    try:
        g = Graph()
        if path is not None:
            fmt = format or rdflib.util.guess_format(path) or "turtle"
            g.parse(path, format=fmt, publicID=base)
        else:
            g.parse(data=data, format=format or "turtle", publicID=base)
        return g
    finally:
        rdflib.NORMALIZE_LITERALS = normalize
