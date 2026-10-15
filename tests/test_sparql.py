"""
Tests for bikidata.sparql that are specific to bikidata: the Python API, the
result format, and loading data through the regular bikidata build process.
Compliance with the SPARQL specification is tested in test_w3c.py
"""

import time

import pytest

import bikidata
import bikidata.main as bikidata_main
from bikidata.sparql_engine import to_sql
from loader import build_db, graph_quads, parse_graph

DATA = """
@prefix : <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

:alice a :Person ; :name "Alice"@en ; :age 30 ; :knows :bob .
:bob a :Person ; :name "Bob" ; :age 25 ; :knows :carol .
:carol a :Person ; :name "Carol" ; :age "41"^^xsd:integer ;
    :quote "She said \\"hi\\"\\nand left" .
:dave :knows [ :name "Anonymous" ] .
"""

PREFIX = "PREFIX : <http://example.org/>\n"


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("sparql") / "test.duckdb")
    build_db(path, graph_quads(parse_graph(DATA)))
    return path


def values(result, var):
    return [b[var]["value"] for b in result["results"]["bindings"] if var in b]


def test_select_result_format(db):
    r = bikidata.sparql(
        PREFIX + "SELECT ?s ?name ?age WHERE { ?s :name ?name OPTIONAL { ?s :age ?age } } ORDER BY ?name",
        db_path=db,
    )
    assert r["head"]["vars"] == ["s", "name", "age"]
    bindings = r["results"]["bindings"]
    assert bindings[0] == {
        "s": {"type": "uri", "value": "http://example.org/alice"},
        "name": {"type": "literal", "value": "Alice", "xml:lang": "en"},
        "age": {
            "type": "literal",
            "value": "30",
            "datatype": "http://www.w3.org/2001/XMLSchema#integer",
        },
    }
    # Unbound variables are left out of a binding
    anonymous = [b for b in bindings if b["name"]["value"] == "Anonymous"][0]
    assert anonymous["s"]["type"] == "bnode"
    assert "age" not in anonymous


def test_escaped_literals_round_trip(db):
    r = bikidata.sparql(PREFIX + "SELECT ?q WHERE { :carol :quote ?q }", db_path=db)
    assert values(r, "q") == ['She said "hi"\nand left']
    r = bikidata.sparql(
        PREFIX + 'ASK { :carol :quote "She said \\"hi\\"\\nand left" }', db_path=db
    )
    assert r["boolean"] is True


def test_ask(db):
    assert bikidata.sparql(PREFIX + "ASK { :alice :knows :bob }", db_path=db) == {
        "head": {},
        "boolean": True,
    }
    assert (
        bikidata.sparql(PREFIX + "ASK { :bob :knows :alice }", db_path=db)["boolean"]
        is False
    )


def test_construct_returns_ntriples_tuples(db):
    r = bikidata.sparql(
        PREFIX + "CONSTRUCT { ?o :knownBy ?s } WHERE { ?s :knows ?o . ?s a :Person }",
        db_path=db,
    )
    assert sorted(r) == [
        ("<http://example.org/bob>", "<http://example.org/knownBy>", "<http://example.org/alice>"),
        ("<http://example.org/carol>", "<http://example.org/knownBy>", "<http://example.org/bob>"),
    ]


def test_describe(db):
    r = bikidata.sparql(PREFIX + "DESCRIBE :bob", db_path=db)
    assert ("<http://example.org/bob>", "<http://example.org/name>", '"Bob"') in r
    assert all(s == "<http://example.org/bob>" for s, _, _ in r)


def test_aggregates_and_expressions(db):
    r = bikidata.sparql(
        PREFIX
        + """SELECT (COUNT(?p) AS ?n) (SUM(?age) AS ?total) (MAX(?age) AS ?oldest)
             WHERE { ?p a :Person ; :age ?age }""",
        db_path=db,
    )
    [b] = r["results"]["bindings"]
    assert (b["n"]["value"], b["total"]["value"], b["oldest"]["value"]) == ("3", "96", "41")


def test_property_paths(db):
    r = bikidata.sparql(PREFIX + "SELECT ?x WHERE { :alice :knows+ ?x }", db_path=db)
    assert sorted(values(r, "x")) == ["http://example.org/bob", "http://example.org/carol"]
    r = bikidata.sparql(PREFIX + "SELECT ?x WHERE { ?x :knows/:name 'Carol' }", db_path=db)
    assert values(r, "x") == ["http://example.org/bob"]
    r = bikidata.sparql(PREFIX + "SELECT ?x WHERE { :carol ^:knows ?x }", db_path=db)
    assert values(r, "x") == ["http://example.org/bob"]


def test_deeply_nested_expressions_compile_quickly(db):
    # Every non-trivial sub-expression gets its own column, so the size of the
    # generated SQL grows linearly with the nesting depth of the expression.
    expr = "?age"
    for i in range(12):
        expr = f"({expr} + {i})"
    query = PREFIX + f"SELECT ?s ({expr} AS ?x) WHERE {{ ?s :age ?age }}"
    start = time.time()
    r = bikidata.sparql(query, db_path=db)
    assert time.time() - start < 10
    assert sorted(int(v) for v in values(r, "x")) == [25 + 66, 30 + 66, 41 + 66]


def test_uses_configured_database(db, monkeypatch):
    monkeypatch.setattr(bikidata_main, "DB_PATH", db)
    r = bikidata.sparql(PREFIX + "SELECT ?n WHERE { :bob :name ?n }")
    assert values(r, "n") == ["Bob"]


def test_syntax_error():
    with pytest.raises(bikidata.SparqlError):
        bikidata.sparql("SELECT ?x WHERE { ?x ", db_path="unused.duckdb")


def test_unsupported_feature():
    with pytest.raises(bikidata.SparqlError):
        to_sql("SELECT * WHERE { SERVICE <http://example.org/sparql> { ?s ?p ?o } }")


def test_to_sql():
    sql = to_sql(PREFIX + "SELECT ?s WHERE { ?s a :Person }")
    assert sql.startswith("WITH ")
    assert "triples" in sql


def test_build_from_ntriples_file(tmp_path, monkeypatch):
    "Data loaded with bikidata.build, including named graphs from a .trig file"
    nt = tmp_path / "data.nt"
    nt.write_text(
        '<http://example.org/a> <http://example.org/label> "caf\\u00E9"@fr .\n'
        "<http://example.org/a> <http://example.org/next> <http://example.org/b> .\n"
    )
    trig = tmp_path / "graphs.trig"
    trig.write_text(
        "<http://example.org/g1> {\n"
        "<http://example.org/b> <http://example.org/label> \"bee\" .\n"
        "}\n"
    )
    db_path = str(tmp_path / "built.duckdb")
    monkeypatch.setattr(bikidata_main, "DB_PATH", db_path)
    monkeypatch.setenv("BIKIDATA_TRIPLE_PATH", str(tmp_path / "triples.tsv"))
    monkeypatch.setenv("BIKIDATA_MAP_PATH", str(tmp_path / "maps.tsv"))
    bikidata.build([str(nt), str(trig)])

    r = bikidata.sparql(
        "SELECT ?l WHERE { <http://example.org/a> <http://example.org/label> ?l }"
    )
    assert r["results"]["bindings"] == [
        {"l": {"type": "literal", "value": "café", "xml:lang": "fr"}}
    ]
    # The triple in the named graph is not in the default graph ...
    query = "SELECT ?l WHERE { <http://example.org/b> <http://example.org/label> ?l }"
    assert bikidata.sparql(query)["results"]["bindings"] == []
    # ... unless the default graph is the union of all graphs
    assert values(bikidata.sparql(query, union_default_graph=True), "l") == ["bee"]
    r = bikidata.sparql(
        "SELECT ?g ?l WHERE { GRAPH ?g { ?s <http://example.org/label> ?l } }"
    )
    assert r["results"]["bindings"] == [
        {
            "g": {"type": "uri", "value": "http://example.org/g1"},
            "l": {"type": "literal", "value": "bee"},
        }
    ]
