"""
Tests for the FastAPI HTTP endpoint that serves SPARQL 1.1 queries over a
bikidata database, at /sparql, as defined in the W3C SPARQL 1.1 Protocol
https://www.w3.org/TR/sparql11-protocol/
"""

import pytest
from fastapi.testclient import TestClient

import bikidata
import bikidata.main as bikidata_main
from bikidata.server import app
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
SELECT = PREFIX + "SELECT ?s ?name WHERE { ?s :name ?name } ORDER BY ?name"
ASK = PREFIX + "ASK { ?s :name ?name }"
CONSTRUCT = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"

client = TestClient(app)


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("sparqlserver") / "test.duckdb")
    build_db(path, graph_quads(parse_graph(DATA)))
    return path


@pytest.fixture(autouse=True)
def configured_db(monkeypatch, db):
    monkeypatch.setattr(bikidata_main, "DB_PATH", db)


# The SPARQL 1.1 Query Results JSON Format


def test_get_select_answered_in_json_results_format(db):
    r = client.get("/sparql", params={"query": SELECT})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/sparql-results+json")
    body = r.json()
    assert body["head"]["vars"] == ["s", "name"]
    bindings = body["results"]["bindings"]
    assert len(bindings) == 4
    alice = next(b for b in bindings if b["s"]["value"] == "http://example.org/alice")
    assert alice["name"] == {"type": "literal", "value": "Alice", "xml:lang": "en"}
    assert any(b["name"]["type"] == "literal" and b["name"]["value"] == "Bob" for b in bindings)


def test_post_form_select_answered_in_json_results_format(db):
    r = client.post("/sparql", data={"query": SELECT})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/sparql-results+json")
    body = r.json()
    assert body["head"]["vars"] == ["s", "name"]
    assert len(body["results"]["bindings"]) == 4


def test_post_raw_query_answered_in_json_results_format(db):
    "The query can be sent unencoded in the body of a POST with content type application/sparql-query"
    r = client.post(
        "/sparql", content=SELECT, headers={"Content-Type": "application/sparql-query"}
    )
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/sparql-results+json")
    assert r.json()["head"]["vars"] == ["s", "name"]


def test_post_json_select_answered_in_json_results_format(db):
    "The query can be sent in the body of a POST with content type application/json"
    r = client.post("/sparql", json={"query": SELECT})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/sparql-results+json")
    assert r.json()["results"]["bindings"]


def test_select_answered_in_xml_results_format(db):
    r = client.post("/sparql", json={"query": SELECT}, headers={"Accept": "application/sparql-results+xml"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/sparql-results+xml")
    assert "<sparql" in r.text


def test_select_answered_in_tsv(db):
    r = client.post(
        "/sparql",
        json={"query": SELECT},
        headers={"Accept": "text/tab-separated-values"},
    )
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/tab-separated-values")
    assert r.text.split("\n")[0] == "?s\t?name"
    assert "<http://example.org/alice>\t\"Alice\"@en" in r.text
    assert "<http://example.org/bob>\t\"Bob\"" in r.text


# ASK queries


def test_ask(db):
    r = client.post("/sparql", data={"query": ASK})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/sparql-results+json")
    assert r.json() == {"head": {}, "boolean": True}

    r = client.post("/sparql", data={"query": PREFIX + 'ASK { ?s :name "NoSuchName" }'})
    assert r.status_code == 200
    assert r.json() == {"head": {}, "boolean": False}


# CONSTRUCT and DESCRIBE queries


def test_construct_answered_in_n_triples(db):
    r = client.post("/sparql", data={"query": CONSTRUCT})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/n-triples")
    assert "<http://example.org/bob> <http://example.org/age> \"25\"^^<http://www.w3.org/2001/XMLSchema#integer> ." in r.text


def test_construct_answered_in_turtle_on_accept(db):
    r = client.post("/sparql", data={"query": CONSTRUCT}, headers={"Accept": "text/turtle"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/turtle")
    assert "alice" in r.text


# Errors


def test_syntax_error_is_a_bad_request(db):
    r = client.post("/sparql", data={"query": "SELECT ?x WHERE { ?x "})
    assert r.status_code == 400
    assert r.json()["error"]["message"]


def test_missing_query_is_a_bad_request(db):
    r = client.get("/sparql")
    assert r.status_code == 400
    r = client.post("/sparql", data={})
    assert r.status_code == 400
