"""
Tests for the full-text search, the Tantivy index built by bikidata.build_fts
and the "fts" filters of bikidata.query
"""

import sys

import pytest

import bikidata
import bikidata.main as bikidata_main
from bikidata.fts import humanize_iri, fts_index_path
from loader import build_db, graph_quads, parse_graph

DATA = """
@prefix crm: <http://www.cidoc-crm.org/cidoc-crm/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://example.org/> .

ex:Rembrandt a crm:E21_Person ;
    rdfs:label "Rembrandt van Rijn"@en .

ex:OilOnCanvas a crm:E57_Material ;
    rdfs:label "Oil on canvas"@en .

ex:NightWatch a ex:E22_HumanMadeObject ;
    rdfs:label "The Night Watch"@en ;
    rdfs:comment "Militia Company of District II under the Command of Captain Frans Banninck Cocq."@en ;
    crm:P45_consists_of ex:OilOnCanvas ;
    crm:P108i_was_produced_by [
        a crm:E12_Production ;
        crm:P14_carried_out_by ex:Rembrandt ;
        crm:P3_has_note "Painted in Amsterdam\\nfor the Kloveniersdoelen"
    ] .

ex:Collection ex:contains ex:NightWatch .

ex:Unlabelled_Thing ex:note "Müller's café" .
"""

EX = "http://example.org/"


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("fts") / "test.duckdb")
    build_db(path, graph_quads(parse_graph(DATA)), fts=True)
    return path


@pytest.fixture
def query(db, monkeypatch):
    # bikidata.query is the function, which shadows the module of the same name
    monkeypatch.setattr(sys.modules["bikidata.query"], "DB_PATH", db)

    def run(*filters, **opts):
        return bikidata.query({"filters": list(filters), **opts})

    return run


def ids(result):
    return list(result["results"].keys())


def test_index_is_built_with_the_database(db):
    hits = dict(bikidata.fts_search("rembrandt", db_path=db))
    # The painting is found via the label of the artist in the production blank node
    assert set(hits) == {f"<{EX}Rembrandt>", f"<{EX}NightWatch>"}
    # A match in the title scores higher than a match in a linked entity's label
    assert hits[f"<{EX}Rembrandt>"] > hits[f"<{EX}NightWatch>"]


def test_blank_nodes_are_not_entities(db):
    hits = bikidata.fts_search("Amsterdam", db_path=db)
    assert [uri for uri, _ in hits] == [f"<{EX}NightWatch>"]


def test_stemming_accents_and_escapes(db):
    assert bikidata.fts_search("paintings", db_path=db)[0][0] == f"<{EX}NightWatch>"
    assert bikidata.fts_search("muller cafe", db_path=db)[0][0] == f"<{EX}Unlabelled_Thing>"
    # \n in the N-Triples literal is a newline, not the letter n glued to the next word
    assert bikidata.fts_search("for", db_path=db)
    assert bikidata.fts_search("nfor", db_path=db) == []


def test_unlabelled_entities_found_by_local_name(db):
    assert humanize_iri(f"<{EX}P14_carried_out_by>") == "P14 carried out by"
    assert humanize_iri(f"<{EX}someThing>") == "some Thing"
    assert bikidata.fts_search("unlabelled", db_path=db)[0][0] == f"<{EX}Unlabelled_Thing>"


def test_terms_are_conjunctive(db):
    assert bikidata.fts_search("night watch", db_path=db)
    assert bikidata.fts_search("night rembrandt", db_path=db)
    assert bikidata.fts_search("night canvas militia", db_path=db)
    assert bikidata.fts_search("night unicorn", db_path=db) == []
    assert bikidata.fts_search("night OR unicorn", db_path=db)


def test_invalid_query_syntax_is_lenient(db):
    assert bikidata.fts_search('"night watch', db_path=db)


def test_query_fts(query):
    r = query({"p": "fts", "o": "rembrandt"})
    assert ids(r) == [f"<{EX}Rembrandt>", f"<{EX}NightWatch>"]
    assert r["total"] == 2
    assert r["results"][f"<{EX}Rembrandt>"]["<http://www.w3.org/2000/01/rdf-schema#label>"] == [
        '"Rembrandt van Rijn"@en'
    ]


def test_query_fts_hops(query):
    r = query({"p": "fts 1", "o": "militia"})
    assert ids(r) == [f"<{EX}Collection>"]


def test_query_fts_property(query):
    comment = "<http://www.w3.org/2000/01/rdf-schema#comment>"
    label = "<http://www.w3.org/2000/01/rdf-schema#label>"
    assert ids(query({"p": f"fts {comment}", "o": "militia"})) == [f"<{EX}NightWatch>"]
    assert ids(query({"p": f"fts {label}", "o": "militia"})) == []
    # Literals of blank nodes are indexed under their own property
    note = "<http://www.cidoc-crm.org/cidoc-crm/P3_has_note>"
    assert ids(query({"p": f"fts {note}", "o": "amsterdam"})) == [f"<{EX}NightWatch>"]
    assert ids(query({"p": f"fts 1 {comment}", "o": "militia"})) == [f"<{EX}Collection>"]


def test_query_fts_combined_with_other_filters(query):
    person = "<http://www.cidoc-crm.org/cidoc-crm/E21_Person>"
    rdf_type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
    r = query({"p": "fts", "o": "rembrandt"}, {"op": "not", "p": rdf_type, "o": person})
    assert ids(r) == [f"<{EX}NightWatch>"]
    r = query({"p": "fts", "o": "rembrandt"}, {"op": "must", "p": rdf_type, "o": person})
    assert ids(r) == [f"<{EX}Rembrandt>"]
    r = query({"p": "fts", "o": "rembrandt"}, {"op": "should", "p": "fts", "o": "canvas"})
    assert set(ids(r)) == {f"<{EX}Rembrandt>", f"<{EX}NightWatch>", f"<{EX}OilOnCanvas>"}
    r = query({"p": "fts", "o": "rembrandt"}, {"op": "must", "p": "fts", "o": "canvas"})
    assert ids(r) == [f"<{EX}NightWatch>"]


def test_query_fts_no_hits(query):
    r = query({"p": "fts", "o": "unicorn"})
    assert r["results"] == {} and r["total"] == 0


def test_missing_index(tmp_path):
    with pytest.raises(bikidata.FTSIndexMissing):
        bikidata.fts_search("x", db_path=str(tmp_path / "nothing.duckdb"))


def test_rebuild_and_stemmer(db):
    bikidata.build_fts(db, stemmer="none")
    assert bikidata.fts_search("paintings", db_path=db) == []
    assert bikidata.fts_search("painted", db_path=db)
    bikidata.build_fts(db)
    assert bikidata.fts_search("paintings", db_path=db)


def test_does_not_overwrite_other_directories(db, tmp_path):
    other = tmp_path / "precious"
    other.mkdir()
    (other / "file.txt").write_text("keep me")
    with pytest.raises(ValueError):
        bikidata.build_fts(db, index_path=str(other))
    assert (other / "file.txt").read_text() == "keep me"


def test_index_path(monkeypatch):
    monkeypatch.setattr(bikidata_main, "DB_PATH", "x.duckdb")
    assert fts_index_path() == "x.duckdb.fts"
    assert fts_index_path("y.duckdb") == "y.duckdb.fts"
    monkeypatch.setenv("BIKIDATA_FTS_PATH", "/elsewhere")
    assert fts_index_path("y.duckdb") == "/elsewhere"
