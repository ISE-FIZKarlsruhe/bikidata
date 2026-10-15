"""
Runs the W3C SPARQL 1.0 and 1.1 query evaluation test suites against bikidata.sparql

The test files are a copy of the query related parts of https://github.com/w3c/rdf-tests
For every test the data is loaded into a fresh bikidata database with the
regular bikidata build process, the query is run with bikidata.sparql and the
results are compared to the expected results.

Tests listed in w3c_known_failures.txt are marked as expected failures.
"""

import logging
import math
import pathlib
from collections import Counter

import pytest
import rdflib
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.compare import isomorphic
from rdflib.namespace import RDF, XSD
from rdflib.query import Result

import bikidata
from bikidata.sparql_engine import parse
from loader import build_db, dataset_quads, graph_quads, parse_graph

HERE = pathlib.Path(__file__).parent
W3C = HERE / "w3c"

MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
QT = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-query#")
DAWGT = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#")
RS = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/result-set#")


def uri_to_path(uri) -> pathlib.Path:
    return pathlib.Path(str(uri).removeprefix("file://"))


def read_known_failures():
    known = {}
    path = HERE / "w3c_known_failures.txt"
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, _, reason = line.partition(" ")
        known[name] = reason.strip()
    return known


def collect_tests():
    tests = []
    for manifest in sorted(W3C.glob("sparql1*/*/manifest.ttl")):
        g = Graph()
        g.parse(manifest, format="turtle")
        for m in g.subjects(RDF.type, MF.Manifest):
            entries = g.value(m, MF.entries)
            if entries is None:
                continue
            for entry in rdflib.collection.Collection(g, entries):
                approval = g.value(entry, DAWGT.approval)
                if approval in (DAWGT.Withdrawn, DAWGT.Rejected):
                    continue
                test_type = g.value(entry, RDF.type)
                local = str(entry).split("#")[-1]
                name = f"{manifest.parent.parent.name}/{manifest.parent.name}/{local}"
                action = g.value(entry, MF.action)
                if test_type == MF.QueryEvaluationTest:
                    tests.append(
                        {
                            "name": name,
                            "type": "eval",
                            "query": g.value(action, QT.query),
                            "data": list(g.objects(action, QT.data)),
                            "graphData": list(g.objects(action, QT.graphData)),
                            "result": g.value(entry, MF.result),
                        }
                    )
                elif test_type in (
                    MF.PositiveSyntaxTest,
                    MF.PositiveSyntaxTest11,
                    MF.NegativeSyntaxTest,
                    MF.NegativeSyntaxTest11,
                ):
                    tests.append(
                        {
                            "name": name,
                            "type": "syntax",
                            "query": action,
                            "positive": "Positive" in str(test_type),
                        }
                    )
    return tests


# The test data has ill-typed literals on purpose, rdflib warns about each one
logging.getLogger("rdflib.term").setLevel(logging.ERROR)

KNOWN_FAILURES = read_known_failures()
TESTS = collect_tests()


def params():
    result = []
    for test in TESTS:
        marks = []
        if test["name"] in KNOWN_FAILURES:
            marks.append(
                pytest.mark.xfail(reason=KNOWN_FAILURES[test["name"]], strict=True)
            )
        result.append(pytest.param(test, id=test["name"], marks=marks))
    return result


# Loading the test data


def load_test_db(test, db_path):
    quads = []
    for data in test["data"]:
        if str(data).endswith((".trig", ".nq")):
            quads += dataset_quads(uri_to_path(data))
        else:
            quads += graph_quads(parse_graph(path=uri_to_path(data), base=str(data)))
    loaded = set()
    for graph_name in test["graphData"]:
        loaded.add(str(graph_name))
        g = parse_graph(path=uri_to_path(graph_name), base=str(graph_name))
        quads += graph_quads(g, f"<{graph_name}>")
    # Graphs mentioned in FROM and FROM NAMED are loaded as named graphs, the
    # same way a store would have them available.
    query = parse(uri_to_path(test["query"]).read_text(), base=str(test["query"]))
    clauses = query.algebra["datasetClause"] if "datasetClause" in query.algebra else None
    for clause in clauses or []:
        graph_name = clause["default"] if "default" in clause else clause["named"]
        path = uri_to_path(graph_name)
        if str(graph_name) not in loaded and path.exists():
            loaded.add(str(graph_name))
            g = parse_graph(path=path, base=str(graph_name))
            quads += graph_quads(g, f"<{graph_name}>")
    build_db(db_path, quads)


# Reading the expected results


def read_result_set_graph(g: Graph):
    "Parse a result set in the RDF result-set vocabulary"
    rs = g.value(None, RDF.type, RS.ResultSet)
    boolean = g.value(rs, RS.boolean)
    if boolean is not None:
        return {"boolean": boolean.toPython()}
    vars = [str(v) for v in g.objects(rs, RS.resultVariable)]
    rows = []
    for solution in g.objects(rs, RS.solution):
        row = {}
        for binding in g.objects(solution, RS.binding):
            row[str(g.value(binding, RS.variable))] = g.value(binding, RS.value)
        index = g.value(solution, RS.index)
        rows.append((index.toPython() if index is not None else None, row))
    ordered = any(i is not None for i, _ in rows)
    if ordered:
        rows.sort(key=lambda r: r[0])
    return {"vars": vars, "rows": [r for _, r in rows], "ordered": ordered}


def read_expected(result_uri):
    path = uri_to_path(result_uri)
    suffix = path.suffix
    if suffix in (".srx", ".srj", ".tsv", ".csv"):
        fmt = {".srx": "xml", ".srj": "json", ".tsv": "tsv", ".csv": "csv"}[suffix]
        normalize = rdflib.NORMALIZE_LITERALS
        rdflib.NORMALIZE_LITERALS = False
        try:
            with open(path, "rb") as f:
                result = Result.parse(f, format=fmt)
        finally:
            rdflib.NORMALIZE_LITERALS = normalize
        if result.type == "ASK":
            return {"boolean": result.askAnswer}
        rows = [
            {str(k): v for k, v in row.items() if v is not None}
            for row in result.bindings
        ]
        return {
            "vars": [str(v) for v in result.vars],
            "rows": rows,
            "ordered": True,
            "csv": fmt == "csv",
        }
    g = parse_graph(path=path, base=str(result_uri))
    if (None, RDF.type, RS.ResultSet) in g:
        return read_result_set_graph(g)
    return {"graph": g}


# Comparing results


NUMERIC = {
    str(XSD.decimal): float,
    str(XSD.float): float,
    str(XSD.double): float,
    str(XSD.integer): int,
}


def term_key(t):
    "A comparable key for an RDF term, with numeric literals compared by value"
    if isinstance(t, Literal):
        dt = str(t.datatype) if t.datatype else None
        if dt == str(XSD.string):
            dt = None
        lang = t.language.lower() if t.language else None
        if dt in NUMERIC:
            try:
                value = NUMERIC[dt](str(t))
                if isinstance(value, float) and not math.isnan(value):
                    value = round(value, 10)
                return ("num", dt, value)
            except ValueError:
                pass
        return ("lit", str(t), dt, lang)
    return (type(t).__name__, str(t))


def json_to_term(b):
    if b["type"] == "uri":
        return URIRef(b["value"])
    if b["type"] == "bnode":
        return BNode(b["value"])
    return Literal(
        b["value"],
        lang=b.get("xml:lang"),
        datatype=b.get("datatype"),
        normalize=False,
    )


def csv_term(t):
    "CSV results lose the term types: compare the plain values only"
    if t is None:
        return None
    if isinstance(t, BNode):
        return BNode("b")
    return Literal(str(t))


def rows_match(expected, actual, ordered):
    """Match two lists of solutions, allowing blank nodes to be renamed
    consistently. Returns True when they are equivalent."""
    if len(expected) != len(actual):
        return False

    def key(row):
        return tuple(
            sorted(
                (k, "bnode" if isinstance(v, BNode) else term_key(v))
                for k, v in row.items()
            )
        )

    if Counter(map(key, expected)) != Counter(map(key, actual)):
        return False
    if not any(isinstance(v, BNode) for row in expected for v in row.values()):
        return True

    # Search for a consistent mapping of the blank nodes
    def unify(e_row, a_row, mapping):
        if set(e_row) != set(a_row):
            return None
        mapping = dict(mapping)
        used = set(mapping.values())
        for k, ev in e_row.items():
            av = a_row[k]
            if isinstance(ev, BNode):
                if not isinstance(av, BNode):
                    return None
                if ev in mapping:
                    if mapping[ev] != av:
                        return None
                elif av in used:
                    return None
                else:
                    mapping[ev] = av
                    used.add(av)
            elif term_key(ev) != term_key(av):
                return None
        return mapping

    def search(i, remaining, mapping):
        if i == len(expected):
            return True
        candidates = [remaining[0]] if ordered else remaining
        for a_index in candidates:
            m = unify(expected[i], actual[a_index], mapping)
            if m is not None:
                rest = [r for r in remaining if r != a_index]
                if search(i + 1, rest, m):
                    return True
        return False

    return search(0, list(range(len(actual))), {})


def order_matches(expected, actual):
    "Check the order of the solutions, ignoring blank node labels"

    def key(row):
        return tuple(
            sorted(
                (k, "bnode" if isinstance(v, BNode) else term_key(v))
                for k, v in row.items()
            )
        )

    return [key(r) for r in expected] == [key(r) for r in actual]


def triples_to_graph(triples):
    g = Graph()
    if triples:
        data = "\n".join(f"{s} {p} {o} ." for s, p, o in triples)
        normalize = rdflib.NORMALIZE_LITERALS
        rdflib.NORMALIZE_LITERALS = False
        try:
            g.parse(data=data, format="nt")
        finally:
            rdflib.NORMALIZE_LITERALS = normalize
    return g


def canonical_graph(g):
    "Normalize numeric literals, so that graphs compare by value"
    out = Graph()
    for s, p, o in g:
        if isinstance(o, Literal) and o.datatype and str(o.datatype) in NUMERIC:
            try:
                o = Literal(str(term_key(o)[2]), datatype=o.datatype)
            except IndexError:
                pass
        if isinstance(o, Literal) and o.datatype == XSD.string:
            o = Literal(str(o))
        out.add((s, p, o))
    return out


def is_ordered(query):
    "Whether the solutions of the query have a defined order"
    node = parse(query).algebra.p
    while node.name in ("Slice", "Distinct", "Reduced", "Project"):
        node = node.p
    return node.name == "OrderBy"


# The tests


@pytest.mark.parametrize("test", params())
def test_w3c(test, tmp_path):
    if test["type"] == "syntax":
        text = uri_to_path(test["query"]).read_text()
        if test["positive"]:
            parse(text, base=str(test["query"]))
        else:
            with pytest.raises(bikidata.SparqlError):
                parse(text, base=str(test["query"]))
        return

    db_path = str(tmp_path / "test.duckdb")
    load_test_db(test, db_path)
    query = uri_to_path(test["query"]).read_text()
    actual = bikidata.sparql(query, db_path=db_path, base=str(test["query"]))
    expected = read_expected(test["result"])

    if "boolean" in expected:
        assert actual["boolean"] == expected["boolean"]
        return

    if "graph" in expected:
        assert isinstance(actual, list), "Expected a graph result"
        got = canonical_graph(triples_to_graph(actual))
        want = canonical_graph(expected["graph"])
        assert isomorphic(got, want), (
            f"Expected:\n{want.serialize(format='nt')}\nGot:\n{got.serialize(format='nt')}"
        )
        return

    actual_rows = [
        {k: json_to_term(v) for k, v in row.items()}
        for row in actual["results"]["bindings"]
    ]
    expected_rows = expected["rows"]
    if expected.get("csv"):
        actual_rows = [{k: csv_term(v) for k, v in r.items()} for r in actual_rows]
        expected_rows = [
            {k: csv_term(v) for k, v in r.items() if str(v) != ""}
            for r in expected_rows
        ]
        actual_rows = [{k: v for k, v in r.items() if str(v) != ""} for r in actual_rows]

    assert sorted(actual["head"]["vars"]) == sorted(expected["vars"])
    assert rows_match(expected_rows, actual_rows, ordered=False), (
        f"Query:\n{query}\nExpected:\n{expected_rows}\nGot:\n{actual_rows}"
    )
    if expected["ordered"] and is_ordered(query):
        assert order_matches(expected_rows, actual_rows), (
            f"Wrong order. Expected:\n{expected_rows}\nGot:\n{actual_rows}"
        )
