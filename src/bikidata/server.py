"""
A FastAPI based HTTP endpoint to serve SPARQL 1.1 queries over a bikidata database.

The endpoint implements the query operation of the W3C SPARQL 1.1 Protocol
(https://www.w3.org/TR/sparql11-protocol/):

- The query is sent with either the HTTP GET or the HTTP POST method. With GET,
  the query is a query string parameter ("query"). With POST, it is sent either
  as `application/x-www-form-urlencoded` form fields, or as the unencoded body of
  a POST request with content type `application/sparql-query`. Optional
  `default-graph-uri` and `named-graph-uri` parameters select the RDF dataset
  the query is run against.
- The format of the query results is selected with the `Accept` header, and
  defaults to the SPARQL 1.1 Query Results JSON Format
  (`application/sparql-results+json`) for SELECT and ASK queries, and to N-Triples
  (`application/n-triples`) for CONSTRUCT and DESCRIBE queries.

Supported response formats:

- SELECT and ASK: `application/sparql-results+json` (default), `application/sparql-results+xml`,
  `text/tab-separated-values`.
- CONSTRUCT and DESCRIBE: `application/n-triples` (default), `text/turtle`,
  `application/rdf+xml`, `application/ld+json`.

A query that is not legal gives a `400 Bad Request` response, a query that fails
during its execution gives a `500 Internal Server Error`.
"""

import json

import rdflib
from fastapi import FastAPI, Query, Request
from fastapi.responses import Response
from rdflib import BNode, Graph, Literal, URIRef
from rdflib.query import Result
from rdflib.term import Variable

from . import main
from .sparql_engine import SparqlError, sparql

app = FastAPI(
    title="bikidata SPARQL endpoint",
    summary="SPARQL 1.1 queries over a bikidata RDF database",
    description=(
        "SPARQL 1.1 query endpoint for bikidata RDF databases, "
        "https://github.com/ISE-FIZKarlsruhe/bikidata . The query is sent with the "
        "HTTP GET or POST method and the format of the query results is selected "
        "with the Accept header, as defined in the W3C SPARQL 1.1 Protocol "
        "(https://www.w3.org/TR/sparql11-protocol/)."
    ),
)

RESULTS_JSON = "application/sparql-results+json"
RESULTS_XML = "application/sparql-results+xml"
TSV = "text/tab-separated-values"
N_TRIPLES = "application/n-triples"
TURTLE = "text/turtle"
RDF_XML = "application/rdf+xml"
JSONLD = "application/ld+json"
N_QUADS = "application/n-quads"
RDF_FORMATS = {
    N_TRIPLES: "nt",
    TURTLE: "turtle",
    RDF_XML: "xml",
    JSONLD: "json-ld",
    N_QUADS: "nquads",
}
RDF_MEDIA_TYPES = list(RDF_FORMATS)

SELECT_MEDIA_TYPES = [RESULTS_JSON, RESULTS_XML, TSV]
ASK_MEDIA_TYPES = [RESULTS_JSON, RESULTS_XML]
DEFAULT_RESULTS_MEDIA_TYPE = RESULTS_JSON
DEFAULT_RDF_MEDIA_TYPE = N_TRIPLES

NUMERIC_TYPES = (
    "integer",
    "int",
    "long",
    "short",
    "byte",
    "nonNegativeInteger",
    "positiveInteger",
    "negativeInteger",
    "nonPositiveInteger",
    "unsignedLong",
    "unsignedInt",
    "unsignedShort",
    "unsignedByte",
    "decimal",
    "float",
    "double",
)
XSD = "http://www.w3.org/2001/XMLSchema#"


def _media_type_from_accept(accept: str | None, supported: list[str], default: str) -> str:
    """The best media type of `supported` accepted by the client, defaulting to `default`.

    The media types are considered in the order of the Accept header, and quality
    values are respected.
    """
    chosen = default
    best_quality = 0.0
    for part in (accept or "").split(","):
        tokens = [t.strip() for t in part.split(";") if t.strip()]
        if not tokens:
            continue
        media_type = tokens[0].lower()
        quality = 1.0
        for token in tokens[1:]:
            if token.lower().startswith("q="):
                try:
                    quality = float(token.split("=", 1)[1])
                except ValueError:
                    quality = 1.0
        if media_type not in supported or quality <= 0:
            continue
        if quality > best_quality:
            best_quality = quality
            chosen = media_type
    return chosen


def _error_response(message: str, status_code: int = 400) -> Response:
    return Response(
        content=json.dumps({"error": {"type": "sparql-error", "message": message}}).encode("utf-8"),
        status_code=status_code,
        media_type="application/json",
    )


def _tsv_escape(s: str) -> str:
    "Escape a literal value for the TSV format, see https://www.w3.org/TR/sparql11-results-csv-tsv/"
    return (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _is_number(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False


def _tsv_term(term: dict | None) -> str:
    "Serialize a term of a SELECT result binding in the TSV format"
    if term is None:
        return ""
    if term.get("type") == "uri":
        return f"<{term.get('value') or ''}>"
    if term.get("type") == "bnode":
        return f"_:{term.get('value') or ''}"
    value = str(term.get("value") or "")
    if term.get("xml:lang"):
        return f'"{_tsv_escape(value)}"@{term["xml:lang"]}'
    datatype = term.get("datatype")
    if datatype and str(datatype).startswith(XSD) and str(datatype)[len(XSD) :] in NUMERIC_TYPES:
        if _is_number(value):
            # The abbreviated forms for numbers should be used
            return value
    if datatype:
        return f'"{_tsv_escape(value)}^^<{datatype}>'
    return f'"{_tsv_escape(value)}"'


def _tsv(result: dict) -> bytes:
    "Serialize the results of a SELECT query in the TSV format"
    variables = result["head"]["vars"]
    lines = ["\t".join("?" + v for v in variables)]
    for binding in result["results"]["bindings"]:
        cells = []
        for var in variables:
            term = binding.get(var)
            cells.append(_tsv_term(term) if term is not None else "")
        lines.append("\t".join(cells))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _json_term_to_rdflib(term: dict):
    "Convert a term of the SPARQL 1.1 JSON results format into an rdflib term"
    if term["type"] == "uri":
        return URIRef(term["value"])
    if term["type"] == "bnode":
        return BNode(term["value"])
    if term["type"] == "literal":
        datatype = URIRef(term["datatype"]) if term.get("datatype") else None
        return Literal(term["value"], datatype=datatype, lang=term.get("xml:lang"))
    return None


def _ask_response(request: Request, result: dict) -> Response:
    media_type = _media_type_from_accept(
        request.headers.get("accept"), ASK_MEDIA_TYPES, DEFAULT_RESULTS_MEDIA_TYPE
    )
    outcome = Result("ASK")
    outcome.askAnswer = bool(result["boolean"])
    if media_type == RESULTS_XML or media_type == "application/sparql-results+xml":
        return Response(
            content=outcome.serialize(destination=None, format="xml"), media_type=RESULTS_XML
        )
    return Response(
        content=outcome.serialize(destination=None, format="json"), media_type=RESULTS_JSON
    )


def _select_response(request: Request, result: dict) -> Response:
    media_type = _media_type_from_accept(
        request.headers.get("accept"), SELECT_MEDIA_TYPES, DEFAULT_RESULTS_MEDIA_TYPE
    )
    if media_type == TSV:
        return Response(content=_tsv(result), media_type=TSV)
    outcome = Result("SELECT")
    outcome.vars = [Variable(v) for v in result["head"]["vars"]]
    outcome.bindings = [
        {Variable(k): _json_term_to_rdflib(v) for k, v in binding.items()}
        for binding in result["results"]["bindings"]
    ]
    if media_type == RESULTS_XML or media_type == "application/sparql-results+xml":
        return Response(
            content=outcome.serialize(destination=None, format="xml"), media_type=RESULTS_XML
        )
    return Response(
        content=outcome.serialize(destination=None, format="json"), media_type=RESULTS_JSON
    )


def _construct_response(request: Request, triples: list) -> Response:
    media_type = _media_type_from_accept(
        request.headers.get("accept"), RDF_MEDIA_TYPES, DEFAULT_RDF_MEDIA_TYPE
    )
    lines = "".join(f"{s} {p} {o} .\n" for s, p, o in triples)
    if media_type == N_TRIPLES or RDF_FORMATS[media_type] == "nt":
        return Response(content=lines.encode("utf-8"), media_type=N_TRIPLES)
    graph = Graph()
    previous_normalize = rdflib.NORMALIZE_LITERALS
    rdflib.NORMALIZE_LITERALS = False
    try:
        graph.parse(data=lines, format="nt")
        content = graph.serialize(destination=None, format=RDF_FORMATS[media_type])
    finally:
        rdflib.NORMALIZE_LITERALS = previous_normalize
    return Response(content=content, media_type=media_type)


def _answer(
    request: Request,
    query: str,
    base: str | None = None,
    default_graph_uri: str | None = None,
    named_graph_uris: list[str] | None = None,
) -> Response:
    try:
        result = sparql(
            query,
            db_path=main.DB_PATH,
            base=base,
            default_graph_uri=default_graph_uri,
            named_graph_uris=named_graph_uris,
        )
    except SparqlError as e:
        return _error_response(str(e), status_code=400)
    except Exception as e:
        return _error_response(f"Error while executing the SPARQL query: {e}", status_code=500)

    if isinstance(result, list):
        # CONSTRUCT and DESCRIBE queries return the (s, p, o) triples of the result
        return _construct_response(request, result)
    if "boolean" in result:
        # ASK queries return the boolean of the query
        return _ask_response(request, result)
    # SELECT queries return the SPARQL 1.1 JSON results format
    return _select_response(request, result)


@app.get("/sparql")
def sparql_endpoint(
    request: Request,
    query: str | None = Query(default=None, description="The SPARQL 1.1 query string"),
    default_graph_uri: str | None = Query(
        default=None, alias="default-graph-uri", description="IRI of the default graph"
    ),
    named_graph_uri: str | None = Query(
        default=None, alias="named-graph-uri", description="IRI of a named graph"
    ),
    base: str | None = Query(default=None, description="Base IRI for relative IRIs"),
) -> Response:
    """Serve a SPARQL 1.1 query, answers with the format selected in the Accept header"""
    if not query or not query.strip():
        return _error_response("Missing query: the SPARQL query is required", status_code=400)
    return _answer(
        request,
        query,
        base=base or None,
        default_graph_uri=default_graph_uri or None,
        named_graph_uris=[named_graph_uri] if named_graph_uri else None,
    )


@app.post("/sparql")
async def sparql_endpoint_post(request: Request) -> Response:
    """Serve a SPARQL 1.1 query, answers with the format selected in the Accept header"""
    content_type = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    query = None
    default_graph_uri = None
    named_graph_uri = None
    base = None

    if "json" in content_type:
        body = await request.json()
        if isinstance(body, dict):
            query = body.get("query")
            default_graph_uri = body.get("defaultGraphUri") or body.get("default_graph_uri")
            named_graph_uri = body.get("namedGraphUri") or body.get("named_graph_uri")
            base = body.get("base")
        else:
            query = body
    elif content_type == "application/sparql-query" or content_type == "":
        query = (await request.body()).decode("utf-8")
        default_graph_uri = (
            request.query_params.get("default-graph-uri") or default_graph_uri
        )
        named_graph_uri = (
            request.query_params.get("named-graph-uri") or named_graph_uri
        )
        base = request.query_params.get("base") or base
    elif (
        content_type == "application/x-www-form-urlencoded"
        or content_type.startswith("application/x-www-form")
        or content_type == "multipart/form-data"
    ):
        form = await request.form()
        query = form.get("query")
        default_graph_uri = form.get("default-graph-uri") or form.get("defaultGraphUri")
        named_graph_uri = form.get("named-graph-uri") or form.get("namedGraphUri")
        base = form.get("base")
    else:
        query = (await request.body()).decode("utf-8")

    if not query or not str(query).strip():
        return _error_response("Missing query: the SPARQL query is required", status_code=400)
    return _answer(
        request,
        str(query),
        base=str(base) if base else None,
        default_graph_uri=str(default_graph_uri) if default_graph_uri else None,
        named_graph_uris=[str(named_graph_uri)] if named_graph_uri else None,
    )


def serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    "Run the bikidata SPARQL endpoint with uvicorn"
    import uvicorn

    uvicorn.run(app, host=host, port=port)
