"""
Full-text search for bikidata, backed by a Tantivy index.

Instead of indexing every literal on its own, the index contains one document
per entity (IRI subject). The document of an entity combines:

    title_text    its labels (rdfs:label, skos:prefLabel, schema:name, ...)
    literal_text  all its other literals
    related_text  the labels of the IRIs it links to (one hop)
    pred_text     its literals keyed by predicate, for "fts <iri>" queries

Blank nodes are not indexed as entities of their own, they are unrolled into
the entity that references them (up to max_bnode_depth levels deep). So a
search for the name of the artist of a painting can find the painting, even
when the production event is a blank node in between.

The index is built from the DuckDB tables in two passes, where DuckDB does the
heavy lifting out-of-core:

    1. collect the best label of every IRI into a temporary table
    2. stream all the edges of every entity, including the unrolled blank
       nodes and the labels of the linked IRIs, sorted by subject, and write
       one Tantivy document per subject
"""

import json, os, re, shutil, time
import duckdb
import pandas as pd
import tantivy
import xxhash

from . import main
from .main import log
from .sparql_engine import nt_unescape

PRIMARY_LABEL_IRIS = {
    "<http://www.w3.org/2000/01/rdf-schema#label>",
    "<http://www.w3.org/2004/02/skos/core#prefLabel>",
    "<https://schema.org/name>",
    "<http://schema.org/name>",
    "<http://purl.org/dc/terms/title>",
    "<http://purl.org/dc/elements/1.1/title>",
    "<http://xmlns.com/foaf/0.1/name>",
}

TYPE_IRIS = {
    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
    "<http://www.cidoc-crm.org/cidoc-crm/P2_has_type>",
    "<https://schema.org/additionalType>",
}

TEXT_FIELDS = ["title_text", "literal_text", "related_text"]
FIELD_BOOSTS = {"title_text": 3.0, "literal_text": 1.5, "related_text": 0.7}

ANALYZER = "bikidata"
SETTINGS_FILE = "bikidata_fts.json"
MAX_LABEL_LENGTH = 150
MAX_LITERAL_LENGTH = 50_000

# The stemmer names of the DuckDB FTS extension that differ from Tantivy's
STEMMER_ALIASES = {"porter": "english", "none": None, "": None}


class FTSIndexMissing(Exception):
    pass


def fts_index_path(db_path: str | None = None) -> str:
    "The Tantivy index lives next to the DuckDB database, unless BIKIDATA_FTS_PATH is set"
    return os.getenv("BIKIDATA_FTS_PATH") or f"{db_path or main.DB_PATH}.fts"


def humanize_iri(iri: str) -> str:
    """Fallback when a IRI has no label in the graph, turns the local name
    into words, e.g. 'http://example.org/P14_carried_out_by' -> 'P14 carried out by'
    """
    iri = iri.strip("<>")
    local = iri.rstrip("/#").rsplit("/", 1)[-1].rsplit("#", 1)[-1]
    local = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", local)
    return local.replace("_", " ").replace("-", " ").strip() or iri


def is_label_predicate(p: str) -> bool:
    if p in PRIMARY_LABEL_IRIS:
        return True
    lower = p.lower().rstrip(">")
    return lower.endswith(("label", "title", "name")) and "alt" not in lower


def lexical_form(literal: str) -> str:
    "The unescaped lexical form of a N-Triples encoded literal"
    end = literal.rfind('"')
    if end < 1:
        return literal
    return nt_unescape(literal[1:end])


def pred_key(p: str) -> str:
    "The key in the pred_text JSON field for predicate p (N-Triples encoded)"
    return "p" + xxhash.xxh64_hexdigest(p).lower()


def _analyzer(stemmer: str | None) -> tantivy.TextAnalyzer:
    builder = (
        tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.simple())
        .filter(tantivy.Filter.remove_long(40))
        .filter(tantivy.Filter.lowercase())
        .filter(tantivy.Filter.ascii_fold())
    )
    if stemmer:
        builder = builder.filter(tantivy.Filter.stemmer(stemmer))
    return builder.build()


def _schema() -> tantivy.Schema:
    builder = tantivy.SchemaBuilder()
    builder.add_text_field("uri", stored=True, tokenizer_name="raw")
    for field in TEXT_FIELDS:
        builder.add_text_field(field, stored=False, tokenizer_name=ANALYZER)
    builder.add_json_field("pred_text", stored=False, tokenizer_name=ANALYZER)
    return builder.build()


def build_fts(
    db_path: str | None = None,
    index_path: str | None = None,
    stemmer: str = "porter",
    max_bnode_depth: int = 3,
    max_vals_per_pred: int = 100,
    heap_size: int = 256_000_000,
    db_connection=None,
) -> dict:
    """(Re)build the full-text index for the database at db_path.
    Pass db_connection to use an already open connection to the database.
    """
    start_time = time.time()
    index_path = index_path or fts_index_path(db_path)
    stemmer = STEMMER_ALIASES.get(stemmer, stemmer)

    if os.path.isdir(index_path):
        if os.listdir(index_path) and not os.path.exists(
            os.path.join(index_path, SETTINGS_FILE)
        ):
            raise ValueError(
                f"{index_path} exists and is not a bikidata full-text index, not overwriting it"
            )
        shutil.rmtree(index_path)
    os.makedirs(index_path)

    index = tantivy.Index(_schema(), path=index_path)
    index.register_tokenizer(ANALYZER, _analyzer(stemmer))

    con = db_connection or duckdb.connect(db_path or main.DB_PATH, read_only=True)
    cursor = con.cursor()
    try:
        count = _index_entities(
            cursor, index, max_bnode_depth, max_vals_per_pred, heap_size
        )
    finally:
        cursor.close()
        if db_connection is None:
            con.close()

    # Written last, it marks the index as complete
    with open(os.path.join(index_path, SETTINGS_FILE), "w") as f:
        json.dump({"stemmer": stemmer, "built": time.time()}, f)

    duration = int(time.time() - start_time)
    log.debug(f"Full-text index with {count} entities built in {duration}s")
    return {"duration": duration, "count": count}


def _index_entities(cursor, index, max_bnode_depth, max_vals_per_pred, heap_size):
    # Pass 1: the best label of each IRI, preferring @en, then untagged literals
    predicates = cursor.execute(
        "select I.value, P.p from (select distinct p from triples) P join iris I on I.hash = P.p"
    ).fetchall()
    label_preds = {h for v, h in predicates if is_label_predicate(v)}
    type_preds = {h for v, h in predicates if v in TYPE_IRIS}

    cursor.execute(
        "create or replace temp table fts_labels (hash ubigint primary key, label varchar)"
    )
    if label_preds:
        hashes = ", ".join(f"{h}::ubigint" for h in label_preds)
        cursor.execute(
            f"""
            insert into fts_labels
            select T.s, arg_min(L.value, case when L.value like '%"@en' then 0
                                              when L.value like '%"' then 1
                                              else 2 end)
            from triples T join literals L on L.hash = T.o
            where T.p in ({hashes}) and length(L.value) < {MAX_LABEL_LENGTH + 2}
            group by T.s
            """
        )

    # Pass 2: the edges of each IRI subject, with the edges of the blank nodes
    # it references, sorted by subject.
    cursor.execute(
        f"""
        with recursive
        bnodes as (select hash from iris where starts_with(value, '_:')),
        subjects as (select hash from iris where starts_with(value, '<')),
        reach(root, node, depth) as (
            select T.s, T.o, 1
            from triples T join subjects S on S.hash = T.s join bnodes B on B.hash = T.o
          union
            select R.root, T.o, R.depth + 1
            from reach R join triples T on T.s = R.node join bnodes B on B.hash = T.o
            where R.depth < {int(max_bnode_depth)}
        ),
        edges(s, p, o) as (
            select T.s, T.p, T.o from triples T join subjects S on S.hash = T.s
          union
            select R.root, T.p, T.o
            from (select distinct root, node from reach) R join triples T on T.s = R.node
        )
        select E.s, SI.value, E.p, L.value, OI.value, LB.label
        from edges E
        join iris SI on SI.hash = E.s
        left join literals L on L.hash = E.o
        left join iris OI on OI.hash = E.o
        left join fts_labels LB on LB.hash = E.o
        order by E.s
        """
    )

    writer = index.writer(heap_size=heap_size)
    count = 0
    current, subject, edges = None, None, []
    while rows := cursor.fetchmany(10_000):
        for s, s_value, p, literal, iri, label in rows:
            if s != current:
                if current is not None:
                    writer.add_document(
                        _entity_doc(
                            subject, edges, label_preds, type_preds, max_vals_per_pred
                        )
                    )
                    count += 1
                current, subject, edges = s, s_value, []
            edges.append((p, literal, iri, label))
    if current is not None:
        writer.add_document(
            _entity_doc(subject, edges, label_preds, type_preds, max_vals_per_pred)
        )
        count += 1
    writer.commit()
    writer.wait_merging_threads()
    return count


def _entity_doc(subject, edges, label_preds, type_preds, max_vals_per_pred):
    titles, literals, related = [], [], []
    by_pred = {}
    pred_counts = {}

    for p, literal, iri, label in edges:
        # Super-node protection, cap the values indexed per predicate
        pred_counts[p] = pred_counts.get(p, 0) + 1
        if pred_counts[p] > max_vals_per_pred:
            continue

        if literal is not None:
            val = lexical_form(literal).strip()
            if not val or len(val) > MAX_LITERAL_LENGTH:
                continue
            if p in label_preds:
                titles.append(val)
            else:
                literals.append(val)
            by_pred.setdefault(f"p{p:016x}", []).append(val)
        elif iri is not None and iri.startswith("<") and p not in type_preds:
            related.append(lexical_form(label) if label else humanize_iri(iri))

    doc = tantivy.Document()
    doc.add_text("uri", subject)
    # Entities without a label can still be found by the local name of their IRI
    for t in titles or [humanize_iri(subject)]:
        doc.add_text("title_text", t)
    for lit in literals:
        doc.add_text("literal_text", lit)
    for rel in related:
        doc.add_text("related_text", rel)
    if by_pred:
        doc.add_json("pred_text", by_pred)
    return doc


_INDEXES = {}


def open_index(index_path: str) -> tantivy.Index:
    settings_path = os.path.join(index_path, SETTINGS_FILE)
    try:
        mtime = os.path.getmtime(settings_path)
    except OSError:
        raise FTSIndexMissing(
            f"No full-text index found at {index_path}, build it with bikidata.build_fts()"
        )
    cached = _INDEXES.get(index_path)
    if cached and cached[0] == mtime:
        return cached[1]
    with open(settings_path) as f:
        settings = json.load(f)
    index = tantivy.Index.open(index_path)
    index.register_tokenizer(ANALYZER, _analyzer(settings.get("stemmer")))
    _INDEXES[index_path] = (mtime, index)
    return index


def parse_fts_query(index: tantivy.Index, q: str, prop: str | None = None):
    """Parse q in the Tantivy query language, all the terms have to match by default.
    When prop is given, only the literals of that predicate are searched."""
    if prop:
        q = f"pred_text.{pred_key(prop)}:({q})"
    kwargs = dict(
        default_field_names=TEXT_FIELDS,
        field_boosts=FIELD_BOOSTS,
        conjunction_by_default=True,
    )
    try:
        return index.parse_query(q, **kwargs)
    except ValueError as e:
        log.debug(f"Could not parse full-text query [{q}], parsing leniently: {e}")
        query, _ = index.parse_query_lenient(q, **kwargs)
        return query


def fts_search(
    q: str,
    prop: str | None = None,
    size: int | None = None,
    db_path: str | None = None,
    index_path: str | None = None,
) -> list[tuple[str, float]]:
    """Search the full-text index, returns a list of (entity, score) tuples,
    best matches first. When size is None, all the matches are returned."""
    if not q or not q.strip():
        return []
    index = open_index(index_path or fts_index_path(db_path))
    searcher = index.searcher()
    query = parse_fts_query(index, q, prop)
    if size is None:
        size = searcher.search(query, limit=1, count=True).count
        if size == 0:
            return []
    return [
        (searcher.doc(address)["uri"][0], score)
        for score, address in searcher.search(query, limit=size).hits
    ]


def fts_hits_frame(hits: list[tuple[str, float]]) -> pd.DataFrame:
    "The hits as a (s, score) DataFrame, with s hashed the way the triples table is"
    return pd.DataFrame(
        {
            "s": pd.Series([xxhash.xxh64_intdigest(u) for u, _ in hits], dtype="uint64"),
            "score": pd.Series([score for _, score in hits], dtype="float64"),
        }
    )
