"""
SPARQL 1.1 query support for bikidata.

A SPARQL query is parsed with rdflib into the SPARQL algebra, and the algebra is
then compiled into a single DuckDB SQL statement that runs directly over the
bikidata tables:

    triples(s, p, o, g)   -- xxhash64 hashes of the N-Triples encoded terms
    iris(hash, value)     -- IRIs, blank nodes and graph names
    literals(hash, value) -- literals, in their N-Triples encoding

Within the generated SQL every solution variable is a VARCHAR column holding
the N-Triples encoding of the bound RDF term (or NULL when unbound). Basic
graph patterns join the triples table on the hashes, and only translate the
hashes of the output variables to terms. SPARQL operators and functions are
implemented as DuckDB macros (prefixed with sp_) that work on these encoded
terms. Expression errors are represented as NULL, which conveniently gives
SPARQL's three-valued logic for && and || from SQL's AND and OR.

Every algebra operator becomes one CTE in the final query, to avoid the SQL
text growing exponentially when sub-patterns are referenced more than once.
For the same reason, non-trivial sub-expressions are computed in their own
projection "layer", so that macros only ever receive column references or
constants as arguments.
"""

import datetime, hashlib, re
import duckdb
import rdflib
import xxhash
from rdflib import BNode, Literal, URIRef, Variable
from rdflib.paths import (
    AlternativePath,
    InvPath,
    MulPath,
    NegatedPath,
    Path,
    SequencePath,
)
from rdflib.plugins.sparql.algebra import translateGroupGraphPattern, translateQuery
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.parserutils import CompValue

from . import main

XSD = "http://www.w3.org/2001/XMLSchema#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

INTEGER_TYPES = [
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
]
NUMERIC_TYPES = [XSD + t for t in INTEGER_TYPES] + [
    XSD + "decimal",
    XSD + "float",
    XSD + "double",
]
NUMERIC_RANKS = [0] * len(INTEGER_TYPES) + [1, 2, 3]

DATETIME_RE = r"^(-?[0-9]{4,})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2}(?:\.[0-9]+)?)(Z|[+-][0-9]{2}:[0-9]{2})?$"
INTEGER_RE = r"\s*[+-]?[0-9]+\s*"
DECIMAL_RE = r"\s*[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)\s*"
DOUBLE_RE = r"\s*([+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?|[+-]?INF|NaN)\s*"


class SparqlError(Exception):
    pass


def H(v: str) -> int:
    "The integer value of the hash used for terms in the bikidata tables"
    return xxhash.xxh64_intdigest(v.encode("utf8"))


DEFAULT_GRAPH = H("")


def sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def nt_escape(s: str) -> str:
    return (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


_UNESCAPES = {
    "t": "\t",
    "b": "\b",
    "n": "\n",
    "r": "\r",
    "f": "\f",
    '"': '"',
    "'": "'",
    "\\": "\\",
}


def nt_unescape(s: str) -> str:
    if "\\" not in s:
        return s

    def repl(m):
        c = m.group(1)
        if c[0] in "uU":
            return chr(int(c[1:], 16))
        return _UNESCAPES[c]

    return re.sub(r'\\(u[0-9a-fA-F]{4}|U[0-9a-fA-F]{8}|[tbnrf"\'\\])', repl, s)


def term_to_nt(term) -> str:
    "Encode an rdflib term the way it is stored in the bikidata tables"
    if isinstance(term, URIRef):
        return f"<{term}>"
    if isinstance(term, BNode):
        return f"_:{term}"
    if isinstance(term, Literal):
        lex = nt_escape(str(term))
        if term.language:
            # Language tags are case insensitive, use lowercase
            return f'"{lex}"@{term.language.lower()}'
        if term.datatype and str(term.datatype) != XSD + "string":
            return f'"{lex}"^^<{term.datatype}>'
        return f'"{lex}"'
    raise SparqlError(f"Can not encode term {term!r}")


def nt_to_json(t: str) -> dict:
    "Convert an N-Triples encoded term into the SPARQL 1.1 JSON results format"
    if t.startswith("<"):
        return {"type": "uri", "value": t[1:-1]}
    if t.startswith("_:"):
        return {"type": "bnode", "value": t[2:]}
    end = t.rfind('"')
    result = {"type": "literal", "value": nt_unescape(t[1:end])}
    suffix = t[end + 1 :]
    if suffix.startswith("@"):
        result["xml:lang"] = suffix[1:]
    elif suffix.startswith("^^<"):
        result["datatype"] = suffix[3:-1]
    return result


MACROS = r"""
sp_islit(t) := starts_with(t, '"');
sp_isiri(t) := starts_with(t, '<');
sp_isbnode(t) := starts_with(t, '_:');
sp_lq(t) := length(t) - strpos(reverse(t), '"') + 1;
sp_suffix(t) := substr(t, sp_lq(t) + 1);
sp_rawlex(t) := substr(t, 2, sp_lq(t) - 2);
sp_unesc(s) := CASE WHEN contains(s, '\') THEN replace(replace(replace(replace(replace(replace(replace(replace(replace(s, '\\', chr(1)), '\"', '"'), '\n', chr(10)), '\r', chr(13)), '\t', chr(9)), '\b', chr(8)), '\f', chr(12)), '\''', ''''), chr(1), '\') ELSE s END;
sp_esc(s) := replace(replace(replace(replace(s, '\', '\\'), '"', '\"'), chr(10), '\n'), chr(13), '\r');
sp_lex(t) := CASE WHEN sp_islit(t) THEN sp_unesc(sp_rawlex(t)) END;
sp_lang(t) := CASE WHEN sp_islit(t) THEN CASE WHEN starts_with(sp_suffix(t), '@') THEN substr(sp_suffix(t), 2) ELSE '' END END;
sp_dt(t) := CASE WHEN sp_islit(t) THEN CASE WHEN starts_with(sp_suffix(t), '^^<') THEN substr(sp_suffix(t), 4, length(sp_suffix(t)) - 4) WHEN starts_with(sp_suffix(t), '@') THEN '{RDF}langString' ELSE '{XSD}string' END END;
sp_isstr(t) := sp_islit(t) AND (ends_with(t, '"') OR ends_with(t, '"^^<{XSD}string>'));
sp_haslang(t) := sp_islit(t) AND NOT ends_with(t, '"') AND NOT ends_with(t, '>');
sp_isstrlike(t) := sp_isstr(t) OR sp_haslang(t);
sp_compat(a, b) := sp_isstrlike(a) AND sp_isstrlike(b) AND (NOT sp_haslang(b) OR sp_lang(a) = sp_lang(b));
sp_samekind(t, s) := '"' || sp_esc(s) || '"' || sp_suffix(t);
sp_isbool(t) := sp_islit(t) AND ends_with(t, '"^^<{XSD}boolean>');
sp_isdtm(t) := sp_islit(t) AND ends_with(t, '"^^<{XSD}dateTime>');
sp_bool(t) := CASE WHEN sp_isbool(t) THEN [TRUE, TRUE, FALSE, FALSE][list_position(['true', '1', 'false', '0'], sp_lex(t))] END;
sp_isdate(t) := sp_islit(t) AND ends_with(t, '"^^<{XSD}date>');
sp_epoch(t) := CASE WHEN sp_isdtm(t) THEN epoch(TRY_CAST(sp_lex(t) AS TIMESTAMPTZ)) WHEN sp_isdate(t) THEN epoch(TRY_CAST(regexp_replace(sp_lex(t), '^(-?[0-9]{4,}-[0-9]{2}-[0-9]{2})', '\1T00:00:00') AS TIMESTAMPTZ)) END;
sp_hastz(t) := regexp_matches(sp_lex(t), '(Z|[+-][0-9]{2}:[0-9]{2})$');
sp_tcmp(a, b) := CASE WHEN (sp_isdtm(a) AND sp_isdtm(b)) OR (sp_isdate(a) AND sp_isdate(b)) THEN CASE WHEN sp_hastz(a) = sp_hastz(b) OR abs(sp_epoch(a) - sp_epoch(b)) > 50400 THEN sign(sp_epoch(a) - sp_epoch(b)) END END;
sp_rank(t) := CASE WHEN sp_islit(t) AND ends_with(t, '>') THEN {RANKS}[list_position({NUMTYPES}, sp_dt(t))] END;
sp_numlex(s, r) := CASE r WHEN 0 THEN CASE WHEN regexp_full_match(s, '{INTEGER_RE}') THEN TRY_CAST(trim(s) AS DOUBLE) END WHEN 1 THEN CASE WHEN regexp_full_match(s, '{DECIMAL_RE}') THEN TRY_CAST(trim(s) AS DOUBLE) END WHEN 2 THEN CASE WHEN regexp_full_match(s, '{DOUBLE_RE}') THEN TRY_CAST(trim(s) AS DOUBLE) END WHEN 3 THEN CASE WHEN regexp_full_match(s, '{DOUBLE_RE}') THEN TRY_CAST(trim(s) AS DOUBLE) END END;
sp_num(t) := sp_numlex(sp_lex(t), sp_rank(t));
sp_nrank(t) := CASE WHEN sp_num(t) IS NOT NULL THEN sp_rank(t) END;
sp_fmtint(v) := CAST(TRY_CAST(v AS HUGEINT) AS VARCHAR);
sp_fmtdec(v) := CASE WHEN isinf(v) OR isnan(v) THEN NULL WHEN v = trunc(v) AND abs(v) < 1e18 THEN CAST(CAST(v AS HUGEINT) AS VARCHAR) || '.0' ELSE CAST(round(v, CAST(greatest(0, 14 - floor(log10(abs(v)))) AS INTEGER)) AS VARCHAR) END;
sp_fmtdbl(v) := CASE WHEN isnan(v) THEN 'NaN' WHEN isinf(v) THEN CASE WHEN v > 0 THEN 'INF' ELSE '-INF' END ELSE CAST(v AS VARCHAR) END;
sp_fmtflt(v) := CASE WHEN isnan(v) THEN 'NaN' WHEN isinf(v) THEN CASE WHEN v > 0 THEN 'INF' ELSE '-INF' END ELSE CAST(CAST(v AS FLOAT) AS VARCHAR) END;
sp_mknum(v, r) := CASE r WHEN 0 THEN '"' || sp_fmtint(v) || '"^^<{XSD}integer>' WHEN 1 THEN '"' || sp_fmtdec(v) || '"^^<{XSD}decimal>' WHEN 2 THEN '"' || sp_fmtflt(v) || '"^^<{XSD}float>' WHEN 3 THEN '"' || sp_fmtdbl(v) || '"^^<{XSD}double>' END;
sp_tb(b) := CASE WHEN b THEN '"true"^^<{XSD}boolean>' WHEN NOT b THEN '"false"^^<{XSD}boolean>' END;
sp_known(t) := sp_isstrlike(t) OR sp_num(t) IS NOT NULL OR sp_bool(t) IS NOT NULL OR sp_epoch(t) IS NOT NULL;
sp_eq(a, b) := CASE WHEN a IS NULL OR b IS NULL THEN NULL WHEN sp_num(a) IS NOT NULL AND sp_num(b) IS NOT NULL THEN sp_num(a) = sp_num(b) WHEN a = b THEN TRUE WHEN sp_isstr(a) AND sp_isstr(b) THEN sp_lex(a) = sp_lex(b) WHEN sp_isbool(a) AND sp_isbool(b) THEN sp_bool(a) = sp_bool(b) WHEN (sp_isdtm(a) AND sp_isdtm(b)) OR (sp_isdate(a) AND sp_isdate(b)) THEN sp_tcmp(a, b) = 0 WHEN sp_haslang(a) AND sp_haslang(b) THEN sp_lex(a) = sp_lex(b) AND lower(sp_lang(a)) = lower(sp_lang(b)) WHEN sp_islit(a) AND sp_islit(b) THEN CASE WHEN sp_known(a) AND sp_known(b) THEN FALSE END ELSE FALSE END;
sp_lt(a, b) := CASE WHEN sp_num(a) IS NOT NULL AND sp_num(b) IS NOT NULL THEN sp_num(a) < sp_num(b) WHEN sp_isstr(a) AND sp_isstr(b) THEN sp_lex(a) < sp_lex(b) WHEN sp_isbool(a) AND sp_isbool(b) THEN sp_bool(a) < sp_bool(b) WHEN (sp_isdtm(a) AND sp_isdtm(b)) OR (sp_isdate(a) AND sp_isdate(b)) THEN sp_tcmp(a, b) < 0 END;
sp_ebv(t) := CASE WHEN sp_isbool(t) THEN coalesce(sp_bool(t), FALSE) WHEN sp_isstr(t) THEN length(sp_rawlex(t)) > 0 WHEN sp_rank(t) IS NOT NULL THEN coalesce(sp_num(t) <> 0 AND NOT isnan(sp_num(t)), FALSE) END;
sp_okey(t) := struct_pack(k1 := CASE WHEN t IS NULL THEN 0 WHEN sp_isbnode(t) THEN 1 WHEN sp_isiri(t) THEN 2 ELSE 3 END, k2 := sp_num(t), k3 := sp_epoch(t), k4 := CASE WHEN sp_isiri(t) THEN substr(t, 2, length(t) - 2) ELSE coalesce(sp_lex(t), t) END, k5 := t);
sp_str(t) := CASE WHEN sp_isiri(t) THEN '"' || sp_esc(substr(t, 2, length(t) - 2)) || '"' WHEN sp_islit(t) THEN '"' || sp_rawlex(t) || '"' END;
sp_fmtplain(v, r) := CASE WHEN r = 0 THEN sp_fmtint(v) WHEN isnan(v) THEN 'NaN' WHEN isinf(v) THEN CASE WHEN v > 0 THEN 'INF' ELSE '-INF' END WHEN v = trunc(v) AND abs(v) < 1e15 THEN CAST(CAST(v AS HUGEINT) AS VARCHAR) ELSE CAST(v AS VARCHAR) END;
sp_strcast(t) := CASE WHEN sp_isbool(t) THEN CASE WHEN sp_bool(t) THEN '"true"' WHEN NOT sp_bool(t) THEN '"false"' END WHEN sp_num(t) IS NOT NULL THEN '"' || sp_fmtplain(sp_num(t), sp_rank(t)) || '"' ELSE sp_str(t) END;
sp_round(v) := floor(v + 0.5);
sp_substr2(s, st) := substr(s, CAST(greatest(sp_round(st), 1) AS BIGINT));
sp_substr3(s, st, ln) := CASE WHEN isnan(st) OR isnan(ln) THEN '' ELSE substr(s, CAST(greatest(sp_round(st), 1) AS BIGINT), CAST(greatest(0, least(sp_round(st) + sp_round(ln), 1e15) - greatest(sp_round(st), 1)) AS BIGINT)) END;
sp_dtpart(t, i) := CASE WHEN sp_isdtm(t) THEN nullif(regexp_extract(sp_lex(t), '{DATETIME_RE}', i), '') END;
sp_tzdur(tz) := CASE WHEN tz = 'Z' THEN 'PT0S' WHEN tz IS NOT NULL THEN CASE WHEN starts_with(tz, '-') THEN '-' ELSE '' END || 'PT' || CASE WHEN CAST(substr(tz, 2, 2) AS INTEGER) > 0 OR CAST(substr(tz, 5, 2) AS INTEGER) = 0 THEN CAST(CAST(substr(tz, 2, 2) AS INTEGER) AS VARCHAR) || 'H' ELSE '' END || CASE WHEN CAST(substr(tz, 5, 2) AS INTEGER) > 0 THEN CAST(CAST(substr(tz, 5, 2) AS INTEGER) AS VARCHAR) || 'M' ELSE '' END END;
sp_xpathrepl(r) := regexp_replace(r, '\$([0-9])', '\\\1', 'g');
"""


def _macro_statements():
    text = MACROS
    for k, v in {
        "{XSD}": XSD,
        "{RDF}": RDF,
        "{RANKS}": str(NUMERIC_RANKS),
        "{NUMTYPES}": "[" + ", ".join(sql_str(t) for t in NUMERIC_TYPES) + "]",
        "{INTEGER_RE}": INTEGER_RE,
        "{DECIMAL_RE}": DECIMAL_RE,
        "{DOUBLE_RE}": DOUBLE_RE,
        "{DATETIME_RE}": DATETIME_RE,
    }.items():
        text = text.replace(k, v)
    statements = []
    for line in text.strip().split("\n"):
        line = line.strip().rstrip(";")
        if not line:
            continue
        head, body = line.split(" := ", 1)
        statements.append(f"CREATE OR REPLACE TEMP MACRO {head} AS {body}")
    return statements


MACRO_STATEMENTS = _macro_statements()


def _sha(name):
    def f(s: str) -> str:
        return hashlib.new(name, s.encode("utf8")).hexdigest()

    return f


def setup_connection(con, consts: dict):
    "Create the temporary macros and views the generated SQL depends on"
    con.execute("SET TimeZone = 'UTC'")
    for statement in MACRO_STATEMENTS:
        con.execute(statement)
    for name in ("sha384", "sha512"):
        try:
            con.create_function(
                f"sp_{name}", _sha(name), ["VARCHAR"], "VARCHAR"
            )
        except duckdb.Error:
            pass  # Already registered on this connection
    # The term dictionary. Constants from the query are added if they are not in
    # the data, as a zero-length property path can bind a variable to them.
    extra = ""
    if consts:
        values = ", ".join(f"({h}::UBIGINT, {sql_str(v)})" for h, v in consts.items())
        extra = f""" UNION ALL SELECT hash, value FROM (VALUES {values}) AS c(hash, value)
         WHERE hash NOT IN (SELECT hash FROM iris) AND hash NOT IN (SELECT hash FROM literals)"""
    con.execute(
        "CREATE OR REPLACE TEMP VIEW bk_terms AS SELECT hash, value FROM iris "
        f"UNION ALL SELECT hash, value FROM literals{extra}"
    )


class Plan:
    "A compiled algebra operator: a CTE with one column per solution variable"

    def __init__(self, name, vars, nullable=(), extras=()):
        self.name = name
        self.vars = list(dict.fromkeys(vars))
        self.nullable = set(nullable) & set(self.vars)
        self.extras = list(extras)  # Extra columns like __ord carried along


ATOM_RE = re.compile(r"x\.\w+|NULL|-?[0-9.]+|'(?:[^']|'')*'")


class ExprContext:
    """Compiles expressions over the solutions of a plan.
    Sub-expressions that are not simple columns or constants are hoisted into
    their own columns, computed in nested projections over the plan."""

    def __init__(self, compiler, plan, gctx):
        self.compiler = compiler
        self.plan = plan
        self.gctx = gctx
        self.layers = []

    def var(self, name):
        if name in self.plan.vars:
            return f"x.{self.compiler.col(name)}"
        return "NULL"

    def atom(self, sql):
        if ATOM_RE.fullmatch(sql):
            return sql
        name = self.compiler.fresh("_e")
        self.layers.append((name, sql))
        return f"x.{name}"

    def source(self):
        sql = self.plan.name
        for name, expr in self.layers:
            sql = f"(SELECT x.*, {expr} AS {name} FROM {sql} AS x)"
        return f"{sql} AS x"

    # Compiled expressions are tuples of (kind, sql), where kind is "t" for an
    # N-Triples encoded term and "b" for an SQL boolean.
    def term(self, e):
        kind, sql = self.compiler.expr(e, self)
        if kind == "b":
            return f"sp_tb({self.atom(sql)})"
        return sql

    def bool(self, e):
        kind, sql = self.compiler.expr(e, self)
        if kind == "t":
            return f"sp_ebv({self.atom(sql)})"
        return sql

    def aterm(self, e):
        return self.atom(self.term(e))

    def abool(self, e):
        return self.atom(self.bool(e))


def _name(term):
    return str(term)


def _list(value):
    "An argument list of an expression, rdflib uses rdf:nil for empty ones"
    if value is None or value == URIRef(RDF + "nil"):
        return []
    return list(value)


def _get(node, key):
    "The value of a key of an algebra node, None if it is not present"
    # CompValue.get returns the key itself for missing keys
    return node[key] if key in node else None


class Compiler:
    def __init__(self, union_default_graph=False, base=None):
        self.union_default_graph = union_default_graph
        self.base = base
        self.ctes = []
        self.cols = {}
        self.consts = {}
        self.counter = 0
        self.default_graphs = None if union_default_graph else [DEFAULT_GRAPH]
        self.named_graphs = None  # None means all graphs except the default one
        self.now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        # The Project of SELECT * and CONSTRUCT queries: rdflib leaves the
        # variables of a trailing VALUES clause out of it.
        self.star_project = None

    # Naming helpers
    def fresh(self, prefix):
        self.counter += 1
        return f"{prefix}{self.counter}"

    def col(self, var):
        if var not in self.cols:
            self.cols[var] = f"v{len(self.cols)}"
        return self.cols[var]

    def add(self, sql, vars, nullable=(), extras=()):
        name = f"q{len(self.ctes)}"
        self.ctes.append((name, sql))
        return Plan(name, vars, nullable, extras)

    def const(self, term) -> str:
        "SQL for the hash of a constant term, registering it in the dictionary"
        nt = term_to_nt(term)
        h = H(nt)
        self.consts[h] = nt
        return f"{h}::UBIGINT"

    def select_list(self, items):
        return ", ".join(items) if items else "TRUE AS __u"

    # Dataset handling
    @property
    def default_merged(self):
        return self.default_graphs is None or len(self.default_graphs) > 1

    def gfilter(self, gctx, alias):
        kind = gctx[0]
        if kind == "default":
            if self.default_graphs is None:
                return "TRUE"
            if not self.default_graphs:
                return "FALSE"
            return f"{alias}.g IN ({', '.join(str(g) for g in self.default_graphs)})"
        if kind == "named":
            h = H(term_to_nt(gctx[1]))
            if self.named_graphs is not None and h not in self.named_graphs:
                return "FALSE"
            return f"{alias}.g = {h}::UBIGINT"
        if self.named_graphs is None:
            return f"{alias}.g <> {DEFAULT_GRAPH}::UBIGINT"
        if not self.named_graphs:
            return "FALSE"
        return f"{alias}.g IN ({', '.join(str(g) for g in self.named_graphs)})"

    def set_dataset(self, dataset_clause):
        if not dataset_clause:
            return
        default, named = [], []
        for d in dataset_clause:
            if _get(d, "default") is not None:
                default.append(H(term_to_nt(d.default)))
            if _get(d, "named") is not None:
                named.append(H(term_to_nt(d.named)))
        self.default_graphs = default
        self.named_graphs = named

    # Query forms
    def compile(self, query):
        algebra = query.algebra
        if query.prologue.base:
            self.base = query.prologue.base
        self.set_dataset(_get(algebra, "datasetClause"))
        gctx = ("default",)
        if getattr(query, "select_star", False) or algebra.name == "ConstructQuery":
            node = algebra.p
            while node.name in ("Slice", "Distinct", "Reduced", "OrderBy"):
                node = node.p
            if node.name == "Project":
                self.star_project = node
        if algebra.name == "SelectQuery":
            plan = self.pattern(algebra.p, gctx)
            if self.star_project is not None:
                vars = [v for v in plan.vars if not v.startswith("__")]
            else:
                vars = [_name(v) for v in algebra.PV]
            return self.finish(plan, vars), vars
        if algebra.name == "AskQuery":
            plan = self.pattern(algebra.p, gctx)
            return self.with_ctes(f"SELECT EXISTS (SELECT 1 FROM {plan.name})"), []
        if algebra.name == "ConstructQuery":
            plan = self.pattern(algebra.p, gctx)
            vars = sorted(
                {
                    _name(t)
                    for triple in construct_template(algebra)
                    for t in triple
                    if isinstance(t, Variable)
                }
            )
            return self.finish(plan, vars), vars
        if algebra.name == "DescribeQuery":
            return self.describe(algebra, gctx), []
        raise SparqlError(f"Unsupported query form {algebra.name}")

    def with_ctes(self, final):
        if not self.ctes:
            return final
        ctes = ",\n".join(f"{name} AS ({sql})" for name, sql in self.ctes)
        return f"WITH {ctes}\n{final}"

    def finish(self, plan, vars):
        cols = [
            f"{plan.name}.{self.col(v)}" if v in plan.vars else "NULL" for v in vars
        ]
        order = " ORDER BY __ord" if "__ord" in plan.extras else ""
        return self.with_ctes(
            f"SELECT {self.select_list(cols)} FROM {plan.name}{order}"
        )

    def describe(self, algebra, gctx):
        resources = []
        plan = self.pattern(algebra.p, gctx) if _get(algebra, "p") is not None else None
        for t in algebra.PV:
            if isinstance(t, Variable):
                if plan is not None and _name(t) in plan.vars:
                    resources.append(
                        f"SELECT {self.col(_name(t))} AS r FROM {plan.name}"
                    )
            else:
                resources.append(f"SELECT {sql_str(term_to_nt(t))} AS r")
        if not resources:
            return "SELECT NULL, NULL, NULL WHERE FALSE"
        sql = f"""SELECT DISTINCT ds.value, dp.value, dob.value
          FROM ({" UNION ".join(resources)}) AS r
          JOIN bk_terms AS ds ON ds.value = r.r
          JOIN triples AS t ON t.s = ds.hash
          JOIN bk_terms AS dp ON dp.hash = t.p
          JOIN bk_terms AS dob ON dob.hash = t.o
          WHERE {self.gfilter(gctx, "t")}"""
        return self.with_ctes(sql)

    # Graph patterns
    def pattern(self, node, gctx) -> Plan:
        name = node.name
        method = getattr(self, "p_" + name, None)
        if method is None:
            raise SparqlError(f"Unsupported SPARQL feature: {name}")
        return method(node, gctx)

    def unit(self):
        return self.add("SELECT TRUE AS __u", [])

    def p_BGP(self, node, gctx):
        return self.bgp(list(node.triples), gctx)

    def bgp(self, triples, gctx):
        if not triples:
            if gctx[0] == "named":
                # Matches once, if the graph exists
                return self.add(
                    f"SELECT TRUE AS __u FROM triples AS t WHERE {self.gfilter(gctx, 't')} LIMIT 1",
                    [],
                )
            return self.unit()

        froms, where, bound, hash_cols = [], [], {}, []

        def bind(term, colref):
            if isinstance(term, (Variable, BNode)):
                key = _name(term) if isinstance(term, Variable) else f"_:{term}"
                if key in bound:
                    where.append(f"{colref} = {bound[key]}")
                else:
                    bound[key] = colref
            else:
                where.append(f"{colref} = {self.const(term)}")

        for i, (s, p, o) in enumerate(triples):
            alias = f"t{i}"
            if isinstance(p, Path):
                seed_s = None if isinstance(s, (Variable, BNode)) else self.const(s)
                seed_o = None if isinstance(o, (Variable, BNode)) else self.const(o)
                froms.append(f"({self.path(p, gctx, seed_s, seed_o)}) AS {alias}")
                bind(s, f"{alias}.s")
                bind(o, f"{alias}.o")
                hash_cols += [f"{alias}.s", f"{alias}.o"]
                if gctx[0] == "var":
                    bind(Variable(gctx[1]), f"{alias}.g")
            else:
                froms.append(f"triples AS {alias}")
                bind(s, f"{alias}.s")
                bind(p, f"{alias}.p")
                bind(o, f"{alias}.o")
                hash_cols += [f"{alias}.s", f"{alias}.p", f"{alias}.o"]
                where.append(self.gfilter(gctx, alias))
                if gctx[0] == "var":
                    bind(Variable(gctx[1]), f"{alias}.g")

        vars = [k for k in bound if not k.startswith("_:")]
        inner_where = " AND ".join(where) if where else "TRUE"
        if gctx[0] == "default" and self.default_merged:
            # The default graph is a merge of graphs, the same triple can be in
            # more than one of them: remove the duplicates.
            inner_cols = [f"{c} AS h{n}" for n, c in enumerate(hash_cols)]
            inner = f"SELECT DISTINCT {', '.join(inner_cols)} FROM {', '.join(froms)} WHERE {inner_where}"
            rename = {c: f"m.h{n}" for n, c in enumerate(hash_cols)}
            refs = {v: rename[bound[v]] for v in vars}
            from_sql = f"({inner}) AS m"
            where_sql = ""
        else:
            refs = {v: bound[v] for v in vars}
            from_sql = ", ".join(froms)
            where_sql = f" WHERE {inner_where}"
        joins, cols = [], []
        for n, v in enumerate(vars):
            joins.append(f" JOIN bk_terms AS d{n} ON d{n}.hash = {refs[v]}")
            cols.append(f"d{n}.value AS {self.col(v)}")
        sql = f"SELECT {self.select_list(cols)} FROM {from_sql}{''.join(joins)}{where_sql}"
        return self.add(sql, vars)

    # Property paths, compiled to SQL with columns s, o, g
    def edges(self, gctx, cond):
        gcol = "0::UBIGINT" if gctx[0] == "default" else "t.g"
        distinct = "DISTINCT " if gctx[0] == "default" and self.default_merged else ""
        return f"SELECT {distinct}t.s AS s, t.o AS o, {gcol} AS g FROM triples AS t WHERE {cond} AND {self.gfilter(gctx, 't')}"

    def nodes(self, gctx, seed):
        "The zero-length path"
        if seed is not None:
            if gctx[0] == "var":
                return f"SELECT DISTINCT {seed} AS s, {seed} AS o, t.g AS g FROM triples AS t WHERE {self.gfilter(gctx, 't')}"
            g = "0::UBIGINT" if gctx[0] == "default" else self.const(gctx[1])
            return f"SELECT {seed} AS s, {seed} AS o, {g} AS g"
        gcol = "0::UBIGINT" if gctx[0] == "default" else "t.g"
        gf = self.gfilter(gctx, "t")
        return f"""SELECT DISTINCT n AS s, n AS o, g FROM (
            SELECT t.s AS n, {gcol} AS g FROM triples AS t WHERE {gf}
            UNION ALL SELECT t.o AS n, {gcol} AS g FROM triples AS t WHERE {gf})"""

    def path(self, path, gctx, seed_s=None, seed_o=None):
        if isinstance(path, URIRef):
            return self.edges(gctx, f"t.p = {self.const(path)}")
        if isinstance(path, InvPath):
            sub = self.path(path.arg, gctx, seed_o, seed_s)
            return f"SELECT i.o AS s, i.s AS o, i.g AS g FROM ({sub}) AS i"
        if isinstance(path, SequencePath):
            sql = self.path(path.args[0], gctx, seed_s, None)
            for n, arg in enumerate(path.args[1:]):
                last = n == len(path.args) - 2
                nxt = self.path(arg, gctx, None, seed_o if last else None)
                sql = f"SELECT a.s AS s, b.o AS o, a.g AS g FROM ({sql}) AS a JOIN ({nxt}) AS b ON a.o = b.s AND a.g = b.g"
            return sql
        if isinstance(path, AlternativePath):
            return " UNION ALL ".join(
                f"SELECT * FROM ({self.path(arg, gctx, seed_s, seed_o)})"
                for arg in path.args
            )
        if isinstance(path, NegatedPath):
            forward = [a for a in path.args if isinstance(a, URIRef)]
            inverse = [a.arg for a in path.args if isinstance(a, InvPath)]
            parts = []
            if forward or not inverse:
                not_in = ", ".join(self.const(a) for a in forward) or "NULL"
                parts.append(self.edges(gctx, f"t.p NOT IN ({not_in})"))
            if inverse:
                not_in = ", ".join(self.const(a) for a in inverse)
                sub = self.edges(gctx, f"t.p NOT IN ({not_in})")
                parts.append(f"SELECT i.o AS s, i.s AS o, i.g AS g FROM ({sub}) AS i")
            return " UNION ALL ".join(f"SELECT * FROM ({p})" for p in parts)
        if isinstance(path, MulPath):
            base = self.path(path.path, gctx)
            if path.mod == "?":
                return f"SELECT * FROM ({self.nodes(gctx, seed_s or seed_o)}) UNION SELECT * FROM ({base})"
            if seed_s is not None:
                closure = f"""WITH RECURSIVE r(s, o, g) AS (
                    SELECT b.s, b.o, b.g FROM ({base}) AS b WHERE b.s = {seed_s}
                    UNION SELECT r.s, b.o, r.g FROM r JOIN ({base}) AS b ON b.s = r.o AND b.g = r.g)
                    SELECT s, o, g FROM r"""
            elif seed_o is not None:
                closure = f"""WITH RECURSIVE r(s, o, g) AS (
                    SELECT b.s, b.o, b.g FROM ({base}) AS b WHERE b.o = {seed_o}
                    UNION SELECT b.s, r.o, r.g FROM r JOIN ({base}) AS b ON b.o = r.s AND b.g = r.g)
                    SELECT s, o, g FROM r"""
            else:
                closure = f"""WITH RECURSIVE r(s, o, g) AS (
                    SELECT b.s, b.o, b.g FROM ({base}) AS b
                    UNION SELECT r.s, b.o, r.g FROM r JOIN ({base}) AS b ON b.s = r.o AND b.g = r.g)
                    SELECT s, o, g FROM r"""
            if path.mod == "+":
                return closure
            return f"SELECT * FROM ({self.nodes(gctx, seed_s or seed_o)}) UNION SELECT * FROM ({closure})"
        raise SparqlError(f"Unsupported property path {path!r}")

    # Combining plans
    def compat(self, a_alias, a, b_alias, b):
        "Join conditions and output columns for the SPARQL join of a and b"
        conds, cols = [], []
        for v in a.vars:
            c = self.col(v)
            if v in b.vars:
                if v in a.nullable or v in b.nullable:
                    conds.append(
                        f"({a_alias}.{c} IS NULL OR {b_alias}.{c} IS NULL OR {a_alias}.{c} = {b_alias}.{c})"
                    )
                    cols.append(f"COALESCE({a_alias}.{c}, {b_alias}.{c}) AS {c}")
                else:
                    conds.append(f"{a_alias}.{c} = {b_alias}.{c}")
                    cols.append(f"{a_alias}.{c} AS {c}")
            else:
                cols.append(f"{a_alias}.{c} AS {c}")
        for v in b.vars:
            if v not in a.vars:
                cols.append(f"{b_alias}.{self.col(v)} AS {self.col(v)}")
        return conds, cols

    def join(self, a, b, extras_from_a=()):
        conds, cols = self.compat("a", a, "b", b)
        cols += [f"a.{e}" for e in extras_from_a]
        nullable = {
            v
            for v in a.vars + b.vars
            if (v not in a.vars or v in a.nullable)
            and (v not in b.vars or v in b.nullable)
        }
        on = " AND ".join(conds) if conds else "TRUE"
        sql = f"SELECT {self.select_list(cols)} FROM {a.name} AS a JOIN {b.name} AS b ON {on}"
        return self.add(sql, a.vars + b.vars, nullable, extras_from_a)

    def p_Join(self, node, gctx):
        if node.p1.name == "BGP" and node.p2.name == "BGP":
            return self.bgp(list(node.p1.triples) + list(node.p2.triples), gctx)
        a = self.pattern(node.p1, gctx)
        b = self.pattern(node.p2, gctx)
        if self.is_unit(a):
            return b
        if self.is_unit(b):
            return a
        return self.join(a, b)

    def is_unit(self, plan):
        return dict(self.ctes).get(plan.name) == "SELECT TRUE AS __u"

    def p_LeftJoin(self, node, gctx):
        a = self.pattern(node.p1, gctx)
        b = self.pattern(node.p2, gctx)
        expr = _get(node, "expr")
        has_filter = expr is not None and not (
            isinstance(expr, CompValue) and expr.name == "TrueFilter"
        )
        nullable = set(a.nullable) | {v for v in b.vars if v not in a.vars}
        if not has_filter:
            conds, cols = self.compat("a", a, "b", b)
            on = " AND ".join(conds) if conds else "TRUE"
            sql = f"SELECT {self.select_list(cols)} FROM {a.name} AS a LEFT JOIN {b.name} AS b ON {on}"
            return self.add(sql, a.vars + b.vars, nullable)
        # With a filter: join, filter the joined rows, then left join those
        # back onto the left hand side using a row id.
        ra = self.add(
            f"SELECT {self.select_list([f'x.{self.col(v)}' for v in a.vars])}, row_number() OVER () AS __rid FROM {a.name} AS x",
            a.vars,
            a.nullable,
            ["__rid"],
        )
        j = self.join(ra, b, ["__rid"])
        jf = self.filter(j, expr, gctx)
        cols = []
        for v in a.vars + [v for v in b.vars if v not in a.vars]:
            c = self.col(v)
            cols.append(
                f"COALESCE(j.{c}, a.{c}) AS {c}" if v in a.vars else f"j.{c} AS {c}"
            )
        sql = f"SELECT {self.select_list(cols)} FROM {ra.name} AS a LEFT JOIN {jf.name} AS j ON a.__rid = j.__rid"
        return self.add(sql, a.vars + b.vars, nullable)

    def p_Union(self, node, gctx):
        a = self.pattern(node.p1, gctx)
        b = self.pattern(node.p2, gctx)
        vars = a.vars + [v for v in b.vars if v not in a.vars]
        nullable = {
            v
            for v in vars
            if v not in a.vars
            or v not in b.vars
            or v in a.nullable
            or v in b.nullable
        }

        def side(p):
            cols = [
                f"{self.col(v)}" if v in p.vars else f"NULL::VARCHAR AS {self.col(v)}"
                for v in vars
            ]
            return f"SELECT {self.select_list(cols)} FROM {p.name}"

        return self.add(f"{side(a)} UNION ALL {side(b)}", vars, nullable)

    def p_Minus(self, node, gctx):
        a = self.pattern(node.p1, gctx)
        b = self.pattern(node.p2, gctx)
        shared = [v for v in a.vars if v in b.vars and not v.startswith("__graph")]
        if not shared:
            return a
        conds, _ = self.compat("a", a, "b", b)
        overlap = " OR ".join(
            f"(a.{self.col(v)} IS NOT NULL AND b.{self.col(v)} IS NOT NULL)"
            for v in shared
        )
        cols = [f"a.{self.col(v)}" for v in a.vars] + [f"a.{e}" for e in a.extras]
        sql = f"""SELECT {self.select_list(cols)} FROM {a.name} AS a WHERE NOT EXISTS (
            SELECT 1 FROM {b.name} AS b WHERE {' AND '.join(conds)} AND ({overlap}))"""
        return self.add(sql, a.vars, a.nullable, a.extras)

    def p_Filter(self, node, gctx):
        return self.filter(self.pattern(node.p, gctx), node.expr, gctx)

    def filter(self, plan, expr, gctx):
        ctx = ExprContext(self, plan, gctx)
        cond = ctx.bool(expr)
        cols = [f"x.{self.col(v)}" for v in plan.vars] + [
            f"x.{e}" for e in plan.extras
        ]
        sql = f"SELECT {self.select_list(cols)} FROM {ctx.source()} WHERE {cond}"
        return self.add(sql, plan.vars, plan.nullable, plan.extras)

    def p_Extend(self, node, gctx):
        plan = self.pattern(node.p, gctx)
        var = _name(node.var)
        ctx = ExprContext(self, plan, gctx)
        value = ctx.term(node.expr)
        cols = [f"x.{self.col(v)}" for v in plan.vars if v != var]
        cols += [f"x.{e}" for e in plan.extras]
        cols.append(f"{value} AS {self.col(var)}")
        sql = f"SELECT {', '.join(cols)} FROM {ctx.source()}"
        return self.add(
            sql,
            [v for v in plan.vars if v != var] + [var],
            plan.nullable | {var},
            plan.extras,
        )

    def graph_names(self, var):
        "The names of the named graphs, bound to var"
        return self.add(
            f"SELECT DISTINCT d.value AS {self.col(var)} FROM triples AS t "
            f"JOIN bk_terms AS d ON d.hash = t.g WHERE {self.gfilter(('var', var), 't')}",
            [var],
        )

    def p_Graph(self, node, gctx):
        term = node.term
        if not isinstance(term, Variable):
            gctx = ("named", term)
            plan = self.pattern(node.p, gctx)
            if node.p.name == "BGP" and node.p.triples:
                return plan
            # Patterns that do not match triples only match if the graph exists
            return self.join(self.bgp([], gctx), plan)
        # The pattern is evaluated with a hidden variable for the graph name, as
        # the graph variable itself is not in scope inside the pattern.
        g = _name(term)
        hidden = self.fresh("__graph")
        plan = self.pattern(node.p, ("var", hidden))
        if hidden not in plan.vars or hidden in plan.nullable:
            plan = self.join(self.graph_names(hidden), plan)
        hc, gc = self.col(hidden), self.col(g)
        vars = [v for v in plan.vars if v not in (hidden, g)]
        cols = [self.col(v) for v in vars] + plan.extras + [f"{hc} AS {gc}"]
        where = f" WHERE {gc} IS NULL OR {gc} = {hc}" if g in plan.vars else ""
        sql = f"SELECT {', '.join(cols)} FROM {plan.name}{where}"
        return self.add(sql, vars + [g], plan.nullable - {g}, plan.extras)

    def hidden_graph_vars(self, plan):
        return [v for v in plan.vars if v.startswith("__graph")]

    def p_ToMultiSet(self, node, gctx):
        if isinstance(node.p, list):
            # rdflib loses the rows of VALUES without variables
            return self.unit()
        if node.p.name == "values":
            return self.values(node.p.res)
        plan = self.pattern(node.p, gctx)
        if plan.extras:
            cols = [self.col(v) for v in plan.vars]
            plan = self.add(
                f"SELECT {self.select_list(cols)} FROM {plan.name}",
                plan.vars,
                plan.nullable,
            )
        return plan

    def values(self, rows):
        vars = []
        for row in rows:
            for v in row:
                if _name(v) not in vars:
                    vars.append(_name(v))
        if not rows:
            cols = [f"NULL::VARCHAR AS {self.col(v)}" for v in vars]
            return self.add(f"SELECT {self.select_list(cols)} WHERE FALSE", vars)
        if not vars:
            return self.add(f"SELECT TRUE AS __u FROM range({len(rows)})", [])
        tuples, nullable = [], set()
        for row in rows:
            items = []
            byname = {_name(k): val for k, val in row.items()}
            for v in vars:
                val = byname.get(v)
                if val is None or val == "UNDEF":
                    items.append("NULL")
                    nullable.add(v)
                else:
                    items.append(sql_str(term_to_nt(val)))
            tuples.append(f"({', '.join(items)})")
        cols = [f"CAST(c{n} AS VARCHAR) AS {self.col(v)}" for n, v in enumerate(vars)]
        names = ", ".join(f"c{n}" for n in range(len(vars)))
        sql = f"SELECT {', '.join(cols)} FROM (VALUES {', '.join(tuples)}) AS vals({names})"
        return self.add(sql, vars, nullable)

    def p_Project(self, node, gctx):
        plan = self.pattern(node.p, gctx)
        if node is self.star_project:
            vars = [v for v in plan.vars if not v.startswith("__")]
        else:
            vars = [_name(v) for v in node.PV]
        vars += self.hidden_graph_vars(plan)
        cols = [
            f"{self.col(v)}" if v in plan.vars else f"NULL::VARCHAR AS {self.col(v)}"
            for v in vars
        ] + plan.extras
        nullable = {v for v in vars if v not in plan.vars or v in plan.nullable}
        return self.add(
            f"SELECT {self.select_list(cols)} FROM {plan.name}",
            vars,
            nullable,
            plan.extras,
        )

    def p_Distinct(self, node, gctx):
        plan = self.pattern(node.p, gctx)
        cols = [self.col(v) for v in plan.vars]
        if "__ord" in plan.extras:
            group = f" GROUP BY {', '.join(cols)}" if cols else ""
            sql = f"SELECT {', '.join(cols + ['min(__ord) AS __ord'])} FROM {plan.name}{group}"
            return self.add(sql, plan.vars, plan.nullable, ["__ord"])
        sql = f"SELECT DISTINCT {self.select_list(cols)} FROM {plan.name}"
        return self.add(sql, plan.vars, plan.nullable)

    def p_Reduced(self, node, gctx):
        # REDUCED permits, but does not require, the removal of duplicates
        return self.pattern(node.p, gctx)

    def p_Slice(self, node, gctx):
        plan = self.pattern(node.p, gctx)
        order = " ORDER BY __ord" if "__ord" in plan.extras else ""
        limit = f" LIMIT {int(node.length)}" if _get(node, "length") is not None else ""
        offset = f" OFFSET {int(node.start)}" if _get(node, "start") else ""
        cols = [self.col(v) for v in plan.vars] + plan.extras
        hidden = self.hidden_graph_vars(plan)
        if hidden:
            # Inside GRAPH ?g the slice applies to the solutions of each graph
            partition = ", ".join(self.col(v) for v in hidden)
            rn = f"row_number() OVER (PARTITION BY {partition}{order})"
            start = int(_get(node, "start") or 0)
            conds = [f"{rn} > {start}"]
            if _get(node, "length") is not None:
                conds.append(f"{rn} <= {start + int(node.length)}")
            sql = f"SELECT {self.select_list(cols)} FROM {plan.name} QUALIFY {' AND '.join(conds)}"
            return self.add(sql, plan.vars, plan.nullable, plan.extras)
        sql = f"SELECT {self.select_list(cols)} FROM {plan.name}{order}{limit}{offset}"
        return self.add(sql, plan.vars, plan.nullable, plan.extras)

    def p_OrderBy(self, node, gctx):
        plan = self.pattern(node.p, gctx)
        ctx = ExprContext(self, plan, gctx)
        keys = []
        for cond in node.expr:
            if isinstance(cond, CompValue) and cond.name == "OrderCondition":
                desc = cond.order == "DESC"
                cond = cond.expr
            else:
                desc = False
            keys.append(f"sp_okey({ctx.aterm(cond)})" + (" DESC" if desc else ""))
        cols = [f"x.{self.col(v)}" for v in plan.vars]
        cols += [f"x.{e}" for e in plan.extras if e != "__ord"]
        cols.append(f"row_number() OVER (ORDER BY {', '.join(keys)}) AS __ord")
        extras = [e for e in plan.extras if e != "__ord"] + ["__ord"]
        sql = f"SELECT {', '.join(cols)} FROM {ctx.source()}"
        return self.add(sql, plan.vars, plan.nullable, extras)

    def p_Group(self, node, gctx):
        return self.p_AggregateJoin(CompValue("AggregateJoin", A=[], p=node), gctx)

    def p_AggregateJoin(self, node, gctx):
        group = node.p
        plan = self.pattern(group.p, gctx)
        if gctx[0] == "var" and not _get(group, "expr"):
            plan = self.group_per_graph(plan, gctx[1])
        ctx = ExprContext(self, plan, gctx)
        keys, out_cols, out_vars = [], [], []
        for e in _get(group, "expr") or []:
            if isinstance(e, Variable):
                ref = ctx.var(_name(e))
                if ref == "NULL":
                    continue
                keys.append(ref)
                out_cols.append(f"{ref} AS {self.col(_name(e))}")
                out_vars.append(_name(e))
            else:
                keys.append(ctx.aterm(e))
        for v in self.hidden_graph_vars(plan):
            keys.append(ctx.var(v))
            out_cols.append(f"{ctx.var(v)} AS {self.col(v)}")
            out_vars.append(v)
        for agg in node.A:
            out_cols.append(f"{self.aggregate(agg, ctx, plan)} AS {self.col(_name(agg.res))}")
            out_vars.append(_name(agg.res))
        if _get(group, "expr") is not None and not keys:
            keys = ["NULL"]
        group_by = f" GROUP BY {', '.join(keys)}" if keys else ""
        sql = f"SELECT {self.select_list(out_cols)} FROM {ctx.source()}{group_by}"
        return self.add(sql, out_vars, set(out_vars))

    def group_per_graph(self, plan, hidden):
        """Inside GRAPH ?g an aggregate without GROUP BY has one group for every
        graph, also for the graphs without solutions. The solutions are left
        joined onto the graph names, the __m column marks the real solutions."""
        names = self.graph_names(hidden)
        hc = self.col(hidden)
        on = f"p.{hc} = n.{hc}" if hidden in plan.vars else "TRUE"
        cols = [f"n.{hc} AS {hc}"]
        cols += [f"p.{self.col(v)}" for v in plan.vars if v != hidden]
        sql = (
            f"SELECT {', '.join(cols)}, p.__m FROM {names.name} AS n "
            f"LEFT JOIN (SELECT *, 1 AS __m FROM {plan.name}) AS p ON {on}"
        )
        vars = [hidden] + [v for v in plan.vars if v != hidden]
        return self.add(sql, vars, set(vars) - {hidden}, ["__m"])

    def aggregate(self, agg, ctx, plan):
        name = agg.name
        distinct = "DISTINCT " if _get(agg, "distinct") == "DISTINCT" else ""
        integer = f"'\"' || %s || '\"^^<{XSD}integer>'"
        if name == "Aggregate_Count":
            if agg.vars == "*":
                rows = "x.__m" if "__m" in plan.extras else "*"
                if distinct and plan.vars:
                    fields = ", ".join(
                        f"{self.col(v)} := x.{self.col(v)}" for v in plan.vars
                    )
                    row = f"struct_pack({fields})"
                    if rows != "*":
                        row = f"CASE WHEN {rows} IS NOT NULL THEN {row} END"
                    return integer % f"count(DISTINCT {row})"
                if distinct:
                    return integer % f"least(count({rows}), 1)"
                return integer % f"count({rows})"
            return integer % f"count({distinct}{ctx.aterm(agg.vars)})"
        a = ctx.aterm(agg.vars)
        if name == "Aggregate_Sample":
            return f"any_value({a})"
        if name in ("Aggregate_Min", "Aggregate_Max"):
            fn = "min_by" if name == "Aggregate_Min" else "max_by"
            return f"{fn}({a}, sp_okey({a})) FILTER (WHERE {a} IS NOT NULL)"
        if name == "Aggregate_GroupConcat":
            sep = _get(agg, "separator")
            sep = " " if sep is None else str(sep)
            return f"""CASE WHEN bool_and(sp_isstrlike({a})) IS NOT FALSE THEN '"' || sp_esc(coalesce(string_agg({distinct}sp_lex({a}), {sql_str(sep)}), '')) || '"' END"""
        n = f"sp_num({a})"
        r = f"sp_nrank({a})"
        all_numeric = f"count({a}) = count({n})"
        if name == "Aggregate_Sum":
            return f"CASE WHEN {all_numeric} THEN sp_mknum(coalesce(sum({distinct}{n}), 0), coalesce(max({r}), 0)) END"
        if name == "Aggregate_Avg":
            return f"""CASE WHEN {all_numeric} THEN CASE WHEN count({n}) = 0 THEN '"0"^^<{XSD}integer>' ELSE sp_mknum(avg({distinct}{n}), greatest(max({r}), 1)) END END"""
        raise SparqlError(f"Unsupported aggregate {name}")

    # Expressions
    def expr(self, e, ctx):
        if isinstance(e, Variable):
            return "t", ctx.var(_name(e))
        if isinstance(e, (URIRef, Literal)):
            return "t", sql_str(term_to_nt(e))
        if isinstance(e, BNode):
            return "t", sql_str(term_to_nt(e))
        if isinstance(e, CompValue):
            method = getattr(self, "e_" + e.name, None)
            if method is None:
                raise SparqlError(f"Unsupported SPARQL expression: {e.name}")
            return method(e, ctx)
        raise SparqlError(f"Unsupported SPARQL expression: {e!r}")

    def e_ConditionalOrExpression(self, e, ctx):
        parts = [ctx.bool(x) for x in [e.expr] + list(e.other or [])]
        return "b", "(" + " OR ".join(parts) + ")"

    def e_ConditionalAndExpression(self, e, ctx):
        parts = [ctx.bool(x) for x in [e.expr] + list(e.other or [])]
        return "b", "(" + " AND ".join(parts) + ")"

    def e_UnaryNot(self, e, ctx):
        return "b", f"(NOT {ctx.bool(e.expr)})"

    def e_RelationalExpression(self, e, ctx):
        op = e.op
        a = ctx.aterm(e.expr)
        if op in ("IN", "NOT IN"):
            parts = [f"sp_eq({a}, {ctx.aterm(x)})" for x in _list(e.other)]
            sql = "(" + " OR ".join(parts) + ")" if parts else "FALSE"
            return "b", sql if op == "IN" else f"(NOT {sql})"
        b = ctx.aterm(e.other)
        return "b", {
            "=": f"sp_eq({a}, {b})",
            "!=": f"(NOT sp_eq({a}, {b}))",
            "<": f"sp_lt({a}, {b})",
            ">": f"sp_lt({b}, {a})",
            "<=": f"(sp_lt({a}, {b}) OR sp_eq({a}, {b}))",
            ">=": f"(sp_lt({b}, {a}) OR sp_eq({a}, {b}))",
        }[op]

    def numeric(self, ctx, e):
        "Hoisted numeric value and type rank of an expression"
        t = ctx.aterm(e)
        return ctx.atom(f"sp_num({t})"), ctx.atom(f"sp_nrank({t})")

    def arith(self, ctx, op, a, b):
        (na, ra), (nb, rb) = a, b
        rank = f"greatest({ra}, {rb})"
        if op == "/":
            value = f"CASE WHEN {rank} <= 1 AND {nb} = 0 THEN NULL ELSE {na} / {nb} END"
            rank = f"greatest({ra}, {rb}, 1)"
        else:
            value = f"{na} {op} {nb}"
        return ctx.atom(f"sp_mknum({ctx.atom(value)}, {ctx.atom(rank)})")

    def e_AdditiveExpression(self, e, ctx):
        return self.fold(e, ctx)

    e_MultiplicativeExpression = e_AdditiveExpression

    def fold(self, e, ctx):
        result = None
        current = self.numeric(ctx, e.expr)
        for op, other in zip(e.op or [], e.other or []):
            result = self.arith(ctx, op, current, self.numeric(ctx, other))
            current = (ctx.atom(f"sp_num({result})"), ctx.atom(f"sp_nrank({result})"))
        if result is None:
            return "t", ctx.term(e.expr)
        return "t", result

    def e_UnaryMinus(self, e, ctx):
        n, r = self.numeric(ctx, e.expr)
        return "t", f"sp_mknum(-{n}, {r})"

    def e_UnaryPlus(self, e, ctx):
        n, r = self.numeric(ctx, e.expr)
        return "t", f"sp_mknum({n}, {r})"

    def exists(self, e, ctx):
        graph = e.graph
        if graph.name in ("GroupGraphPatternSub", "GroupGraphPattern"):
            graph = translateGroupGraphPattern(graph)
        inner = self.pattern(graph, ctx.gctx)
        alias = self.fresh("ex")
        conds = []
        for v in inner.vars:
            if v in ctx.plan.vars:
                c = self.col(v)
                conds.append(f"(x.{c} IS NULL OR {alias}.{c} = x.{c})")
        where = " AND ".join(conds) if conds else "TRUE"
        return f"EXISTS (SELECT 1 FROM {inner.name} AS {alias} WHERE {where})"

    def e_Builtin_EXISTS(self, e, ctx):
        return "b", self.exists(e, ctx)

    def e_Builtin_NOTEXISTS(self, e, ctx):
        return "b", f"(NOT {self.exists(e, ctx)})"

    def e_Builtin_BOUND(self, e, ctx):
        return "b", f"({ctx.var(_name(e.arg))} IS NOT NULL)"

    def e_Builtin_COALESCE(self, e, ctx):
        args = [ctx.term(a) for a in _list(e.arg)]
        if not args:
            return "t", "NULL"
        return "t", f"COALESCE({', '.join(args)})"

    def e_Builtin_IF(self, e, ctx):
        c = ctx.abool(e.arg1)
        return "t", f"CASE WHEN {c} THEN {ctx.term(e.arg2)} WHEN NOT {c} THEN {ctx.term(e.arg3)} END"

    def e_Builtin_sameTerm(self, e, ctx):
        return "b", f"({ctx.term(e.arg1)} = {ctx.term(e.arg2)})"

    def e_Builtin_isIRI(self, e, ctx):
        return "b", f"sp_isiri({ctx.aterm(e.arg)})"

    e_Builtin_isURI = e_Builtin_isIRI

    def e_Builtin_isBLANK(self, e, ctx):
        return "b", f"sp_isbnode({ctx.aterm(e.arg)})"

    def e_Builtin_isLITERAL(self, e, ctx):
        return "b", f"sp_islit({ctx.aterm(e.arg)})"

    def e_Builtin_isNUMERIC(self, e, ctx):
        a = ctx.aterm(e.arg)
        return "b", f"CASE WHEN {a} IS NOT NULL THEN sp_num({a}) IS NOT NULL END"

    def e_Builtin_STR(self, e, ctx):
        return "t", f"sp_str({ctx.aterm(e.arg)})"

    def e_Builtin_LANG(self, e, ctx):
        a = ctx.aterm(e.arg)
        return "t", f"""CASE WHEN sp_islit({a}) THEN '"' || sp_lang({a}) || '"' END"""

    def e_Builtin_DATATYPE(self, e, ctx):
        a = ctx.aterm(e.arg)
        return "t", f"CASE WHEN sp_islit({a}) THEN '<' || sp_dt({a}) || '>' END"

    def e_Builtin_IRI(self, e, ctx):
        a = ctx.aterm(e.arg)
        base = sql_str(self.base or "")
        return "t", (
            f"CASE WHEN sp_isiri({a}) THEN {a} WHEN sp_isstr({a}) THEN '<' || "
            f"CASE WHEN regexp_matches(sp_lex({a}), '^[A-Za-z][A-Za-z0-9+.-]*:') THEN sp_lex({a}) "
            f"ELSE {base} || sp_lex({a}) END || '>' END"
        )

    e_Builtin_URI = e_Builtin_IRI

    def e_Builtin_BNODE(self, e, ctx):
        if _get(e, "arg") is None:
            return "t", "'_:b' || replace(CAST(gen_random_uuid() AS VARCHAR), '-', '')"
        a = ctx.aterm(e.arg)
        # The same string gives the same blank node within a solution, and a
        # different one in other solutions: include the other bindings in the
        # hash, except blank nodes which could have been made by BNODE itself.
        others = [
            f"CASE WHEN NOT sp_isbnode(x.{self.col(v)}) THEN x.{self.col(v)} END"
            for v in ctx.plan.vars
            if not v.startswith("__")
        ]
        key = f"concat_ws(chr(1), sp_lex({a}), {', '.join(others)})" if others else f"sp_lex({a})"
        return "t", f"CASE WHEN sp_isstr({a}) THEN '_:b' || md5({key}) END"

    def e_Builtin_STRLEN(self, e, ctx):
        a = ctx.aterm(e.arg)
        return "t", f"""CASE WHEN sp_isstrlike({a}) THEN '"' || length(sp_lex({a})) || '"^^<{XSD}integer>' END"""

    def e_Builtin_UCASE(self, e, ctx):
        a = ctx.aterm(e.arg)
        return "t", f"CASE WHEN sp_isstrlike({a}) THEN sp_samekind({a}, upper(sp_lex({a}))) END"

    def e_Builtin_LCASE(self, e, ctx):
        a = ctx.aterm(e.arg)
        return "t", f"CASE WHEN sp_isstrlike({a}) THEN sp_samekind({a}, lower(sp_lex({a}))) END"

    def e_Builtin_SUBSTR(self, e, ctx):
        a = ctx.aterm(e.arg)
        start, _ = self.numeric(ctx, e.start)
        if _get(e, "length") is not None:
            length, _ = self.numeric(ctx, e.length)
            sub = f"sp_substr3(sp_lex({a}), {start}, {length})"
        else:
            sub = f"sp_substr2(sp_lex({a}), {start})"
        return "t", f"CASE WHEN sp_isstrlike({a}) THEN sp_samekind({a}, {ctx.atom(sub)}) END"

    def two_strings(self, e, ctx):
        return ctx.aterm(e.arg1), ctx.aterm(e.arg2)

    def e_Builtin_STRSTARTS(self, e, ctx):
        a, b = self.two_strings(e, ctx)
        return "b", f"CASE WHEN sp_compat({a}, {b}) THEN starts_with(sp_lex({a}), sp_lex({b})) END"

    def e_Builtin_STRENDS(self, e, ctx):
        a, b = self.two_strings(e, ctx)
        return "b", f"CASE WHEN sp_compat({a}, {b}) THEN ends_with(sp_lex({a}), sp_lex({b})) END"

    def e_Builtin_CONTAINS(self, e, ctx):
        a, b = self.two_strings(e, ctx)
        return "b", f"CASE WHEN sp_compat({a}, {b}) THEN contains(sp_lex({a}), sp_lex({b})) END"

    def e_Builtin_STRBEFORE(self, e, ctx):
        a, b = self.two_strings(e, ctx)
        pos = ctx.atom(f"strpos(sp_lex({a}), sp_lex({b}))")
        return "t", (
            f"""CASE WHEN sp_compat({a}, {b}) THEN CASE WHEN {pos} = 0 THEN '""' """
            f"ELSE sp_samekind({a}, substr(sp_lex({a}), 1, {pos} - 1)) END END"
        )

    def e_Builtin_STRAFTER(self, e, ctx):
        a, b = self.two_strings(e, ctx)
        pos = ctx.atom(f"strpos(sp_lex({a}), sp_lex({b}))")
        return "t", (
            f"""CASE WHEN sp_compat({a}, {b}) THEN CASE WHEN {pos} = 0 THEN '""' """
            f"ELSE sp_samekind({a}, substr(sp_lex({a}), {pos} + length(sp_lex({b})))) END END"
        )

    def e_Builtin_ENCODE_FOR_URI(self, e, ctx):
        a = ctx.aterm(e.arg)
        return "t", f"""CASE WHEN sp_isstrlike({a}) THEN '"' || url_encode(sp_lex({a})) || '"' END"""

    def e_Builtin_CONCAT(self, e, ctx):
        args = [ctx.aterm(a) for a in _list(e.arg)]
        if not args:
            return "t", "'\"\"'"
        ok = " AND ".join(f"sp_isstrlike({a})" for a in args)
        lex = " || ".join(f"sp_lex({a})" for a in args)
        suffixes = [ctx.atom(f"sp_suffix({a})") for a in args]
        same = " AND ".join(f"{s} = {suffixes[0]}" for s in suffixes[1:]) or "TRUE"
        suffix = f"CASE WHEN {same} THEN {suffixes[0]} ELSE '' END"
        return "t", f"""CASE WHEN {ok} THEN '"' || sp_esc({lex}) || '"' || {suffix} END"""

    def e_Builtin_LANGMATCHES(self, e, ctx):
        a, b = self.two_strings(e, ctx)
        tag, rng = f"lower(sp_lex({a}))", f"lower(sp_lex({b}))"
        return "b", (
            f"CASE WHEN sp_isstr({a}) AND sp_isstr({b}) THEN CASE WHEN {rng} = '*' THEN {tag} <> '' "
            f"ELSE {tag} = {rng} OR starts_with({tag}, {rng} || '-') END END"
        )

    def regex_pattern(self, ctx, pattern, flags):
        "SQL for an RE2 pattern, with the XPath flags as an inline prefix"
        p = ctx.aterm(pattern)
        if flags is None:
            return f"sp_lex({p})"
        if not isinstance(flags, Literal):
            raise SparqlError("Only constant regular expression flags are supported")
        flags = str(flags)
        if "q" in flags:
            p = f"regexp_replace(sp_lex({p}), '([\\\\^$.|?*+()\\[\\]{{}}])', '\\\\\\1', 'g')"
        else:
            p = f"sp_lex({p})"
        if "x" in flags:
            p = f"regexp_replace({p}, '\\s', '', 'g')"
        inline = "".join(f for f in flags if f in "ism")
        return f"'(?{inline})' || {p}" if inline else p

    def e_Builtin_REGEX(self, e, ctx):
        t = ctx.aterm(e.text)
        pattern = self.regex_pattern(ctx, e.pattern, _get(e, "flags"))
        return "b", f"CASE WHEN sp_isstrlike({t}) THEN regexp_matches(sp_lex({t}), {pattern}) END"

    def e_Builtin_REPLACE(self, e, ctx):
        t = ctx.aterm(e.arg)
        pattern = self.regex_pattern(ctx, e.pattern, _get(e, "flags"))
        repl = ctx.aterm(e.replacement)
        value = ctx.atom(
            f"regexp_replace(sp_lex({t}), {pattern}, sp_xpathrepl(sp_lex({repl})), 'g')"
        )
        return "t", f"CASE WHEN sp_isstrlike({t}) THEN sp_samekind({t}, {value}) END"

    def e_Builtin_ABS(self, e, ctx):
        n, r = self.numeric(ctx, e.arg)
        return "t", f"sp_mknum(abs({n}), {r})"

    def e_Builtin_CEIL(self, e, ctx):
        n, r = self.numeric(ctx, e.arg)
        return "t", f"sp_mknum(ceil({n}), {r})"

    def e_Builtin_FLOOR(self, e, ctx):
        n, r = self.numeric(ctx, e.arg)
        return "t", f"sp_mknum(floor({n}), {r})"

    def e_Builtin_ROUND(self, e, ctx):
        n, r = self.numeric(ctx, e.arg)
        return "t", f"sp_mknum(sp_round({n}), {r})"

    def e_Builtin_RAND(self, e, ctx):
        return "t", f"""'"' || random() || '"^^<{XSD}double>'"""

    def e_Builtin_NOW(self, e, ctx):
        return "t", sql_str(f'"{self.now}"^^<{XSD}dateTime>')

    def e_Builtin_UUID(self, e, ctx):
        return "t", "'<urn:uuid:' || gen_random_uuid() || '>'"

    def e_Builtin_STRUUID(self, e, ctx):
        return "t", """'"' || gen_random_uuid() || '"'"""

    def datetime_part(self, e, ctx, group):
        return ctx.atom(f"sp_dtpart({ctx.aterm(e.arg)}, {group})")

    def integer_part(self, e, ctx, group):
        p = self.datetime_part(e, ctx, group)
        return "t", f"""'"' || CAST(CAST({p} AS BIGINT) AS VARCHAR) || '"^^<{XSD}integer>'"""

    def e_Builtin_YEAR(self, e, ctx):
        return self.integer_part(e, ctx, 1)

    def e_Builtin_MONTH(self, e, ctx):
        return self.integer_part(e, ctx, 2)

    def e_Builtin_DAY(self, e, ctx):
        return self.integer_part(e, ctx, 3)

    def e_Builtin_HOURS(self, e, ctx):
        return self.integer_part(e, ctx, 4)

    def e_Builtin_MINUTES(self, e, ctx):
        return self.integer_part(e, ctx, 5)

    def e_Builtin_SECONDS(self, e, ctx):
        p = self.datetime_part(e, ctx, 6)
        return "t", f"""'"' || sp_fmtdec(CAST({p} AS DOUBLE)) || '"^^<{XSD}decimal>'"""

    def e_Builtin_TIMEZONE(self, e, ctx):
        p = self.datetime_part(e, ctx, 7)
        return "t", f"""'"' || sp_tzdur({p}) || '"^^<{XSD}dayTimeDuration>'"""

    def e_Builtin_TZ(self, e, ctx):
        a = ctx.aterm(e.arg)
        p = self.datetime_part(e, ctx, 7)
        return "t", f"""CASE WHEN sp_isdtm({a}) THEN '"' || coalesce({p}, '') || '"' END"""

    def hash_function(self, e, ctx, fn):
        a = ctx.aterm(e.arg)
        return "t", f"""CASE WHEN sp_isstr({a}) THEN '"' || {fn}(sp_lex({a})) || '"' END"""

    def e_Builtin_MD5(self, e, ctx):
        return self.hash_function(e, ctx, "md5")

    def e_Builtin_SHA1(self, e, ctx):
        return self.hash_function(e, ctx, "sha1")

    def e_Builtin_SHA256(self, e, ctx):
        return self.hash_function(e, ctx, "sha256")

    def e_Builtin_SHA384(self, e, ctx):
        return self.hash_function(e, ctx, "sp_sha384")

    def e_Builtin_SHA512(self, e, ctx):
        return self.hash_function(e, ctx, "sp_sha512")

    def e_Builtin_STRLANG(self, e, ctx):
        a, b = self.two_strings(e, ctx)
        return "t", (
            f"""CASE WHEN sp_isstr({a}) AND sp_isstr({b}) AND sp_lex({b}) <> '' """
            f"""THEN '"' || sp_rawlex({a}) || '"@' || sp_lex({b}) END"""
        )

    def e_Builtin_STRDT(self, e, ctx):
        a, b = self.two_strings(e, ctx)
        return "t", (
            f"""CASE WHEN sp_isstr({a}) AND sp_isiri({b}) THEN '"' || sp_rawlex({a}) || '"' || """
            f"""CASE WHEN {b} = '<{XSD}string>' THEN '' ELSE '^^' || {b} END END"""
        )

    def e_Function(self, e, ctx):
        iri = str(e.iri)
        args = _list(e.expr)
        if not iri.startswith(XSD) or len(args) != 1:
            raise SparqlError(f"Unsupported function <{iri}>")
        a = ctx.aterm(args[0])
        target = iri[len(XSD) :]
        n = f"sp_num({a})"
        string_lex = f"CASE WHEN sp_isstr({a}) THEN trim(sp_lex({a})) END"
        s = ctx.atom(string_lex)
        if target == "string":
            return "t", f"sp_strcast({a})"
        if target == "boolean":
            value = (
                f"CASE WHEN sp_isbool({a}) THEN sp_bool({a}) "
                f"WHEN {n} IS NOT NULL THEN {n} <> 0 AND NOT isnan({n}) "
                f"WHEN {s} IN ('true', '1') THEN TRUE WHEN {s} IN ('false', '0') THEN FALSE END"
            )
            return "b", value
        if target == "integer":
            value = (
                f"CASE WHEN sp_isbool({a}) THEN CASE WHEN sp_bool({a}) THEN 1 ELSE 0 END "
                f"WHEN {n} IS NOT NULL THEN trunc({n}) "
                f"WHEN regexp_full_match({s}, '{INTEGER_RE}') THEN TRY_CAST({s} AS DOUBLE) END"
            )
            return "t", f"sp_mknum({ctx.atom(value)}, 0)"
        if target == "decimal":
            value = (
                f"CASE WHEN sp_isbool({a}) THEN CASE WHEN sp_bool({a}) THEN 1 ELSE 0 END "
                f"WHEN {n} IS NOT NULL THEN {n} "
                f"WHEN regexp_full_match({s}, '{DECIMAL_RE}') THEN TRY_CAST({s} AS DOUBLE) END"
            )
            return "t", f"sp_mknum({ctx.atom(value)}, 1)"
        if target in ("float", "double"):
            value = (
                f"CASE WHEN sp_isbool({a}) THEN CASE WHEN sp_bool({a}) THEN 1 ELSE 0 END "
                f"WHEN {n} IS NOT NULL THEN {n} "
                f"WHEN regexp_full_match({s}, '{DOUBLE_RE}') THEN TRY_CAST({s} AS DOUBLE) END"
            )
            return "t", f"sp_mknum({ctx.atom(value)}, {2 if target == 'float' else 3})"
        if target == "dateTime":
            return "t", (
                f"CASE WHEN sp_isdtm({a}) THEN {a} "
                f"""WHEN regexp_matches({s}, '{DATETIME_RE}') THEN '"' || {s} || '"^^<{XSD}dateTime>' END"""
            )
        raise SparqlError(f"Unsupported function <{iri}>")


def _walk(node):
    "All the CompValues in a parse tree"
    if isinstance(node, CompValue):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, (list, tuple)) or type(node).__name__ == "ParseResults":
        for value in node:
            yield from _walk(value)


def _walk_terms(node):
    "All the terms in a parse tree"
    if isinstance(node, CompValue):
        for value in node.values():
            yield from _walk_terms(value)
    elif isinstance(node, (list, tuple)) or type(node).__name__ == "ParseResults":
        for value in node:
            yield from _walk_terms(value)
    else:
        yield node


def _free_vars(node):
    "Variables in an expression that are not inside an aggregate"
    if isinstance(node, Variable):
        yield node
    elif isinstance(node, CompValue):
        if node.name.startswith("Aggregate_") or node.name.endswith("EXISTS"):
            return
        for value in node.values():
            yield from _free_vars(value)
    elif isinstance(node, (list, tuple)) or type(node).__name__ == "ParseResults":
        for value in node:
            yield from _free_vars(value)


def _validate_grouping(tree):
    """Check the rule that rdflib does not: in an aggregate query only grouped
    variables may be used outside of aggregates in the projection."""
    for select in _walk(tree):
        if select.name not in ("SelectQuery", "SubSelect"):
            continue
        projection = _get(select, "projection")
        groupby = _get(select, "groupby")
        clauses = [projection, _get(select, "having"), _get(select, "orderby")]
        has_aggregate = any(
            n.name.startswith("Aggregate_") for c in clauses for n in _walk(c)
        )
        if not groupby and not has_aggregate:
            continue
        if not projection:
            raise SparqlError("SELECT * is not allowed in an aggregate query")
        grouped = set()
        # Variables of a trailing VALUES clause are joined after the grouping
        values = _get(select, "valuesClause")
        if values is not None:
            grouped.update(n for n in _walk_terms(values) if isinstance(n, Variable))
        for condition in groupby.condition if groupby else []:
            if isinstance(condition, Variable):
                grouped.add(condition)
            elif _get(condition, "var") is not None:
                grouped.add(condition.var)
        for item in projection:
            if _get(item, "expr") is None:
                if item.var not in grouped:
                    raise SparqlError(
                        f"Variable ?{item.var} is used in an aggregate query without being grouped"
                    )
            else:
                for v in _free_vars(item.expr):
                    if v not in grouped:
                        raise SparqlError(
                            f"Variable ?{v} is used in an aggregate query without being grouped"
                        )
                grouped.add(item.evar)


def parse(query: str, base: str | None = None):
    "Parse a SPARQL query into the SPARQL algebra, keeping literals as written"
    normalize = rdflib.NORMALIZE_LITERALS
    rdflib.NORMALIZE_LITERALS = False
    try:
        tree = parseQuery(query)
        _validate_grouping(tree)
        q = translateQuery(tree, base=base)
        q.select_star = tree[1].name == "SelectQuery" and not _get(tree[1], "projection")
        return q
    except SparqlError:
        raise
    except Exception as e:
        raise SparqlError(f"Could not parse SPARQL query: {e}") from e
    finally:
        rdflib.NORMALIZE_LITERALS = normalize


def to_sql(
    query: str, union_default_graph: bool = False, base: str | None = None
) -> str:
    "Return the DuckDB SQL a SPARQL query is translated into"
    compiler = Compiler(union_default_graph, base)
    sql, _ = compiler.compile(parse(query, base))
    return sql


def construct_template(algebra):
    "The template of a CONSTRUCT query, the pattern for a CONSTRUCT WHERE query"
    if algebra.template is not None:
        return algebra.template
    return [t for n in _walk(algebra.p) if n.name == "BGP" for t in n.triples]


def _construct(template, rows, vars):
    triples = []
    seen = set()
    for n, row in enumerate(rows):
        binding = dict(zip(vars, row))
        for triple in template:
            out = []
            for t in triple:
                if isinstance(t, Variable):
                    out.append(binding.get(_name(t)))
                elif isinstance(t, BNode):
                    out.append(f"_:{t}_{n}")
                else:
                    out.append(term_to_nt(t))
            s, p, o = out
            if s is None or p is None or o is None:
                continue
            if s.startswith('"') or not p.startswith("<"):
                continue
            if (s, p, o) not in seen:
                seen.add((s, p, o))
                triples.append((s, p, o))
    return triples


def sparql(
    query: str,
    db_path: str | None = None,
    union_default_graph: bool = False,
    base: str | None = None,
):
    """Run a SPARQL 1.1 query over a bikidata database.

    SELECT queries return a dict in the SPARQL 1.1 Query Results JSON format:
        {"head": {"vars": [...]}, "results": {"bindings": [...]}}
    ASK queries return {"head": {}, "boolean": True/False}
    CONSTRUCT and DESCRIBE queries return a list of (s, p, o) tuples, where each
    term is encoded as in N-Triples, for example: ("<http://x>", "<http://y>", '"z"@en')

    The default graph consists of the triples that were not loaded into a
    named graph. Pass union_default_graph=True to query the union of all the
    graphs as the default graph instead.
    """
    q = parse(query, base)
    compiler = Compiler(union_default_graph, base)
    sql, vars = compiler.compile(q)
    con = duckdb.connect(db_path or main.DB_PATH, read_only=True)
    try:
        setup_connection(con, compiler.consts)
        rows = con.execute(sql).fetchall()
    finally:
        con.close()

    form = q.algebra.name
    if form == "AskQuery":
        return {"head": {}, "boolean": bool(rows[0][0])}
    if form == "ConstructQuery":
        return _construct(construct_template(q.algebra), rows, vars)
    if form == "DescribeQuery":
        return [tuple(row) for row in rows]
    bindings = []
    for row in rows:
        binding = {}
        for var, value in zip(vars, row):
            if value is not None:
                binding[var] = nt_to_json(value)
        bindings.append(binding)
    return {"head": {"vars": vars}, "results": {"bindings": bindings}}
