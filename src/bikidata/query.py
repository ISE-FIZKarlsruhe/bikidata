import time, json, random, hashlib, os
from .semantic import get_embedding, VEC_DIM
import xxhash
from .main import DB_PATH, log
import duckdb

try:
    import redis.asyncio as redis

    REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
    redis_client = redis.Redis(host=REDIS_HOST)
except:
    log.warning("Redis not available, async queries will not work")


def raw():
    DB = duckdb.connect(DB_PATH, read_only=True)
    return DB.cursor()


def total():
    DB = duckdb.connect(DB_PATH, read_only=True)
    db_cursor = DB.cursor()
    total = db_cursor.execute("select count(distinct s) from triples").fetchone()[0]
    return total


def properties():
    """
    Returns a list of all properties in the database.
    """
    SQL = "select distinct I.value, count(distinct s) from triples T join iris I on T.p = I.hash group by I.value"
    DB = duckdb.connect(DB_PATH, read_only=True)
    db_cursor = DB.cursor()
    return dict(db_cursor.execute(SQL).fetchall())


def count_by_property(property):
    SQL = "select I.value, count(distinct s) from triples T join iris I on T.o = I.hash join iris II on T.p = II.hash where II.value = ? group by I.value"
    DB = duckdb.connect(DB_PATH, read_only=True)
    db_cursor = DB.cursor()

    return dict(db_cursor.execute(SQL, (property,)).fetchall())


def sp(s: list[str], p: str | None):
    "For a list of subjects s,  and a predicates p, return the triples where s and p match"
    if not isinstance(s, list):
        raise TypeError("s must be a list of strings")
    ss = [xxhash.xxh64_hexdigest(x).lower() for x in s]
    sss = ",".join(f"'0x{x}'::ubigint" for x in ss)
    where = (
        f"where U.hash in ({sss}) and UU.hash = '0x{xxhash.xxh64_hexdigest(p).lower()}'::ubigint"
        if p
        else f"where U.hash in ({sss})"
    )

    SQL = f"select U.value, UU.value, UUU.value, L.value from triples T left join iris U on T.s = U.hash left join iris UU on T.p = UU.hash left join iris UUU on T.o = UUU.hash left join literals L on T.o = L.hash {where}"

    DB = duckdb.connect(DB_PATH, read_only=True)
    db_cursor = DB.cursor()
    data = {}
    for s, p, o, oo in db_cursor.execute(SQL).fetchall():
        data.setdefault(s, []).append(o if o else oo)
    return data


def spo(*args, **kwargs):
    """
    Returns triples with the given subject, predicate, and object.
    """
    vals = {}
    for i, t in enumerate(args):
        if t is not None:
            if not isinstance(t, str):
                raise TypeError("s, p, and o must be strings or None")
            vals[i] = xxhash.xxh64_hexdigest(t).lower() if t else None

    conditions = []
    for i, t in enumerate(("s", "p", "o", "g")):
        tt = vals.get(i)
        if tt:
            conditions.append(f"{t} = '0x{tt}'::ubigint")

    size = kwargs.get("size", 1000)
    start = kwargs.get("start", 0)
    start = "" if start == 0 else f" offset {start}"

    conditions_ = " and ".join(conditions)
    where = f" where {conditions_}" if conditions_ else ""
    SQL = f"select U.value, UU.value, UUU.value, L.value from triples T left join iris U on T.s = U.hash left join iris UU on T.p = UU.hash left join iris UUU on T.o = UUU.hash left join literals L on T.o = L.hash{where}{start} limit {size}"

    DB = duckdb.connect(DB_PATH, read_only=True)
    db_cursor = DB.cursor()
    return [(s, p, o if o else oo) for s, p, o, oo in db_cursor.execute(SQL).fetchall()]


def q_to_sql(query: dict):
    p = str(query.get("p", "")).strip(" ")
    o = str(query.get("o", "")).strip(" ")
    g = str(query.get("g", "")).strip(" ")
    pp = xxhash.xxh64_hexdigest(p).lower()

    gg = [xxhash.xxh64_hexdigest(g_).lower() for g_ in g.split(" ")]

    if o.startswith("<") and o.endswith(">") and len(o.split(" ")) > 1:
        oo = ", ".join(
            [
                "'0x" + xxhash.xxh64_hexdigest(multi_o).lower() + "'::ubigint"
                for multi_o in o.split(" ")
            ]
        )
        oo = f" in ({oo})"
    else:
        oo = xxhash.xxh64_hexdigest(o).lower()
        oo = f" = '0x{oo}'::ubigint"
    extra_g = ""
    if g != "":
        g_hashes = ", ".join([f"'0x{ggg}'::ubigint" for ggg in gg])
        extra_g = f" and T0.g in ({g_hashes})"

    extra_fts_fields = query.get("_extra_fts_fields", "")

    if p == "" and (o.startswith("<") or o.startswith("_:")):
        return f"(select distinct s from triples T0 where o{oo} {extra_g})"
    elif p == "id":
        if o.startswith("random") or o.startswith("sample"):
            o_split = o.split(" ")
            o_count = 1
            if len(o_split) > 1:
                o = o_split[0]
                try:
                    o_count = int(o_split[1])
                except ValueError:
                    o_count = 1
            return f"(select distinct s from triples using sample {o_count} {extra_g})"
        return f"(select distinct s from triples where s{oo} {extra_g})"
    elif p.startswith("semantic"):
        # convert the o to a vector
        q_vector = get_embedding(o)
        return f"""(select distinct s{extra_fts_fields} from (select T0.s, array_cosine_distance(vec, CAST({q_vector} AS FLOAT[{VEC_DIM}])) as distance, 1/distance as score from literals_semantic LS join triples T0 on T0.s = LS.hash where distance < 0.5 {extra_g}))
        """

    elif p.startswith("regex"):
        parts = p.split(" ")
        extra = ""
        if len(parts) == 2:
            p, p_property = parts
            if p_property[0] == "<" and p_property[-1] == ">":
                p_property_hash = xxhash.xxh64_hexdigest(p_property).lower()
                extra = f" and T.p = '0x{p_property_hash}'::ubigint"
        psql = f"select distinct T.s from triples T join literals L on T.o = L.hash where L.value similar to '{o}'{extra} {extra_g}"
        return psql
    elif p.startswith("ftss"):
        psql = f"""(with scored as (select *, fts_main_fts.match_bm25(s, '{o}', conjunctive:=1) AS score from fts)
select s{extra_fts_fields} from scored where score is not null"""
        return f"{psql} {extra_g} )"
    elif p.startswith("fts"):
        parts = p.split(" ")
        parents = 0
        if len(parts) == 2:
            p, parents = parts
            try:
                parents = int(parents)
            except ValueError:
                parents = 0

        if parents > 0:
            extra = "\n".join(
                [f" join triples T{p+1} on T{p}.s = T{p+1}.o" for p in range(parents)]
            )
        else:
            extra = ""

        psql = f"""(with scored as (select *, fts_main_literals.match_bm25(hash, '{o}', conjunctive:=1) AS score from literals)
    select distinct T{parents}.s{extra_fts_fields} from (select * from scored where score is not null) S join triples T0
    on S.hash = T0.o
    """
        return f"{psql} {extra} {extra_g} )"

    elif p[0] == "<" and p[-1] == ">":
        if o:
            return f"(select distinct s from triples T0 where p = '0x{pp}'::ubigint and o{oo} {extra_g})"
        else:
            return f"(select distinct s from triples T0 where p = '0x{pp}'::ubigint {extra_g})"


# --- ADDED: sort-api (helpers) ---
RDFS_LABEL_IRI = "<http://www.w3.org/2000/01/rdf-schema#label>"


def _iri_hex(iri: str) -> str:
    """Return SQL ubigint literal for a given IRI using the same xxhash64 scheme."""
    h = xxhash.xxh64_hexdigest(iri).lower()
    return f"'0x{h}'::ubigint"


def _normalize_order_rules(order_rules):
    """Accept dict | [dict] | [[dict]] and return a flat [dict] list."""
    if not order_rules:
        return []
    if isinstance(order_rules, dict):
        return [order_rules]
    if (
        isinstance(order_rules, list)
        and order_rules
        and isinstance(order_rules[0], list)
    ):
        return order_rules[0]
    return order_rules


def _lang_case_sql(val_expr: str, langs: list[str]) -> str:
    """
    Build a CASE expression to rank labels by language preference.
    val_expr should be a column like L.value (e.g. '"Text"@de').
    """
    parts = []
    rank = 1
    for lg in langs or []:
        parts.append(f"WHEN {val_expr} LIKE '%\"@{lg}' THEN {rank}")
        rank += 1
    parts.append(f"WHEN {val_expr} NOT LIKE '%\"@%' THEN {rank}")
    rank += 1
    parts.append(f"ELSE {rank}")
    return "CASE " + " ".join(parts) + " END"


def _build_clean_expr(base_expr: str, clean: dict, mode: str) -> str:
    """
    Build SQL expression for sorting ('sort_label') based on cleaning config.
    base_expr is the raw label text WITHOUT @lang (e.g. regexp_extract(...)).
    mode: 'lex' (case-insensitive, normalized) or 'raw' (original string, unless flags set).
    clean: {lower, trim, strip_punct, collapse_space, remove_quotes}
    """
    expr = base_expr
    c = clean or {}
    # Optional: remove surrounding quotes if still present
    if c.get("remove_quotes", False):
        expr = f"regexp_replace({expr}, '^\"|\"$', '')"
    # Collapse multiple whitespace
    if c.get("collapse_space", False):
        expr = f"regexp_replace({expr}, '\\s+', ' ')"
    # Strip leading punctuation / non-alnum
    if c.get("strip_punct", False):
        expr = f"regexp_replace({expr}, '^[^0-9A-Za-z]+', '')"
    # Trim
    if c.get("trim", True):
        expr = f"trim({expr})"
    # Lowercase for lexicographic stability
    if mode == "lex" and c.get("lower", True):
        expr = f"lower({expr})"
    return expr


def _natural_order_block(prefix_alias: str, dir_sql: str) -> str:
    """
    Build ORDER BY block that prioritizes numeric leading prefixes when present.
    prefix_alias: table alias used in SELECT that exposes 'sort_label' (e.g. 'P' or 'N').
    dir_sql: 'ASC' or 'DESC' for both numeric and string sort.
    """
    # We prefer entries WITH a numeric prefix first, then sort by that number,
    # then tie-break by the normalized/picked sort_label, then by s for stability.
    return f"""
ORDER BY
  sort_label IS NULL ASC,
  ({prefix_alias}.num_prefix IS NULL),
  {prefix_alias}.num_prefix {dir_sql},
  sort_label {dir_sql},
  S.s
"""


def _plain_order_block(dir_sql: str, nulls_sql: str) -> str:
    """Fallback ORDER BY without numeric prefix handling."""
    return f"""
ORDER BY
  {nulls_sql},
  sort_label {dir_sql},
  S.s
"""


def _order_build_sorted_table(db_cursor, order_rules: list):
    """
    Create temp table s_sorted(s, sort_label) sorted per the first rule.
    Supported:
      {"by":"label","lang":["de","en"],"dir":"asc","nulls":"last",
       "mode":"lex"|"raw","natural":true|false,
       "clean":{"trim":true,"lower":true,"strip_punct":true,"collapse_space":true}}
      {"by":"property","prop":"<IRI>", ...}
      {"by":"object_label","via":"<IRI>", ...}
    """
    if not order_rules:
        return

    rule = order_rules[0]
    by = (rule.get("by") or "label").lower()
    langs = rule.get("lang") or ["de", "en"]
    direction = (rule.get("dir") or "asc").lower()
    nulls = (rule.get("nulls") or "last").lower()
    mode = (rule.get("mode") or "lex").lower()  # 'lex' or 'raw'
    clean = rule.get("clean") or {"trim": True, "lower": (mode == "lex")}
    natural = bool(
        rule.get("natural", False)
    )  # --- ADDED: sort-api (natural numbers) ---

    dir_sql = "ASC" if direction != "desc" else "DESC"
    nulls_sql = (
        "sort_label IS NULL DESC" if nulls == "first" else "sort_label IS NULL ASC"
    )

    case_expr = _lang_case_sql("L.value", langs)
    raw_text = "regexp_extract(L.value, '^\"(.+)\"', 1)"
    sort_expr = _build_clean_expr(raw_text, clean, mode)

    # Choose post-CTE SELECT and ORDER BY depending on `natural`
    if natural:
        post_block = f"""
, numbered AS (
    SELECT s,
           sort_label,
           TRY_CAST(NULLIF(regexp_extract(sort_label, '^(\\d+)', 1), '') AS INTEGER) AS num_prefix
    FROM pref
)
SELECT S.s, N.sort_label
FROM s_results S
LEFT JOIN numbered N ON N.s = S.s
{_natural_order_block('N', dir_sql)}
"""
    else:
        post_block = f"""
SELECT S.s, P.sort_label
FROM s_results S
LEFT JOIN pref P ON P.s = S.s
{_plain_order_block(dir_sql, nulls_sql)}
"""

    if by == "label":
        prop_sql = _iri_hex(RDFS_LABEL_IRI)
        db_cursor.execute(
            f"""
            create temp table s_sorted as
            with labels as (
                select S.s,
                       L.value as lbl_val,
                       {case_expr} as lang_rank,
                       {sort_expr} as sort_label
                from s_results S
                join triples T on T.s = S.s and T.p = {prop_sql}
                join literals L on L.hash = T.o
            ),
            pref as (
                select s, sort_label
                from (
                    select s, sort_label, lang_rank,
                           row_number() over (partition by s order by lang_rank asc, sort_label asc) as rn
                    from labels
                )
                where rn = 1
            )
            {post_block}
        """
        )

    elif by == "property":
        prop_iri = rule.get("prop")
        if not prop_iri:
            raise ValueError("order.by='property' requires 'prop' (IRI).")
        prop_sql = _iri_hex(prop_iri)
        db_cursor.execute(
            f"""
            create temp table s_sorted as
            with labels as (
                select S.s,
                       L.value as lbl_val,
                       {case_expr} as lang_rank,
                       {sort_expr} as sort_label
                from s_results S
                join triples T on T.s = S.s and T.p = {prop_sql}
                join literals L on L.hash = T.o
            ),
            pref as (
                select s, sort_label
                from (
                    select s, sort_label, lang_rank,
                           row_number() over (partition by s order by lang_rank asc, sort_label asc) as rn
                    from labels
                )
                where rn = 1
            )
            {post_block}
        """
        )

    elif by == "object_label":
        via_iri = rule.get("via")
        if not via_iri:
            raise ValueError("order.by='object_label' requires 'via' (IRI).")
        via_sql = _iri_hex(via_iri)
        rdfs_sql = _iri_hex(RDFS_LABEL_IRI)
        db_cursor.execute(
            f"""
            create temp table s_sorted as
            with objs as (
                select S.s, T1.o as obj
                from s_results S
                join triples T1 on T1.s = S.s and T1.p = {via_sql}
            ),
            olabels as (
                select O.s,
                       L.value as lbl_val,
                       {case_expr} as lang_rank,
                       {sort_expr} as sort_label
                from objs O
                join triples T2 on T2.s = O.obj and T2.p = {rdfs_sql}
                join literals L on L.hash = T2.o
            ),
            pref as (
                select s, sort_label
                from (
                    select s, sort_label, lang_rank,
                           row_number() over (partition by s order by lang_rank asc, sort_label asc) as rn
                    from olabels
                )
                where rn = 1
            )
            {post_block}
        """
        )

    else:
        raise ValueError(f"Unsupported order.by='{by}'")


# --- END ADDED: sort-api (helpers) ---


async def redis_worker():
    log.debug("Entering worker loop, using Redis")
    while True:
        _, serial_query = await redis_client.blpop("bikidata:queries")
        opts = json.loads(serial_query)
        query_hash = opts.get("query_hash")
        query_ticket = opts.get("query_ticket")
        if not query_ticket:
            log.error("No query ticket found in query")
            continue
        cached = await redis_client.get(query_hash)
        if cached:
            log.debug(f"Cache hit for query ticket {query_ticket}")
            result = json.loads(cached)
        else:
            log.debug(f"Processing query ticket {query_ticket}")
            result = query(opts)
            await redis_client.set(query_hash, json.dumps(result), ex=60 * 60 * 24 * 7)
        await redis_client.lpush(query_ticket, json.dumps(result))


class TimeoutError(Exception):
    pass


async def query_async(opts: dict):
    query_ticket = f"{time.time()}-{random.randint(0,1000000)}"
    query_hash = hashlib.md5(
        json.dumps(opts, sort_keys=True).encode("utf8")
    ).hexdigest()
    opts["query_ticket"] = query_ticket
    opts["query_hash"] = query_hash
    serial_query = json.dumps(opts)
    await redis_client.lpush("bikidata:queries", serial_query)
    popresult = await redis_client.blpop(query_ticket, timeout=60)
    if popresult is None:
        raise TimeoutError("Query timed out")
    _, result = popresult
    return json.loads(result)


def query(opts):
    try:
        size = int(opts.get("size", 999))
    except:
        size = 999
    try:
        start = int(opts.get("start", 0))
    except:
        start = 0
    queries = []
    queries_except = []
    # due to the way set semantic works in SQL, the EXCEPT queries should be last in the list
    # but we can not control how users specify them, they might be added first
    fts_for_sorting = []

    exclude_properties = opts.get("exclude_properties", [])

    # --- ADDED: sort-api (order parse & normalize) ---
    order_rules = _normalize_order_rules(opts.get("order", []))
    # --- END ADDED: sort-api ---

    for query in opts.get("filters", []):
        op = query.get("op", "should")
        if str(query.get("p")).startswith("fts") or str(query.get("p")).startswith(
            "semantic"
        ):
            fts_query = query.copy()
            fts_query["_extra_fts_fields"] = ", score "
            if not fts_for_sorting:
                fts_for_sorting = [q_to_sql(fts_query)]
            else:
                if op in ("should", "or"):
                    fts_for_sorting.append(" UNION " + q_to_sql(fts_query))
                elif op in ("must", "and"):
                    fts_for_sorting.append(" INTERSECT " + q_to_sql(fts_query))
        if not queries:
            queries = [q_to_sql(query)]
        else:
            theq = q_to_sql(query)
            if not theq:
                continue
            if op in ("should", "or"):
                queries.append(" UNION " + theq)
            elif op in ("must", "and"):
                queries.append(" INTERSECT " + theq)
            elif op == "not":
                queries_except.append(" EXCEPT " + theq)
    queries.extend(queries_except)
    queries = list(filter(None, queries))

    DB = duckdb.connect(DB_PATH, read_only=True)
    db_cursor = DB.cursor()
    total = 0
    tofetch = set()
    results = {}
    aggregates = {}

    if len(queries) > 0:

        if len(fts_for_sorting) > 0:
            fts_queries_joined = (
                "create temp table s_by_score as select s, max(score) as score from ("
                + "\n".join(fts_for_sorting)
                + ") group by s"
            )
            db_cursor.execute(fts_queries_joined)
            # --- CHANGED: sort-api (remove early ORDER BY; we sort later) ---
            queries_joined = (
                "create temp table s_results as select distinct QJ.s from ("
                + "\n".join(queries)
                + ") QJ left join s_by_score SS on QJ.s = SS.s"
            )
            # --- END CHANGED: sort-api ---
        else:
            # --- CHANGED: sort-api (remove early ORDER BY; we sort later) ---
            queries_joined = (
                "create temp table s_results as select distinct s from ("
                + "\n".join(queries)
                + ")"
            )
            # --- END CHANGED: sort-api ---

        db_cursor.execute(queries_joined)

        # --- ADDED: sort-api (total & wanted page in SQL) ---
        total = db_cursor.execute("select count(*) from s_results").fetchone()[0]

        if order_rules:
            _order_build_sorted_table(db_cursor, order_rules)
            db_cursor.execute(
                f"""
                create temp table wanted as
                select s, row_number() over () as pos
                from s_sorted
                limit {size} offset {start}
            """
            )
        else:
            if len(fts_for_sorting) > 0:
                db_cursor.execute(
                    f"""
                    create temp table wanted as
                    select QJ.s,
                           row_number() over () as pos
                    from s_results QJ
                    left join s_by_score SS on QJ.s = SS.s
                    order by SS.score desc, QJ.s
                    limit {size} offset {start}
                """
                )
            else:
                db_cursor.execute(
                    f"""
                    create temp table wanted as
                    select s, row_number() over () as pos
                    from s_results
                    order by s
                    limit {size} offset {start}
                """
                )
        # --- END ADDED: sort-api ---

        # check for aggregates (computed on full s_results set)
        for agg in opts.get("aggregates", []):
            if agg == "graphs":
                tmp = f"select distinct count(g) as count, I.value as val from s_results S join triples T on S.s = T.s join iris I on T.g = I.hash group by T.g, I.value"
            elif agg == "properties":
                tmp = f"select count(p) as count, I.value as val from s_results S join triples T on S.s = T.s join iris I on T.p = I.hash group by p, I.value"
            else:
                agg_o = xxhash.xxh64_hexdigest(str(agg)).lower()
                tmp = f"(select distinct count(T.s) as count, I.value as val from s_results S join triples T on S.s = T.s join iris I on T.o = I.hash where T.p = '0x{agg_o}'::ubigint group by o, I.value) union (select distinct count(T.s) as count, L.value as val from s_results S join triples T on S.s = T.s join literals L on T.o = L.hash where T.p = '0x{agg_o}'::ubigint group by T.o, L.value) order by count desc"
            aggs = db_cursor.execute(tmp).df()
            aggregates[agg] = aggs

        # fetch triples for the current page in deterministic order (by wanted.pos)
        if db_cursor.execute("select count(*) from wanted").fetchone()[0] > 0:
            if len(exclude_properties) > 0:
                exclude_properties_list = ",".join(
                    [f"'{ep}'" for ep in exclude_properties]
                )
                s_ids_q = f"""
                    with excl_props as (select hash from iris where value in ({exclude_properties_list}))
                    select distinct T.s, T.p, T.o, T.g
                    from wanted W
                    join triples T on T.s = W.s
                    where T.p not in (select hash from excl_props)
                    order by W.pos
                """
            else:
                s_ids_q = """
                    select distinct T.s, T.p, T.o, T.g
                    from wanted W
                    join triples T on T.s = W.s
                    order by W.pos
                """
            triples = db_cursor.execute(s_ids_q).df()

            for _, row in triples.iterrows():
                r_s = row.get("s")
                r_p = row.get("p")
                r_o = row.get("o")
                r_g = row.get("g")
                tofetch.add(r_s)
                tofetch.add(r_p)
                tofetch.add(r_o)
                if r_g is False:
                    tofetch.add(r_g)
                    results.setdefault(r_s, {}).setdefault("graph", set()).add(r_g)
                results.setdefault(r_s, {}).setdefault(r_p, set()).add(r_o)

            # Fetch the paths (restricted to current page subjects)
            for pad in opts.get("paths", []):
                padd = xxhash.xxh64_hexdigest(str(pad)).lower()
                padsql = f"""with recursive parents(s, parent) as 
 (select distinct s , parent from triples left join (select s as part, o as parent from triples where p = '0x{padd}'::ubigint) on s = part),
hier(source, path) as (
    select s, [s]::ubigint[] as path
    from parents
    where parent is null
  union all
    select s, list_prepend(s, hier.path)
    from parents, hier
    where parent = hier.source
)
select source, path from hier where source in (select s from wanted)
"""
                for _, row in db_cursor.execute(padsql).df().iterrows():
                    padr_s = row.get("source")
                    results.setdefault(padr_s, {}).setdefault("_paths", {})
                    results[padr_s]["_paths"][pad] = list(row.get("path"))
                    for x in row.get("path"):
                        tofetch.add(x)

    # Special aggregates
    if "properties" in opts.get("aggregates", []) and len(queries) < 1:
        aggregates["properties"] = db_cursor.execute(
            "select count(p) as count, I.value as val from triples T join iris I on T.p = I.hash group by p, I.value"
        ).df()
    if "graphs" in opts.get("aggregates", []) and len(queries) < 1:
        aggregates["graphs"] = db_cursor.execute(
            "select count(g) as count, I.value as val from triples T join iris I on T.g = I.hash group by g, I.value"
        ).df()

    if len(tofetch) > 0:
        tofetch = ", ".join([str(x) for x in tofetch])

        HV = dict(
            [
                (hash, value)
                for hash, value in db_cursor.execute(
                    f"(select hash, value from iris where hash in ({tofetch})) union (select hash, value from literals where hash in ({tofetch}))"
                ).fetchall()
            ]
        )
        HV["graph"] = "graph"

    results_mapped = {}
    for entity, fields in results.items():
        for field, vals in fields.items():
            if field == "_paths":
                continue
            for val in vals:
                results_mapped.setdefault(HV.get(entity), {}).setdefault(
                    HV.get(field), []
                ).append(HV.get(val))
        mapped_entity = HV.get(entity)
        if mapped_entity in results_mapped:
            results_mapped[mapped_entity]["id"] = mapped_entity
            ## fetching the paths recursively can cause entities to be returned with only the path field, which is not a valid result entity. Skip them
            graph = results_mapped[mapped_entity].get("graph", [])
            results_mapped[mapped_entity]["graph"] = list(graph)
            if "_paths" in fields:
                for path, vals in fields["_paths"].items():
                    vals = [HV.get(val) for val in vals if val != entity]
                    results_mapped[mapped_entity].setdefault("_paths", {})[path] = vals

    # This is a security risk, we can not just accept a random filename
    # Either remove it completely, or add some form of sanitization
    # if "dump" in opts:
    #     with open(opts["dump"], "w") as DUMPFILE:
    #         for entity, fields in results_mapped.items():
    #             for field, vals in fields.items():
    #                 for val in vals:
    #                     DUMPFILE.write(f"{entity} {field} {val} .\n")

    aggregates_mapped = {}
    for agg, aggs in aggregates.items():
        aggregates_mapped[agg] = []
        for _, row in aggs.iterrows():
            aggregates_mapped[agg].append((row["count"], row["val"]))

    back = {"results": results_mapped, "total": total, "size": size, "start": start}
    if aggregates:
        back["aggregates"] = aggregates_mapped

    return back
