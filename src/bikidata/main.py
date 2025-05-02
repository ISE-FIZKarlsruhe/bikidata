import sys, logging, gzip, re, os
import duckdb
import xxhash

DEBUG = os.environ.get("DEBUG", "0") == "1"

log = logging.getLogger("bikidata")
handler = logging.StreamHandler()
log.addHandler(handler)
if DEBUG:
    log.setLevel(logging.DEBUG)
    handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(levelname)-9s %(name)s %(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)

DB_PATH = os.getenv("BIKIDATA_DB", "bikidata.duckdb")
log.debug(f"BIKIDATA_DB is configured as {DB_PATH}")
DB = duckdb.connect(DB_PATH)


def literal_to_parts(literal: str):
    literal_value = language = datatype = None
    if literal.startswith('"'):
        end_index = literal.rfind('"')
        if end_index > 0:
            literal_value = literal[1:end_index]
            remainder = literal[end_index + 1 :].strip()
            language = datatype = None
            if remainder.startswith("@"):
                language = remainder[1:]
                datatype = None
            elif remainder.startswith("^^"):
                datatype = remainder[2:]
                language = None
    return literal_value, language, datatype


def decode_unicode_escapes(s):
    # See: https://www.w3.org/TR/n-triples/#grammar-production-UCHAR
    unicode_escape_pattern_u = re.compile(
        r"\\u([0-9a-fA-F]{4})"
    )  # \uXXXX (4 hex digits)
    unicode_escape_pattern_U = re.compile(
        r"\\U([0-9a-fA-F]{8})"
    )  # \UXXXXXXXX (8 hex digits)

    def replace_unicode_escape_u(match):
        hex_value = match.group(1)
        return chr(
            int(hex_value, 16)
        )  # Convert hex to integer, then to Unicode character

    def replace_unicode_escape_U(match):
        hex_value = match.group(1)
        return chr(
            int(hex_value, 16)
        )  # Convert hex to integer, then to Unicode character

    s = unicode_escape_pattern_U.sub(replace_unicode_escape_U, s)
    s = unicode_escape_pattern_u.sub(replace_unicode_escape_u, s)

    return s


class StringParamException(Exception):
    pass


def read_nt(triplefile_paths: list):
    if not type(triplefile_paths) == list:
        raise StringParamException(
            "triplefile_paths must be a list of paths to n-triple files, or file-like objects"
        )
    for triplefile_path in triplefile_paths:
        if isinstance(triplefile_path, (str, bytes, os.PathLike)):
            if triplefile_path.endswith(".gz"):
                thefile = gzip.open(triplefile_path, "rb")
            else:
                thefile = open(triplefile_path, "rb")
        elif hasattr(triplefile_path, "read"):
            thefile = triplefile_path
        else:
            raise StringParamException(
                "Each path in triplefile_paths must be a string, bytes, os.PathLike object, or a file-like object"
            )

        g = ""
        for line in thefile:
            if not line.endswith(b" .\n"):
                if line.endswith(b" {\n") and line.startswith(b"<"):
                    # Cater for .trig files by looking for a pattern like
                    # ^<IRI> {\n
                    parts = line.decode("utf8").split(" ")
                    if len(parts) == 2:
                        g = parts[0]
                        continue
                else:
                    continue
            line = decode_unicode_escapes(line.decode("utf8"))
            line = line.strip()
            line = line[:-2]
            parts = line.split(" ")
            if len(parts) > 2:
                s = parts[0]
                p = parts[1]
                o = " ".join(parts[2:])

            if not (s.startswith("<") and s.endswith(">")):
                continue
            if not (p.startswith("<") and p.endswith(">")):
                continue

            yield s, p, o, g


def H(v: str):
    return xxhash.xxh64_hexdigest(v).upper()


def build(
    triplefile_paths: list,
    stemmer: str = "porter",
):
    if len(triplefile_paths) > 0:
        log.debug(f"Building Bikidata index with {triplefile_paths}")
        iterator = read_nt(triplefile_paths)
    else:
        log.error("No triples to index, triplefile_paths not given")
        return

    db_connection = DB.cursor()
    try:
        triple_count = db_connection.execute("select count(*) from triples").fetchall()
        if triple_count[0][0] > 0:
            log.debug(f"The database [{DB_PATH}] already has data, doing nothing")
            return
    except duckdb.CatalogException:
        log.debug("No triples in database yet")

    TRIPLE_PATH = os.getenv("BIKIDATA_TRIPLE_PATH", "triples")
    MAP_PATH = os.getenv("BIKIDATA_MAP_PATH", "maps")

    count = 0

    TRIPLE_OUT_FILE = open(TRIPLE_PATH, "w")
    MAP_OUT_FILE = open(MAP_PATH, "w")

    all_graphs = set()
    for s, p, o, g in iterator:
        try:
            ss = H(s)
            pp = H(p)
            oo = H(o)
            gg = H(g)
            all_graphs.add(g)
            TRIPLE_OUT_FILE.write(f"{ss}\t{pp}\t{oo}\t{gg}\n")
            MAP_OUT_FILE.write(f"{ss}\t|\t{s}\n")
            MAP_OUT_FILE.write(f"{pp}\t|\t{p}\n")
            MAP_OUT_FILE.write(f"{oo}\t|\t{o}\n")
        except UnicodeEncodeError as e:
            log.error(f"Error hashing {e}")
            continue
            # Certain strings can casues errors, especially emojis encoded in JSON
            # For example, "\ud83d\ude09" is how the smiley gets encoded in JSON (as a Javascript string)
            # If you try to treat this as a UTF-8 string it throws an error.
            # json.loads(r'"\ud83d\ude09"') <- this works
            # "\ud83d\ude09".encode('utf8') <- this throws an error
    for g in all_graphs:
        gg = H(g)
        MAP_OUT_FILE.write(f"{gg}\t|\t{g}\n")

    TRIPLE_OUT_FILE.close()
    MAP_OUT_FILE.close()

    DB_SCHEMA = """
    create table if not exists literals (hash ubigint, value varchar);
    create table if not exists iris (hash ubigint, value varchar);
    create table if not exists triples (s ubigint, p ubigint, o ubigint, g ubigint);    
    """

    db_connection.execute(DB_SCHEMA)
    db_connection.execute(
        rf"insert into triples(s,p,o,g) select ('0x' || column0).lower()::ubigint, ('0x' || column1).lower()::ubigint, ('0x' || column2).lower()::ubigint, ('0x' || column3).lower()::ubigint from read_csv('{TRIPLE_PATH}', delim='\t', header=false)"
    )
    db_connection.execute(
        rf"""insert into literals select ('0x' || column0).lower()::ubigint, ANY_VALUE(column1) from read_csv('{MAP_PATH}', delim='\t|\t', header=false, max_line_size=5100000, quote='') where substr(column1, 1, 1) = '"' group by column0 order by column0 """
    )

    db_connection.execute(
        rf"""insert into iris select ('0x' || column0).lower()::ubigint, ANY_VALUE(column1) from read_csv('{MAP_PATH}', delim='\t|\t', header=false, max_line_size=5100000, quote='') where substr(column1, 1, 1) != '"'  group by column0 order by column0 """
    )
    db_connection.execute(
        f"pragma create_fts_index('literals', 'hash', 'value', stemmer='{stemmer}')"
    )
    db_connection.commit()

    os.unlink(TRIPLE_PATH)
    os.unlink(MAP_PATH)


def build_ftss(stemmer: str = "porter"):
    # For effective searches, the literals should be grouped by entity
    db_connection = DB.cursor()

    db_connection.execute(
        """
CREATE TEMPORARY TABLE temp_fts1 AS
WITH list_values AS (
  SELECT
    s, list_distinct(list(value)) AS value_list
  FROM
    triples T
    JOIN literals L ON T.o = L.hash
  GROUP BY s
),
unnested AS (
  SELECT s, unnest(value_list) AS val FROM list_values
)
SELECT
  s,
  string_agg(val, '\n') AS values
FROM unnested GROUP BY s
"""
    )
    db_connection.execute(
        "CREATE TEMPORARY TABLE temp_fts2 AS SELECT T.s, string_agg(R.values, '\n') AS values FROM triples T JOIN temp_fts1 R ON T.o = R.s  GROUP BY T.s"
    )
    db_connection.execute(
        """
CREATE TABLE fts AS select s, string_agg(values, '\t') AS values 
FROM 
    (SELECT s, values FROM temp_fts1 UNION SELECT s, values FROM temp_fts2) 
GROUP BY s
"""
    )
    db_connection.execute(
        f"pragma create_fts_index('fts', 's', 'values', stemmer='{stemmer}')"
    )
    db_connection.execute("update fts set values = null")
    db_connection.commit()


####################################################################################
####################################################################################
####################################################################################


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

    if p == "" and o.startswith("<"):
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

    for query in opts.get("filters", []):
        op = query.get("op", "should")
        if str(query.get("p")).startswith("fts"):
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
            queries_joined = (
                "create temp table s_results as select distinct QJ.s from ("
                + "\n".join(queries)
                + ") QJ left join s_by_score SS on QJ.s = SS.s order by SS.score desc, QJ.s"
            )
        else:
            queries_joined = (
                "create temp table s_results as select distinct s from ("
                + "\n".join(queries)
                + ") order by s"
            )

        db_cursor.execute(queries_joined)

        # calc the total size based on unique s
        subjects = db_cursor.execute("select s from s_results").df()
        total = subjects.shape[0]
        wanted = subjects[start : start + size]

        # check for aggregates
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

        if wanted.shape[0] > 0:
            s_ids = ", ".join([str(row["s"]) for index, row in wanted.iterrows()])
            s_ids_q = f"select distinct T.s,p,o,g from s_results S join triples T on S.s = T.s where S.s in ({s_ids})"
            triples = db_cursor.execute(s_ids_q).df()

            for index, row in triples.iterrows():
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

            # Fetch the paths
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
select source, path from hier where source in (select s from s_results)
"""
                buf = []
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
