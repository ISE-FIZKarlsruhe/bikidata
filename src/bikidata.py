import sys, logging, gzip, re, os
import duckdb
import xxhash

log = logging.getLogger("bikidata")
handler = logging.StreamHandler()
log.addHandler(handler)
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


def build_bikidata(
    triplefile_paths: list,
):
    if len(triplefile_paths) > 0:
        log.debug(f"Building Bikidata index with {triplefile_paths}")
        iterator = read_nt(triplefile_paths)
    else:
        log.error("No triples to index, triplefile_paths not given")
        return

    if os.path.exists(DB_PATH):
        log.error(f"The database {DB_PATH} already exists")
        return

    TRIPLE_PATH = os.getenv("BIKIDATA_TRIPLE_PATH", "triples")
    MAP_PATH = os.getenv("BIKIDATA_MAP_PATH", "maps")

    count = 0

    TRIPLE_OUT_FILE = open(TRIPLE_PATH, "w")
    MAP_OUT_FILE = open(MAP_PATH, "w")

    all_graphs = set()
    for s, p, o, g in iterator:
        ss = H(s)
        pp = H(p)
        oo = H(o)
        gg = H(g)
        all_graphs.add(g)
        TRIPLE_OUT_FILE.write(f"{ss}\t{pp}\t{oo}\t{gg}\n")
        MAP_OUT_FILE.write(f"{ss}\t|\t{s}\n")
        MAP_OUT_FILE.write(f"{pp}\t|\t{p}\n")
        MAP_OUT_FILE.write(f"{oo}\t|\t{o}\n")
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

    DB = duckdb.connect(DB_PATH)
    DB.execute(DB_SCHEMA)
    DB.execute(
        rf"insert into triples(s,p,o,g) select ('0x' || column0).lower()::ubigint, ('0x' || column1).lower()::ubigint, ('0x' || column2).lower()::ubigint, ('0x' || column3).lower()::ubigint from read_csv('{TRIPLE_PATH}', delim='\t', header=false)"
    )
    DB.execute(
        rf"""insert into literals select ('0x' || column0).lower()::ubigint, ANY_VALUE(column1) from read_csv('{MAP_PATH}', delim='\t|\t', header=false, max_line_size=5100000) where substr(column1, 1, 1) = '"' group by column0 order by column0 """
    )

    DB.execute(
        rf"""insert into iris select ('0x' || column0).lower()::ubigint, ANY_VALUE(column1) from read_csv('{MAP_PATH}', delim='\t|\t', header=false, max_line_size=5100000) where substr(column1, 1, 1) != '"'  group by column0 order by column0 """
    )
    DB.execute("pragma create_fts_index('literals', 'hash', 'value')")
    DB.commit()

    os.unlink(TRIPLE_PATH)
    os.unlink(MAP_PATH)


def q_to_sql(query: dict):
    p = str(query.get("p", "")).strip(" ")
    o = str(query.get("o", "")).strip(" ")
    g = str(query.get("g", "")).strip(" ")
    pp = xxhash.xxh64_hexdigest(p).lower()
    oo = xxhash.xxh64_hexdigest(o).lower()
    gg = [xxhash.xxh64_hexdigest(g_).lower() for g_ in g.split(" ")]

    extra_g = ""
    if g != "":
        g_hashes = ", ".join([f"'0x{ggg}'::ubigint" for ggg in gg])
        extra_g = f" and T0.g in ({g_hashes})"

    if p == "" and o.startswith("<"):
        return (
            f"(select distinct s from triples T0 where o = '0x{oo}'::ubigint {extra_g})"
        )
    elif p == "id":
        if o.startswith("random") or o.startswith("sample"):
            o_split = o.split(" ")
            if len(o_split) > 1:
                o = o_split[0]
                try:
                    o_count = int(o_split[1])
                except ValueError:
                    o_count = 1
            f"(select distinct s from triples using sample {o_count} {extra_g})"
        return f"(select distinct s from triples where s = '0x{oo}'::ubigint {extra_g})"

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
select distinct T{parents}.s from (select * from scored where score is not null) S join triples T0
on S.hash = T0.o
"""

        return psql + "\n  " + extra + f"{extra_g} order by score desc)"
    elif p[0] == "<" and p[-1] == ">":
        if o:
            return f"(select distinct s from triples T0 where p = '0x{pp}'::ubigint and o = '0x{oo}'::ubigint {extra_g})"

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
    for query in opts.get("filters", []):
        op = query.get("op", "should")
        if not queries:
            queries = [q_to_sql(query)]
        else:
            if op in ("should", "or"):
                queries.append(" UNION " + q_to_sql(query))
            elif op in ("must", "and"):
                queries.append(" INTERSECT " + q_to_sql(query))
            elif op == "not":
                queries.append(" EXCEPT " + q_to_sql(query))

    total = 0
    tofetch = set()
    results = {}
    aggregates = {}

    if len(queries) > 0:
        # calc the total size based on unique s
        queries_joined = "\n".join(queries)
        subjects = DB.execute(queries_joined).df()
        total = subjects.shape[0]

        # check for aggregates

        for agg in opts.get("aggregates", []):
            if agg == "graphs":
                tmp = f"select distinct count(g) as count, I.value as val from triples T join iris I on T.g = I.hash where s in ({queries_joined}) group by g, I.value"
            elif agg == "properties":
                tmp = f"select count(p) as count, I.value as val from triples T join iris I on T.p = I.hash where s in ({queries_joined}) group by p, I.value"
            else:
                agg_o = xxhash.xxh64_hexdigest(str(agg)).lower()

                tmp = f"(select distinct count(s) as count, I.value as val from triples T join iris I on T.o = I.hash where p = '0x{agg_o}'::ubigint and s in ({queries_joined}) group by o, I.value) union (select distinct count(s) as count, L.value as val from triples T join literals L on T.o = L.hash where p = '0x{agg_o}'::ubigint and s in ({queries_joined}) group by o, L.value) order by count desc"
            aggs = DB.execute(tmp).df()
            aggregates[agg] = aggs

        # get the batch based on size and start
        wanted = subjects[start : start + size]
        if wanted.shape[0] > 0:
            s_ids = ", ".join([str(row["s"]) for index, row in wanted.iterrows()])
            triples = DB.execute(
                f"select distinct s,p,o,g from triples  where s in ({s_ids})"
            ).df()

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
select source, path from hier where source in ({s_ids})
"""
                buf = []
                for _, row in DB.execute(padsql).df().iterrows():
                    padr_s = row.get("source")
                    results.setdefault(padr_s, {}).setdefault("_paths", {})
                    results[padr_s]["_paths"][pad] = list(row.get("path"))
                    for x in row.get("path"):
                        tofetch.add(x)

    # Special aggregates
    if "properties" in opts.get("aggregates", []) and len(queries) < 1:
        aggregates["properties"] = DB.execute(
            "select count(p) as count, I.value as val from triples T join iris I on T.p = I.hash group by p, I.value"
        ).df()
    if "graphs" in opts.get("aggregates", []) and len(queries) < 1:
        aggregates["graphs"] = DB.execute(
            "select count(g) as count, I.value as val from triples T join iris I on T.g = I.hash group by g, I.value"
        ).df()

    if len(tofetch) > 0:
        tofetch = ", ".join([str(x) for x in tofetch])

        HV = dict(
            [
                (hash, value)
                for hash, value in DB.execute(
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
        if HV.get(entity) not in results_mapped:
            print("foo!")
        graph = results_mapped[HV.get(entity)].get("graph", [])
        results_mapped[HV.get(entity)]["graph"] = list(graph)
        if "_paths" in fields:
            for path, vals in fields["_paths"].items():
                vals = [HV.get(val) for val in vals]
                results_mapped[HV.get(entity)].setdefault("_paths", {})[path] = vals

    if "dump" in opts:
        with open(opts["dump"], "w") as DUMPFILE:
            for entity, fields in results_mapped.items():
                for field, vals in fields.items():
                    for val in vals:
                        DUMPFILE.write(f"{entity} {field} {val} .\n")

    aggregates_mapped = {}
    for agg, aggs in aggregates.items():
        aggregates_mapped[agg] = []
        for _, row in aggs.iterrows():
            aggregates_mapped[agg].append((row["count"], row["val"]))

    back = {"results": results_mapped, "total": total, "size": size, "start": start}
    if aggregates:
        back["aggregates"] = aggregates_mapped

    return back


def check_suffix(filename):
    for suffix in (".gz", ".nt", ".trig"):
        if filename.endswith(suffix):
            return True
    return False


if __name__ == "__main__":
    if check_suffix(sys.argv[1]):
        build_bikidata([sys.argv[1]])
    else:
        filepaths = [
            os.path.join(sys.argv[1], x)
            for x in os.listdir(sys.argv[1])
            if check_suffix(x)
        ]
        build_bikidata(filepaths)
