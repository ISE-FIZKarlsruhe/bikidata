from .semantic import get_embedding, VEC_DIM
import xxhash
from .main import DB_PATH, log
import duckdb


def raw():
    DB = duckdb.connect(DB_PATH, read_only=True)
    return DB.cursor()


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
