import sys, logging, gzip, re, os, time
import duckdb
import xxhash

DEBUG = os.environ.get("DEBUG", "1") == "1"

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
        build_from_iterator(iterator, stemmer)
    else:
        error = "No triples to index, triplefile_paths length < 1"
        log.error(error)
        return {"duration": 0, "error": error}


def build_from_iterator(iterator, stemmer: str = "porter"):

    start_time = time.time()

    DB = duckdb.connect(DB_PATH)
    db_connection = DB.cursor()
    try:
        triple_count = db_connection.execute("select count(*) from triples").fetchall()
        if triple_count[0][0] > 0:
            error = f"The database [{DB_PATH}] already has data, doing nothing"
            log.debug(error)
            return {"duration": 0, "error": error}

    except duckdb.CatalogException:
        log.debug("Good, there are no triples in bikidate table yet")

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
            count += 1
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
    end_time = time.time()
    return {"duration": int(end_time - start_time), "count": count}


def build_ftss(stemmer: str = "porter"):
    # For effective searches, the literals should be grouped by entity
    start_time = time.time()

    DB = duckdb.connect(DB_PATH)
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
    db_connection.commit()
    end_time = time.time()
    return {"duration": int(end_time - start_time)}
