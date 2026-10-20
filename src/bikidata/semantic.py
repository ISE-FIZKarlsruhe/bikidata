import os, time
import duckdb
from .main import DB_PATH, log
import cohere

VEC_DIM = 1024

COHERE_API_KEY = os.environ.get("COHERE_API_KEY")
if not COHERE_API_KEY:
    log.error("COHERE_API_KEY environment variable is not set. ")
else:
    co = cohere.ClientV2(COHERE_API_KEY)


def get_embedding(text: str) -> list:
    doc_emb = co.embed(
        model="embed-v4.0",
        input_type="search_query",
        texts=[text],
        max_tokens=8000,
        truncate="END",
        output_dimension=VEC_DIM,
        embedding_types=["float"],
    ).embeddings.float
    return doc_emb[0]


def get_buf_embeddings(buf):
    doc_emb = co.embed(
        model="embed-v4.0",
        input_type="search_document",
        texts=[text for _, text in buf],
        max_tokens=8000,
        truncate="END",
        output_dimension=VEC_DIM,
        embedding_types=["float"],
    ).embeddings.float
    return [(sid, vec) for (sid, _), vec in zip(buf, doc_emb)]


def build_entity_texts(db_connection):
    """Create the table entity_texts(s, values) with the literals of each entity,
    and of the entities it links to, as the input for the embeddings."""
    db_connection.execute(
        """
CREATE TEMPORARY TABLE temp_texts1 AS
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
        "CREATE TEMPORARY TABLE temp_texts2 AS SELECT T.s, string_agg(R.values, '\n') AS values FROM triples T JOIN temp_texts1 R ON T.o = R.s  GROUP BY T.s"
    )
    db_connection.execute(
        """
CREATE TABLE entity_texts AS select s, string_agg(values, '\t') AS values
FROM
    (SELECT s, values FROM temp_texts1 UNION SELECT s, values FROM temp_texts2)
GROUP BY s
"""
    )
    db_connection.commit()


def build_semantic(batch_size: int = 96) -> dict:
    # The max batch size in cohere is 96: https://docs.cohere.com/reference/embed
    start_time = time.time()

    DB = duckdb.connect(DB_PATH)
    db_connection = DB.cursor()

    try:
        literals = db_connection.execute("SELECT s, values FROM entity_texts").fetchall()
    except duckdb.CatalogException:
        log.debug("entity_texts table not found, creating it.")
        build_entity_texts(db_connection)
        literals = db_connection.execute("SELECT s, values FROM entity_texts").fetchall()

    db_connection.execute(
        f"CREATE TABLE IF NOT EXISTS literals_semantic (hash ubigint, vec FLOAT[{VEC_DIM}]);"
    )
    buf = []
    log.debug(
        f"Starting semantic index build for {len(literals)} items with batch size of {batch_size}"
    )
    idx = 0
    for sid, values in literals:
        idx += 1
        if not values:
            continue
        buf.append((sid, values))
        if len(buf) >= batch_size:
            progress = int((idx / len(literals)) * 100)
            duration = time.time() - start_time
            tps = int(idx / duration)
            log.debug(f"Now inserting {len(buf)} literals, at {progress}% {tps} tps")
            db_connection.executemany(
                "INSERT INTO literals_semantic (hash, vec) VALUES (?, ?)",
                get_buf_embeddings(buf),
            )
            buf = []

    if buf:
        log.debug(f"Now inserting final {len(buf)}")
        db_connection.executemany(
            "INSERT INTO literals_semantic (hash, vec) VALUES (?, ?)",
            get_buf_embeddings(buf),
        )
    db_connection.commit()
    end_time = time.time()
    return {"duration": int(end_time - start_time), "count": idx}
