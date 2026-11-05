import os, time, json, urllib.request
import duckdb
from .main import DB_PATH, log

VEC_DIM = int(os.environ.get("VEC_DIM", "1024"))

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL")
OPENAI_EMBEDDING_MODEL = os.environ.get("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
OPENAI_TIMEOUT_SECONDS = float(os.environ.get("OPENAI_TIMEOUT_SECONDS", "60"))


def _embed(texts: list, model: str = OPENAI_EMBEDDING_MODEL) -> list:
    """Embed a list of texts with an OpenAI-compatible /embeddings endpoint,
    for example https://api.openai.com/v1/embeddings or any other compatible provider.
    The host (OPENAI_BASE_URL) and the token (OPENAI_API_KEY) are set from the environment."""
    if not OPENAI_API_KEY:
        raise RuntimeError(
            "OPENAI_API_KEY environment variable is not set. "
            "Set it to the token of the embedding API, and OPENAI_BASE_URL to the host of an "
            "OpenAI-compatible embeddings endpoint."
        )
    url = f"{OPENAI_BASE_URL}/embeddings"
    payload = json.dumps({"model": model, "input": texts}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}",
        },
    )
    with urllib.request.urlopen(request, timeout=OPENAI_TIMEOUT_SECONDS) as response:
        data = json.loads(response.read().decode("utf-8"))
    embeddings = [
        d["embedding"]
        for d in sorted(data.get("data", []), key=lambda d: d.get("index", 0))
    ]
    return embeddings


def get_embedding(text: str) -> list:
    "Embed a single text as a search query embedding, using the OpenAI-compatible API."""
    return _embed([text])[0]


def get_buf_embeddings(buf):
    "Embed a batch of texts, and return the (sid, vec) pairs."""
    embeddings = _embed([text for _, text in buf])
    return [(sid, vec) for (sid, _), vec in zip(buf, embeddings)]


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
    # The max batch size depends on the embedding provider, an OpenAI-compatible
    # endpoint accepts up to 2048 documents per request by default.
    
    if not OPENAI_BASE_URL:
        log.error("Make sure you have the required dependencies installed and the OPENAI_BASE_URL environment variable is set.")
        return 
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
