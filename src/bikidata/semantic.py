import os, time
import duckdb
from .main import DB_PATH, log, build_ftss
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


def build_semantic(batch_size: int = 96) -> dict:
    # The max batch size in cohere is 96: https://docs.cohere.com/reference/embed
    start_time = time.time()

    DB = duckdb.connect(DB_PATH)
    db_connection = DB.cursor()

    try:
        literals = db_connection.execute("SELECT s, values FROM fts").fetchall()
    except duckdb.CatalogException:
        log.error("Error: fts table not found, now running build_ftss() to create it.")
        build_ftss()
        literals = db_connection.execute("SELECT s, values FROM fts").fetchall()

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
