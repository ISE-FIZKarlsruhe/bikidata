import duckdb
from gensim.models import Word2Vec
import os
from .main import log

WORKER_COUNT = int(os.getenv("WORKER_COUNT", default=6))
EPOCHS = int(os.getenv("EPOCHS", default=10))
WALK_LIMIT = int(os.getenv("WALK_LIMIT", default=-1))


class WalkYielder:
    def __init__(self, db_path: str = "bikidata.duckdb", limit: int = -1):
        self.db_path = db_path
        self.limit = limit

    def __iter__(self):
        con = duckdb.connect(self.db_path, read_only=True)
        must_continue = True
        result = con.execute(
            f"SELECT UNNEST(walks) AS walk FROM random_walks{' LIMIT ' + str(self.limit) if self.limit > 0 else ''}"
        )
        while must_continue:
            row = result.fetchone()
            if row is None:
                break
            yield list(filter(None, [int(x) for x in row[0]]))


def make_rdf2vec_model(model_path: str = "bikidata_word2vec.model"):
    model = Word2Vec(
        sentences=WalkYielder(limit=WALK_LIMIT),
        vector_size=100,  # embedding dimensionality
        window=5,  # context window size
        min_count=1,  # keep all ids, even rare ones
        sg=1,  # skip-gram (use sg=0 for CBOW)
        workers=WORKER_COUNT,
        epochs=EPOCHS,
    )
    model.save(model_path)

    return model


def get_rdf2vec_model(model_path: str = "bikidata_word2vec.model"):
    if os.path.exists(model_path):
        return Word2Vec.load(model_path)
    else:
        log.warning(f"Model not found at {model_path}, creating a new one.")
        return make_rdf2vec_model(model_path)
