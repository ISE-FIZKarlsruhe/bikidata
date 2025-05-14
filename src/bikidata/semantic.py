import time
import platform
from transformers import AutoTokenizer, AutoModel
import torch
import torch.nn.functional as F
import duckdb
from .main import DB_PATH, log

MAX_LENGTH = 8192


class EuroBERTEmbedder:
    def __init__(self, model_id: str = "EuroBERT/EuroBERT-210m"):
        is_macos = platform.system() == "Darwin"
        if is_macos and hasattr(torch, "mps") and torch.backends.mps.is_available():
            device = torch.device("mps")
        elif torch.cuda.is_available():
            device = torch.device("cuda")
        else:
            device = torch.device("cpu")

        self.device = device
        self.tokenizer = AutoTokenizer.from_pretrained(model_id)
        self.model = AutoModel.from_pretrained(model_id, trust_remote_code=True).to(
            self.device
        )

    def get_embeddings(
        self,
        text: str,
    ) -> list[float]:
        inputs = self.tokenizer(
            text,
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=MAX_LENGTH,
        )
        inputs = {k: v.to(self.device) for k, v in inputs.items()}
        with torch.no_grad():
            outputs = self.model(**inputs)

        token_embeddings = outputs.last_hidden_state
        cls_embedding = token_embeddings[:, 0]  # Shape: [batch_size, hidden_size]
        normalized_cls_embedding = F.normalize(cls_embedding, p=2, dim=1)

        return normalized_cls_embedding[0].tolist()


embedder = EuroBERTEmbedder()


def build_semantic():
    BATCH_SIZE = 1000
    start_time = time.time()

    DB = duckdb.connect(DB_PATH)
    db_connection = DB.cursor()

    db_connection.execute(
        "CREATE TABLE IF NOT EXISTS literals_semantic (hash ubigint, vec FLOAT[768]);"
    )
    literals = db_connection.execute("SELECT hash, value FROM literals")
    buf = []
    log.debug(f"Starting semantic build with batch size of {BATCH_SIZE}")
    for hash, value in literals.fetchall():
        if not value:
            continue
        try:
            embedding = embedder.get_embeddings(value)
            buf.append((hash, embedding))
        except Exception as e:
            log.error(f"Error processing value: {value}, error: {e}")
            continue
        if len(buf) >= BATCH_SIZE:
            log.debug(f"Now inserting {len(buf)} literals")
            db_connection.executemany(
                "INSERT INTO literals_semantic (hash, vec) VALUES (?, ?)",
                buf,
            )
            buf = []

    db_connection.commit()
    end_time = time.time()
    return {"duration": int(end_time - start_time)}
