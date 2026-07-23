import duckdb
import numpy as np
from .main import DB_PATH, log
import igraph as ig
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import os, time

NO_WALKS = 100
MAX_WALK_LENGTH = 15

# How many vertices to hand to the pool per outer-loop iteration, and how
# many of those go to each worker per task. Keeping BATCH_SIZE a healthy
# multiple of the worker count (and CHUNKSIZE small enough to give every
# worker several tasks) keeps all cores fed.
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 9600))
CHUNKSIZE = int(os.getenv("CHUNKSIZE", 100))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", mp.cpu_count()))
log.debug(
    f"Using BATCH_SIZE={BATCH_SIZE} and CHUNKSIZE={CHUNKSIZE} with {MAX_WORKERS} workers"
)


# Built once in the parent process BEFORE the pool is created.
# Forked children inherit these via copy-on-write — no pickling,
# no per-worker duplication.
#
# IMPORTANT: _worker_small_big is a numpy array, not a dict. Indexing into
# a numpy array touches one contiguous C buffer and doesn't mutate Python
# object refcounts, so the OS's copy-on-write pages stay shared across all
# worker processes. A plain Python dict here would get incrementally
# copied (page by page) into every worker process as soon as it's read
# from, because every dict lookup touches per-object refcount fields —
# silently duplicating an 18M+ entry structure up to N-worker times over
# and blowing up memory / cache locality.
_worker_graph = None
_worker_small_big = None


def _init_worker():
    # With fork, _worker_graph/_worker_small_big are already populated
    # by inheritance from the parent — nothing to do here except confirm.
    global _worker_graph, _worker_small_big
    assert _worker_graph is not None, "graph must be built before pool creation"


def t_generate_walks(s):
    walks = set()
    for _ in range(NO_WALKS):
        walk = _worker_graph.random_walk(s, MAX_WALK_LENGTH, return_type="vertices")
        # Bulk-translate the whole walk via numpy fancy indexing instead of
        # a per-node Python-level dict/array lookup loop.
        hashes = _worker_small_big[np.asarray(walk, dtype=np.int64)]
        walks.add(tuple(hashes.tolist()))
    return int(_worker_small_big[s]), [list(w) for w in walks]


def make_random_walks(n_batches=None):
    global _worker_graph, _worker_small_big

    DB = duckdb.connect(DB_PATH)
    cursor = DB.cursor()
    cursor.execute(
        "create table if not exists random_walks (s ubigint, walks ubigint[][])"
    )

    small_big = {}
    big_small = {}
    i = 0
    for row in DB.execute(
        "select hash from iris union select hash from literals order by hash"
    ).fetchall():
        h = row[0]
        big_small[h] = i
        small_big[i] = h
        i += 1
    log.debug(f"Retrieved {len(small_big)} hashes")

    edges = []
    for s, o in DB.execute("select distinct s,o from triples").fetchall():
        edges.append((big_small[s], big_small[o]))
    log.debug(f"Retrieved {len(edges)} edges")

    n_vertices = len(small_big)

    # Build these as globals BEFORE forking workers, so children inherit
    # them via copy-on-write instead of via pickled initargs.
    graph = ig.Graph(n=n_vertices)
    graph.add_edges(edges)
    _worker_graph = graph

    # numpy array instead of dict: see comment on _worker_small_big above.
    small_big_arr = np.zeros(n_vertices, dtype=np.uint64)
    for idx, h in small_big.items():
        small_big_arr[idx] = h
    _worker_small_big = small_big_arr

    # Free the parent's non-shared copies we no longer need directly
    # (graph/small_big are now referenced via the globals above).
    del edges
    del small_big

    # --- Compute the full "still to do" work queue ONCE up front. ---
    # The old approach re-ran a LEFT JOIN of the entire (growing) triples
    # table against the entire (growing) random_walks table on every single
    # batch, to find just 1000 rows. As random_walks filled up this got
    # progressively more expensive — pure bookkeeping overhead on top of
    # the actual walk computation, and it scales with total progress made
    # so far rather than staying constant per batch.
    already_done = {
        row[0] for row in DB.execute("select s from random_walks").fetchall()
    }
    log.debug(f"{len(already_done)} vertices already have walks computed")

    all_s = [row[0] for row in DB.execute("select distinct s from triples").fetchall()]
    log.debug(f"Retrieved {len(all_s)} distinct source vertices")

    todo = [big_small[s] for s in all_s if s not in already_done]
    del all_s
    del already_done
    log.debug(f"{len(todo)} vertices remaining to process")

    ctx = mp.get_context("fork")  # be explicit; don't rely on platform default

    with ProcessPoolExecutor(
        max_workers=MAX_WORKERS,
        mp_context=ctx,
        initializer=_init_worker,
    ) as executor:
        batch = 0
        for batch_start in range(0, len(todo), BATCH_SIZE):
            start_time = time.time()
            vertices_to_process = todo[batch_start : batch_start + BATCH_SIZE]
            if not vertices_to_process:
                break

            results = list(
                executor.map(t_generate_walks, vertices_to_process, chunksize=CHUNKSIZE)
            )

            cursor.executemany(
                "insert into random_walks (s, walks) values (?, CAST(? AS UBIGINT[][]))",
                results,
            )
            cursor.commit()
            end_time = time.time()
            log.debug(f"Batch {batch} processed in {end_time - start_time:.2f} seconds")

            batch += 1
            if n_batches is not None and batch >= n_batches:
                break
