import duckdb
import numpy as np
from .main import DB_PATH, log
import igraph as ig
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import time

NO_WALKS = 100
MAX_WALK_LENGTH = 15

# Populated once per worker process by _init_worker, not passed per-task.
_worker_graph = None
_worker_small_big = None


def _init_worker(n_vertices, edges, small_big):
    global _worker_graph, _worker_small_big
    graph = ig.Graph(n=n_vertices)
    graph.add_edges(edges)
    _worker_graph = graph
    _worker_small_big = small_big


def t_generate_walks(s):
    walks = set()
    for _ in range(NO_WALKS):
        walk = _worker_graph.random_walk(s, MAX_WALK_LENGTH, return_type="vertices")
        walks.add(tuple(_worker_small_big[node] for node in walk))
    return _worker_small_big[s], [list(w) for w in walks]


def make_random_walks(n_batches=None):
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

    with ProcessPoolExecutor(
        max_workers=mp.cpu_count(),
        initializer=_init_worker,
        initargs=(n_vertices, edges, small_big),
    ) as executor:
        batch = 0
        while True:
            start_time = time.time()
            vertices_to_process = [
                big_small[row[0]]
                for row in DB.execute(
                    "select distinct(T.s) from triples T left join random_walks R on T.s = R.s where R.s is null limit 1000"
                ).fetchall()
            ]
            if not vertices_to_process:
                break

            results = list(
                executor.map(t_generate_walks, vertices_to_process, chunksize=25)
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
