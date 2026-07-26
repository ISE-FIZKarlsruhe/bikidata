"""Random walk generation without igraph.

This mirrors the Go rewrite's design as closely as Python allows:

  - Graph:      CSR adjacency (offsets + neighbors numpy arrays) built from
                DuckDB query results, instead of an igraph.Graph object.
  - Walk step:  fully vectorized with numpy — every walk for a chunk of
                vertices is advanced one step at a time *all at once* via
                array operations, instead of a Python-level loop over
                (vertex, walk-number, step).
  - Parallelism: ProcessPoolExecutor (real OS processes/cores), exactly
                like Go's goroutines-across-cores — see the note below on
                why plain asyncio can't do this part by itself.
  - asyncio:    orchestrates submitting work to the process pool without
                manual callback/queue plumbing, AND pipelines the DB write
                of batch N with the computation of batch N+1, so the two
                overlap instead of running strictly back-to-back.

IMPORTANT HONESTY NOTE ABOUT asyncio's ROLE HERE:
asyncio's event loop is single-threaded cooperative concurrency. It cannot,
by itself, run Python/numpy code on multiple CPU cores — that would still
be true even if every line here were rewritten as `async def`. The actual
cross-core parallelism comes from ProcessPoolExecutor, i.e. real separate
OS processes, the same way the Go version gets parallelism from goroutines
scheduled across cores. What asyncio buys us is:
  1. A clean way to fan work out to the process pool and await the results
     (`loop.run_in_executor`) without hand-rolling a queue/callback system.
  2. Overlapping I/O (the DuckDB insert of the previous batch) with CPU
     work (the process pool computing the next batch), via
     `asyncio.create_task` + deferred `await`, instead of blocking on the
     write before starting the next batch's computation.
If you removed the ProcessPoolExecutor and ran the numpy kernel on asyncio
alone, it would run on a single core, no faster than plain synchronous code.
"""

import time
import asyncio
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor

import duckdb
import numpy as np

from .main import DB_PATH, log

NO_WALKS = 100
MAX_WALK_LENGTH = 15

# Vertices processed per outer batch (progress logging / DB flush
# granularity) — analogous to Go's BATCH_SIZE.
BATCH_SIZE = 9600

# How many process-pool tasks to aim for per worker per batch. Chunk size
# is derived from this at call time (see make_random_walks_async), rather
# than being a fixed constant — a fixed constant silently starves workers
# if BATCH_SIZE is ever changed (this bit us once already; see the Go
# TASKS_PER_WORKER comment for the same lesson).
TASKS_PER_WORKER = 4

# Populated in the parent process BEFORE the pool is forked, so children
# inherit them via copy-on-write. All three are numpy arrays, not dicts —
# reading a numpy array never touches per-element Python object refcounts,
# so the OS's COW pages stay genuinely shared across worker processes. A
# plain dict here would get incrementally copied into every worker as soon
# as it's read from (this is the same lesson from the earlier Python
# multiprocessing fix: dict reads mutate refcounts, array reads don't).
_offsets = None
_neighbors = None
_small_big = None


def _init_worker():
    assert _offsets is not None, "graph must be built before pool creation"


def _generate_walks_chunk(vertex_chunk: np.ndarray):
    """Runs in a worker process. Generates NO_WALKS random walks for every
    vertex in vertex_chunk *simultaneously* via numpy array operations —
    there is no per-vertex/per-walk Python loop for the actual stepping,
    only a loop over the (small, fixed) MAX_WALK_LENGTH steps, each of
    which advances every still-alive walk in the chunk in one shot.
    """
    offsets = _offsets
    neighbors = _neighbors
    small_big = _small_big

    n_v = vertex_chunk.shape[0]
    w = n_v * NO_WALKS  # total number of walks being advanced this call

    current = np.repeat(vertex_chunk, NO_WALKS)  # (w,) current position of each walk
    walk_matrix = np.empty((w, MAX_WALK_LENGTH + 1), dtype=np.int32)
    walk_matrix[:, 0] = current
    length = np.ones(
        w, dtype=np.int16
    )  # how many vertices are filled in per row so far
    alive = np.ones(w, dtype=bool)

    rng = np.random.default_rng()

    for step in range(MAX_WALK_LENGTH):
        active_idx = np.flatnonzero(alive)
        if active_idx.size == 0:
            break  # every walk in this chunk hit a dead end already

        cur_active = current[active_idx]
        deg = offsets[cur_active + 1] - offsets[cur_active]

        dead_mask = deg == 0
        if dead_mask.any():
            # Matches igraph's behavior: a walk with nowhere to go just
            # stops (ends up shorter than MAX_WALK_LENGTH + 1 vertices).
            alive[active_idx[dead_mask]] = False

        live_mask = ~dead_mask
        if not live_mask.any():
            continue

        live_idx = active_idx[live_mask]
        live_cur = cur_active[live_mask]
        live_deg = deg[live_mask]

        # Uniform random neighbor index per row in one vectorized call
        # (equivalent to random.randrange(deg) per row, but batched).
        rnd = (rng.random(live_idx.size) * live_deg).astype(np.int64)
        rnd = np.minimum(
            rnd, live_deg - 1
        )  # defensive clip against fp rounding at the edge

        nxt = neighbors[offsets[live_cur] + rnd]
        current[live_idx] = nxt
        walk_matrix[live_idx, step + 1] = nxt
        length[live_idx] += 1

    # Translate every entry in the whole matrix to real hashes in one
    # vectorized fancy-index call, rather than per-node in a loop.
    hash_matrix = small_big[walk_matrix]

    # Per-vertex dedup (mirrors the Python set() of walk tuples in the
    # original script). This part IS a Python-level loop — tuple hashing
    # for dedup isn't something numpy vectorizes cleanly — but it's now
    # split across worker processes like everything else, and it's the
    # only remaining per-row Python work instead of the whole walk.
    results = []
    for i in range(n_v):
        row_start = i * NO_WALKS
        seen = set()
        unique = []
        for j in range(NO_WALKS):
            r = row_start + j
            walk = tuple(hash_matrix[r, : length[r]].tolist())
            if walk not in seen:
                seen.add(walk)
                unique.append(list(walk))
        results.append((int(small_big[vertex_chunk[i]]), unique))
    return results


async def _write_batch(cursor, results):
    """Runs the (synchronous) DuckDB insert in a worker thread so the event
    loop stays free to keep driving the process pool for the next batch
    while this insert is in flight."""

    def _do_insert():
        cursor.executemany(
            "insert into random_walks (s, walks) values (?, CAST(? AS UBIGINT[][]))",
            results,
        )
        cursor.commit()

    await asyncio.to_thread(_do_insert)


async def make_random_walks_async(n_batches=None):
    global _offsets, _neighbors, _small_big

    DB = duckdb.connect(DB_PATH)
    cursor = DB.cursor()
    cursor.execute(
        "create table if not exists random_walks (s ubigint, walks ubigint[][])"
    )

    # --- 1. vertex hash <-> small id mapping ---
    log.debug("loading vertex hashes...")
    small_big = (
        DB.execute(
            "select hash from iris union select hash from literals order by hash"
        )
        .fetchnumpy()["hash"]
        .astype(np.uint64)
    )
    # big_small is only ever used here in the parent process, during setup,
    # to translate hashes to small ids before the pool is forked — it never
    # needs to be touched by a worker, so it isn't stored in a global and
    # carries none of the COW-duplication risk a dict would have if workers
    # read from it.
    big_small = {int(h): i for i, h in enumerate(small_big.tolist())}
    n_vertices = small_big.shape[0]
    log.debug(f"retrieved {n_vertices} hashes")

    # --- 2. edges -> CSR adjacency, vectorized with numpy ---
    log.debug("loading edges...")
    edge_rows = DB.execute("select distinct s, o from triples").fetchnumpy()
    edge_s = np.fromiter(
        (big_small[int(h)] for h in edge_rows["s"]),
        dtype=np.int32,
        count=len(edge_rows["s"]),
    )
    edge_o = np.fromiter(
        (big_small[int(h)] for h in edge_rows["o"]),
        dtype=np.int32,
        count=len(edge_rows["o"]),
    )
    log.debug(f"retrieved {edge_s.shape[0]} edges")

    log.debug("building CSR adjacency structure...")
    # Undirected, matching igraph's default Graph() behavior (as in the Go
    # rewrite): each row becomes a neighbor entry on BOTH endpoints,
    # including duplicate parallel edges if a pair recurs.
    all_from = np.concatenate([edge_s, edge_o])
    all_to = np.concatenate([edge_o, edge_s])
    # Stable sort by source groups every vertex's neighbors together in one
    # pass — the numpy-only way to build CSR without a manual scatter loop.
    order = np.argsort(all_from, kind="stable")
    neighbors = all_to[order]
    degree = np.bincount(all_from, minlength=n_vertices)
    offsets = np.zeros(n_vertices + 1, dtype=np.int64)
    np.cumsum(degree, out=offsets[1:])
    del all_from, all_to, order, degree, edge_s, edge_o

    _offsets = offsets
    _neighbors = neighbors
    _small_big = small_big
    log.debug(
        f"adjacency structure built: {n_vertices} vertices, {neighbors.shape[0]} neighbor entries"
    )

    # --- 3. one-time work queue (computed once, not re-derived per batch) ---
    log.debug("computing work queue...")
    already_done = set(
        int(s) for s in DB.execute("select s from random_walks").fetchnumpy()["s"]
    )
    all_s = DB.execute("select distinct s from triples").fetchnumpy()["s"]
    todo = np.fromiter(
        (big_small[int(s)] for s in all_s if int(s) not in already_done),
        dtype=np.int32,
    )
    del already_done, all_s, big_small
    log.debug(
        f"{todo.shape[0]} vertices remaining to process, {todo.shape[0] // BATCH_SIZE} batches at BATCH_SIZE={BATCH_SIZE}"
    )

    ctx = mp.get_context("fork")
    n_workers = mp.cpu_count()
    loop = asyncio.get_running_loop()

    with ProcessPoolExecutor(
        max_workers=n_workers, mp_context=ctx, initializer=_init_worker
    ) as executor:
        batch = 0
        prev_write_task = None

        for batch_start in range(0, todo.shape[0], BATCH_SIZE):
            batch_vertices = todo[batch_start : batch_start + BATCH_SIZE]
            if batch_vertices.size == 0:
                break

            start_time = time.time()

            chunk_size = max(
                1, batch_vertices.shape[0] // (n_workers * TASKS_PER_WORKER)
            )
            chunks = [
                batch_vertices[i : i + chunk_size]
                for i in range(0, batch_vertices.shape[0], chunk_size)
            ]
            futures = [
                loop.run_in_executor(executor, _generate_walks_chunk, chunk)
                for chunk in chunks
            ]
            chunk_results = await asyncio.gather(*futures)
            results = [row for chunk_result in chunk_results for row in chunk_result]

            # Pipelining: make sure the PREVIOUS batch's write finished
            # before reusing the cursor, but don't wait for the write we're
            # about to start — it runs in the background (asyncio.to_thread)
            # while the next iteration's process-pool futures are already
            # computing. In practice, since computation dominates over the
            # insert, this previous-write await returns almost immediately.
            if prev_write_task is not None:
                await prev_write_task
            prev_write_task = asyncio.create_task(_write_batch(cursor, results))

            elapsed = time.time() - start_time
            log.debug(
                f"Batch {batch} processed in {elapsed:.2f} seconds, {elapsed / BATCH_SIZE:.4f} seconds per vertex"
            )

            batch += 1
            if n_batches is not None and batch >= n_batches:
                break

        if prev_write_task is not None:
            await prev_write_task


def make_random_walks(n_batches=None):
    """Synchronous entry point — drop-in replacement for the original
    igraph-based function, for callers that don't run their own event loop."""
    asyncio.run(make_random_walks_async(n_batches))
