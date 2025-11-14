import os, time, json, random, hashlib, traceback
import xxhash
import duckdb
from .query import query, spo, handle_insert
from .main import DB_PATH, log

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
log.debug("Trying Redis at " + REDIS_HOST)

try:
    import redis.asyncio as redis

    redis_client = redis.Redis(host=REDIS_HOST)
except:
    log.exception(f"Redis at {REDIS_HOST} not available, async queries will not work")

WORKER_FETCH_Q = "bikidata:queries"
WORKER_FETCH_Q_READY = "bikidata:queries_ready"


async def redis_manager():
    log.debug(f"Starting Redis worker manager")
    while True:
        _, serial_query = await redis_client.blpop(WORKER_FETCH_Q)
        query_ticket = None
        try:
            opts = json.loads(serial_query)
            log.debug(f"opts keys {opts.keys()}")
            if opts.get("action") == "insert":
                query_ticket = opts.get("query_ticket")
                log.debug(f"Processing insert action fotr ticket {query_ticket}")
                result = handle_insert(opts)
                if query_ticket:
                    await redis_client.lpush(query_ticket, json.dumps(result))
                continue
            else:
                await redis_client.lpush(WORKER_FETCH_Q_READY, serial_query)
        except:
            if query_ticket:
                await redis_client.lpush(
                    query_ticket,
                    json.dumps(
                        {
                            "error": "Failed to process query",
                            "trace": traceback.format_exc(),
                        }
                    ),
                )
            log.exception("Failed to process query")
            continue


async def redis_worker():
    log.debug(f"Entering worker loop, using Redis and queue {WORKER_FETCH_Q_READY}")
    while True:
        _, serial_query = await redis_client.blpop(WORKER_FETCH_Q_READY)
        opts = json.loads(serial_query)
        query_hash = opts.get("query_hash")
        query_ticket = opts.get("query_ticket")
        if not query_ticket:
            log.error("No query ticket found in query")
            continue
        cached = await redis_client.get(query_hash or "")
        if cached:
            log.debug(f"Cache hit for query ticket {query_ticket}")
            result = json.loads(cached)
        else:
            log.debug(f"Processing query ticket {query_ticket}")
            result = query(opts)
            await redis_client.set(query_hash, json.dumps(result), ex=60 * 60 * 24 * 7)
        await redis_client.lpush(query_ticket, json.dumps(result))


class TimeoutError(Exception):
    pass


async def query_async(opts: dict, timeout: int = 60):
    query_ticket = f"{time.time()}-{random.randint(0,1000000)}"
    query_hash = hashlib.md5(
        json.dumps(opts, sort_keys=True).encode("utf8")
    ).hexdigest()
    opts["query_ticket"] = query_ticket
    opts["query_hash"] = query_hash
    serial_query = json.dumps(opts)
    await redis_client.lpush(WORKER_FETCH_Q, serial_query)
    popresult = await redis_client.blpop(query_ticket, timeout=timeout)
    if popresult is None:
        raise TimeoutError("Query timed out")
    _, result = popresult
    return json.loads(result)


async def insert_async(s: str, p: str, o: str, g: str = "", timeout: int = 60):
    query_ticket = f"{time.time()}-{random.randint(0,1000000)}"
    opts = {
        "action": "insert",
        "data": [{"s": s, "p": p, "o": o, "g": g}],
        "query_ticket": query_ticket,
    }
    serial_query = json.dumps(opts)
    await redis_client.lpush(WORKER_FETCH_Q, serial_query)
    popresult = await redis_client.blpop(query_ticket, timeout=timeout)
    if popresult is None:
        raise TimeoutError("Query timed out")
    _, result = popresult
    return json.loads(result)
