import os, time, json, random, hashlib, traceback
import xxhash
import duckdb
from .query import query, handle_insert, handle_delete
from .main import log

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
            opts["msg_received_time"] = time.time()
            if opts.get("action") in ("insert", "delete"):
                if opts.get("action") == "insert":
                    result = handle_insert(opts)
                if opts.get("action") == "delete":
                    result = handle_delete(opts)
                query_ticket = opts.get("query_ticket")
                if query_ticket:
                    result["msg_received_time"] = opts["msg_received_time"]
                    result["msg_processed_time"] = time.time()
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
                            "msg_received_time": opts.get("msg_received_time"),
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
        opts["msg_worker_received_time"] = time.time()
        query_hash = opts.get("query_hash")
        query_ticket = opts.get("query_ticket")
        if not query_ticket:
            log.error("No query ticket found in query")
            continue
        if not query_hash:
            log.error("No query hash found in query")
            continue
        use_cache = opts.get("use_cache", True)
        if use_cache:
            cached = await redis_client.get(query_hash or "")
        else:
            cached = None
        if cached:
            log.debug(f"Cache hit for query ticket {query_ticket}")
            result = json.loads(cached)
        else:
            log.debug(f"Processing query ticket {query_ticket}")
            result = query(opts)
            result["msg_processed_time"] = time.time()
            if use_cache:
                await redis_client.set(
                    query_hash, json.dumps(result), ex=60 * 60 * 24 * 7
                )
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
    return await insert_delete_async("insert", s, p, o, g, timeout)


async def delete_async(
    s: str, p: str | None, o: str | None, g: str = "", timeout: int = 60
):
    return await insert_delete_async("delete", s, p, o, g, timeout)


async def insert_delete_async(
    action: str, s: str, p: str, o: str, g: str = "", timeout: int = 60
):
    query_ticket = f"{time.time()}-{random.randint(0,1000000)}"
    opts = {
        "action": action,
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
