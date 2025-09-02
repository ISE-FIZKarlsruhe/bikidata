import os, sys
from .main import build
from .query import redis_worker
import asyncio


def check_suffix(filename):
    for suffix in (".gz", ".nt", ".trig"):
        if filename.endswith(suffix):
            return True
    return False


if sys.argv[1] == "worker":
    asyncio.run(redis_worker())
    sys.exit(0)

if check_suffix(sys.argv[1]):
    build([sys.argv[1]])
else:
    filepaths = [
        os.path.join(sys.argv[1], x) for x in os.listdir(sys.argv[1]) if check_suffix(x)
    ]
    build(filepaths)
