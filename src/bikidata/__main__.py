import os, sys
from .main import build
from .workers import worker_main
import asyncio


def check_suffix(filename):
    for suffix in (".gz", ".nt", ".trig"):
        if filename.endswith(suffix):
            return True
    return False


if sys.argv[1] == "worker":
    if len(sys.argv) > 2:
        try:
            num_workers = int(sys.argv[2])
        except:
            num_workers = 1
    asyncio.run(worker_main(num_workers))
    sys.exit(0)

if check_suffix(sys.argv[1]):
    build([sys.argv[1]])
else:
    filepaths = [
        os.path.join(sys.argv[1], x) for x in os.listdir(sys.argv[1]) if check_suffix(x)
    ]
    build(filepaths)
