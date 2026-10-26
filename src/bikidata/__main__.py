import os, sys
from .main import build
from .workers import worker_main


def check_suffix(filename):
    for suffix in (".gz", ".nt", ".trig"):
        if filename.endswith(suffix):
            return True
    return False


if len(sys.argv) > 1 and sys.argv[1] in ("serve", "server", "run", "sparql"):
    import uvicorn

    from .server import app

    uvicorn.run(
        app,
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8000")),
    )
    sys.exit(0)


if sys.argv[1] == "worker":
    num_workers = 1
    if len(sys.argv) > 2:
        try:
            num_workers = int(sys.argv[2])
        except:
            num_workers = 1
    worker_main(num_workers)
    sys.exit(0)

if check_suffix(sys.argv[1]):
    build([sys.argv[1]])
else:
    filepaths = [
        os.path.join(sys.argv[1], x) for x in os.listdir(sys.argv[1]) if check_suffix(x)
    ]
    build(filepaths)
