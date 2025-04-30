import os, sys
from .main import DB, build


def check_suffix(filename):
    for suffix in (".gz", ".nt", ".trig"):
        if filename.endswith(suffix):
            return True
    return False


if check_suffix(sys.argv[1]):
    build([sys.argv[1]], DB.cursor())
else:
    filepaths = [
        os.path.join(sys.argv[1], x) for x in os.listdir(sys.argv[1]) if check_suffix(x)
    ]
    build(filepaths, DB.cursor())
