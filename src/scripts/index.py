import sys, time, logging, gzip, bz2, os
import xxhash
import traceback
import multiprocessing as mp

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

WORKER_COUNT = 4
MAX_COUNT = int(os.getenv("MAX_COUNT", 0))
DURATION_LIMIT = 30  # in seconds
BUF_LIMIT = 500000
DUMP_SIZE = 8_233_335_182  # this varies depending on the dump
OUT_PATH = "raw"


def worker(number, Q):
    worker_count = 0
    chunk_time = time.time() - DURATION_LIMIT
    start_time = time.time()
    with open(
        os.path.join(OUT_PATH, f"out-{str(number).zfill(2)}.csv"), "wt"
    ) as OUTFILE:
        while True:
            line = Q.get()
            if not line:
                break
            line = line.strip()
            if not line.endswith(" ."):
                continue
            line = line[:-2]
            parts = line.split(" ")
            if len(parts) < 3:
                continue
            s = parts[0]
            p = parts[1]
            o = " ".join(parts[2:])
            OUTFILE.write(
                f"{xxhash.xxh64(s).intdigest()},{xxhash.xxh64(p).intdigest()},{xxhash.xxh64(o).intdigest()} \n"
            )
            worker_count += 1
            loop_time = time.time()
            if loop_time - chunk_time > DURATION_LIMIT:
                lps = int(worker_count / (loop_time - start_time))
                logging.debug(f"Worker#{number} at {worker_count} doing {lps} lps")
                chunk_time = time.time()
    logging.debug(f"Worker: {number} done")


def main():
    workers = []
    Q = mp.Queue()
    for w in range(WORKER_COUNT):
        wp = mp.Process(target=worker, daemon=True, args=(w, Q))
        wp.start()
        workers.append(wp)

    start_time = time.time()
    chunk_time = start_time - DURATION_LIMIT

    INPUT_FILE = sys.argv[1]
    if INPUT_FILE.endswith(".gz"):
        F = gzip.open(INPUT_FILE, "rt")
    elif INPUT_FILE.endswith(".bz2"):
        F = bz2.open(INPUT_FILE, "rt")
    else:
        F = open(INPUT_FILE, "rt")

    line = True
    line_no = 0
    with F:
        while line:
            line_no += 1
            try:
                line = F.readline()
            except:
                print("###### ", line_no)
                traceback.print_exc()
                continue
            if not line:
                break
            if MAX_COUNT and line_no > MAX_COUNT:
                logging.debug(f"MAX_COUNT {MAX_COUNT} reached, stopping")
                break

            Q.put(line)

            loop_time = time.time()
            if loop_time - chunk_time > DURATION_LIMIT:
                lps = line_no / (loop_time - start_time)
                estimate = DUMP_SIZE / lps
                print(
                    time.ctime(),
                    "line: ",
                    line_no,
                    "lps: ",
                    int(lps),
                    "eta: ",
                    time.ctime(start_time + estimate),
                )
                chunk_time = time.time()

    for x in range(WORKER_COUNT):
        Q.put(None)
    logging.debug("Done filling Q, waiting for workers to finish")

    for w in workers:
        w.join()


if __name__ == "__main__":
    main()
