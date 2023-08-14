import sys, time
import xxhash
import pandas as pd
import fastparquet
import traceback
from rbloom import Bloom

# The file has been split on linecount using split -u --lines=500000000
LINECOUNT = 500000000
DURATION_LIMIT = 10  # in seconds
BUF_LIMIT = 1000000

DF_COLUMNS = ["s", "p", "o"]

bf = Bloom(LINECOUNT, 0.01)


def add_if_not_seen(buf, s, p, o):
    val = f"{s}{p}{o}"
    if val in bf:
        return
    buf.append([s, p, o])
    bf.add(val)


def add_buf(df, buf):
    new_row = pd.DataFrame(
        buf,
        columns=DF_COLUMNS,
    )
    return pd.concat([df, new_row], ignore_index=True), []


def main():
    start_time = time.time()
    chunk_time = start_time
    df = pd.DataFrame(columns=DF_COLUMNS, dtype="uint64")
    with open(sys.argv[1]) as F:
        buf = []
        for line_no in range(LINECOUNT):
            line = F.readline().strip()
            if not line.endswith(" ."):
                continue
            line = line[:-2]
            parts = line.split(" ")
            if len(parts) < 3:
                continue
            s = parts[0]
            p = parts[1]
            o = " ".join(parts[2:])
            add_if_not_seen(
                buf,
                xxhash.xxh64(s).intdigest(),
                xxhash.xxh64(p).intdigest(),
                xxhash.xxh64(o).intdigest(),
            )
            if len(buf) > BUF_LIMIT:
                df, buf = add_buf(df, buf)

            loop_time = time.time()
            if loop_time - chunk_time > DURATION_LIMIT:
                lps = line_no / (loop_time - start_time)
                estimate = LINECOUNT / lps
                print(
                    time.ctime(),
                    sys.argv[1],
                    line_no,
                    int(lps),
                    time.ctime(start_time + estimate),
                )
                chunk_time = time.time()
    # write out the current df
    df, _ = add_buf(df, buf)
    print(time.ctime(), int((time.time() - start_time) / 60), "min", df.shape)
    try:
        fastparquet.write(
            f"{sys.argv[1]}.parquet",
            df,
            compression={"_default": {"type": "GZIP", "args": None}},
        )
    except:
        traceback.print_exc()
    return df


if __name__ == "__main__":
    df = main()
