from rbloom import Bloom
import os, sys, time
import xxhash
import pandas as pd
import fastparquet
import traceback

# The original Wikidata files were split on linecount using split -u --lines=500000000
LINECOUNT = 500000000
DURATION_LIMIT = 10  # in seconds
BUF_LIMIT = 1000000
bf = Bloom(LINECOUNT, 0.01)

if os.path.exists("map.parquet"):
    print("Reading existing map.parquet")
    df = fastparquet.ParquetFile("map.parquet").to_pandas(["literal"])
    print("Seeding bloomfilter... ", end="")
    count = 0
    for row in df.values:
        bf.add(row[0])
        count += 1
    print(f"done, {count} items in Bloomfilter")


def add_if_not_seen(buf, val):
    if val in bf:
        return
    buf.append((xxhash.xxh64(val).intdigest(), val))
    bf.add(val)


def add_buf(df, buf):
    new_row = pd.DataFrame(
        {
            "hash": pd.Series([a for a, _ in buf], dtype="uint64"),
            "literal": pd.Series([a for _, a in buf], dtype="str"),
        }
    )
    return pd.concat([df, new_row], ignore_index=True), []


def main():
    start_time = time.time()
    chunk_time = start_time

    df = pd.DataFrame(
        {"hash": pd.Series([], dtype="uint64"), "literal": pd.Series([], dtype="str")}
    )

    with open(sys.argv[1]) as F:
        buf = []
        line = True
        line_no = 0
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
            line = line.strip()
            if not line.endswith(" ."):
                line = True
                continue
            line = line[:-2]
            parts = line.split(" ")
            if len(parts) < 3:
                continue
            s = parts[0]
            p = parts[1]
            o = " ".join(parts[2:])

            add_if_not_seen(buf, s)
            add_if_not_seen(buf, p)
            add_if_not_seen(buf, o)

            if len(buf) > BUF_LIMIT:
                df, buf = add_buf(df, buf)
            loop_time = time.time()
            if loop_time - chunk_time > DURATION_LIMIT:
                lps = line_no / (loop_time - start_time)
                estimate = 0
                if lps > 0:
                    estimate = LINECOUNT / lps
                print(
                    time.ctime(),
                    sys.argv[1],
                    line_no,
                    int(lps),
                    time.ctime(start_time + estimate),
                )
                chunk_time = time.time()

    df, _ = add_buf(df, buf)
    print(
        time.ctime(), int((time.time() - start_time) / 60), "min", df.shape, sys.argv[1]
    )
    try:
        fastparquet.write(
            "map.parquet",
            df,
            append=os.path.exists("map.parquet"),
            compression={"_default": {"type": "GZIP", "args": None}},
        )
    except:
        traceback.print_exc()
    return df


if __name__ == "__main__":
    df = main()
