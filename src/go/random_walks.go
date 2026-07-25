// Package main is a Go rewrite of the Python random-walk generator.
//
// Design decisions vs. the Python/igraph/multiprocessing version, and why
// they should be faster here:
//
//  1. Graph representation: CSR (compressed sparse row) adjacency built from
//     two flat []int32 slices (offsets + neighbors) instead of an igraph
//     Graph object. Neighbor lookups for a vertex are a single contiguous
//     slice (offsets[v]:offsets[v+1]) — cache-friendly, no per-vertex Python
//     object overhead, no separate C library graph structure to marshal
//     into/out of.
//
//  2. Concurrency: goroutines + channels instead of OS processes. Everything
//     shares one address space by default (real threads, real parallelism —
//     Go has no GIL), so there's no dict/COW-duplication problem like the
//     Python version had, and no need to manually tune a "chunksize": the
//     jobs channel naturally load-balances work across workers as they
//     finish, one vertex at a time, with no idle workers waiting on an
//     oversized chunk.
//
//  3. RNG: each worker owns its own *rand.Rand instance (no shared/mutex-
//     guarded global source), so there's zero lock contention generating
//     millions of random walk steps concurrently.
//
//  4. Bulk insert: DuckDB's native Appender API instead of executemany with
//     parameter binding — rows are streamed directly into DuckDB's internal
//     columnar format rather than being parsed as SQL text or bound one at a
//     time.
//
// Build:
//
//	CGO_ENABLED=1 go build -o random_walks .
//
// (go-duckdb uses cgo bindings to the DuckDB C API, so cgo must stay on and
// a C toolchain must be available on the build machine.)
//
// Run:
//
//	./random_walks -db /path/to/db.duckdb -workers 96 -batch-size 9600
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// Result is what a worker sends back after processing one vertex.
type Result struct {
	S     uint64
	Walks [][]uint64
}

func worker(
	id int,
	jobs <-chan int32,
	results chan<- Result,
	offsets []int32,
	neighbors []int32,
	smallBig []uint64,
	noWalks int,
	maxWalkLen int,
) {
	// Independent RNG per worker: no shared mutex, no contention.
	rng := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(id)<<32))

	walkBuf := make([]int32, 0, maxWalkLen+1)
	keyBuf := make([]byte, 0, (maxWalkLen+1)*8)

	for v := range jobs {
		seen := make(map[string]struct{}, noWalks)
		unique := make([][]uint64, 0, noWalks)

		for i := 0; i < noWalks; i++ {
			// --- generate one random walk starting at v ---
			walkBuf = walkBuf[:0]
			walkBuf = append(walkBuf, v)
			cur := v
			for step := 0; step < maxWalkLen; step++ {
				lo, hi := offsets[cur], offsets[cur+1]
				deg := hi - lo
				if deg == 0 {
					// Dead end: matches igraph's behavior of returning a
					// shorter walk when there's nowhere left to go.
					break
				}
				next := neighbors[lo+int32(rng.Intn(int(deg)))]
				walkBuf = append(walkBuf, next)
				cur = next
			}

			// --- translate to real hashes + dedupe (mirrors the Python
			// set() of walk tuples) ---
			bigWalk := make([]uint64, len(walkBuf))
			keyBuf = keyBuf[:0]
			for j, node := range walkBuf {
				h := smallBig[node]
				bigWalk[j] = h
				keyBuf = append(keyBuf,
					byte(h), byte(h>>8), byte(h>>16), byte(h>>24),
					byte(h>>32), byte(h>>40), byte(h>>48), byte(h>>56),
				)
			}
			key := string(keyBuf) // copies keyBuf; safe as a map key
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				unique = append(unique, bigWalk)
			}
		}

		results <- Result{S: smallBig[v], Walks: unique}
	}
}

func main() {
	dbPath := flag.String("db", "", "path to the DuckDB database file")
	noWalks := flag.Int("no-walks", 100, "number of random walks per vertex")
	maxWalkLen := flag.Int("max-walk-length", 15, "max steps per walk")
	batchSize := flag.Int("batch-size", 9600, "vertices processed per outer batch (progress/flush granularity)")
	numWorkers := flag.Int("workers", runtime.NumCPU(), "number of concurrent walk-generating goroutines")
	nBatches := flag.Int("n-batches", 0, "stop after this many batches (0 = process everything)")
	flag.Parse()

	if *dbPath == "" {
		log.Fatal("must supply -db")
	}

	ctx := context.Background()

	connector, err := duckdb.NewConnector(*dbPath, nil)
	if err != nil {
		log.Fatalf("connecting to duckdb: %v", err)
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	if _, err := db.ExecContext(ctx,
		"create table if not exists random_walks (s ubigint, walks ubigint[][])"); err != nil {
		log.Fatalf("creating random_walks table: %v", err)
	}

	// --- 1. Build the small<->big id mapping ---
	log.Println("loading vertex hashes...")
	var bigSmall = make(map[uint64]int32)
	var smallBig []uint64
	{
		rows, err := db.QueryContext(ctx,
			"select hash from iris union select hash from literals order by hash")
		if err != nil {
			log.Fatalf("querying hashes: %v", err)
		}
		var i int32
		for rows.Next() {
			var h uint64
			if err := rows.Scan(&h); err != nil {
				log.Fatalf("scanning hash: %v", err)
			}
			bigSmall[h] = i
			smallBig = append(smallBig, h)
			i++
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("iterating hashes: %v", err)
		}
		rows.Close()
	}
	nVertices := int32(len(smallBig))
	log.Printf("retrieved %d hashes", nVertices)

	// --- 2. Load edges and build a CSR adjacency structure ---
	// (undirected, matching igraph's default Graph() behavior: each row
	// becomes a neighbor entry on BOTH endpoints, including duplicates if
	// the same pair appears more than once — i.e. a multigraph, same as
	// what add_edges() produced in the original script.)
	log.Println("loading edges...")
	var edgeS, edgeO []int32
	{
		rows, err := db.QueryContext(ctx, "select distinct s, o from triples")
		if err != nil {
			log.Fatalf("querying triples: %v", err)
		}
		for rows.Next() {
			var s, o uint64
			if err := rows.Scan(&s, &o); err != nil {
				log.Fatalf("scanning triple: %v", err)
			}
			edgeS = append(edgeS, bigSmall[s])
			edgeO = append(edgeO, bigSmall[o])
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("iterating triples: %v", err)
		}
		rows.Close()
	}
	log.Printf("retrieved %d edges", len(edgeS))

	log.Println("building CSR adjacency structure...")
	degree := make([]int32, nVertices+1) // +1: convenient for the prefix sum below
	for i := range edgeS {
		degree[edgeS[i]]++
		degree[edgeO[i]]++
	}
	offsets := make([]int32, nVertices+1)
	var running int32
	for v := int32(0); v < nVertices; v++ {
		offsets[v] = running
		running += degree[v]
	}
	offsets[nVertices] = running

	neighbors := make([]int32, running)
	fillPos := make([]int32, nVertices)
	copy(fillPos, offsets[:nVertices])
	for i := range edgeS {
		s, o := edgeS[i], edgeO[i]
		neighbors[fillPos[s]] = o
		fillPos[s]++
		neighbors[fillPos[o]] = s
		fillPos[o]++
	}
	edgeS, edgeO, degree, fillPos = nil, nil, nil, nil // free, no longer needed
	log.Printf("adjacency structure built: %d vertices, %d directed neighbor entries", nVertices, len(neighbors))

	// --- 3. Compute the work queue once ---
	log.Println("computing work queue...")
	alreadyDone := make(map[uint64]struct{})
	{
		rows, err := db.QueryContext(ctx, "select s from random_walks")
		if err != nil {
			log.Fatalf("querying random_walks: %v", err)
		}
		for rows.Next() {
			var s uint64
			if err := rows.Scan(&s); err != nil {
				log.Fatalf("scanning s: %v", err)
			}
			alreadyDone[s] = struct{}{}
		}
		rows.Close()
	}
	log.Printf("%d vertices already have walks computed", len(alreadyDone))

	var todo []int32
	{
		rows, err := db.QueryContext(ctx, "select distinct s from triples")
		if err != nil {
			log.Fatalf("querying distinct s: %v", err)
		}
		for rows.Next() {
			var s uint64
			if err := rows.Scan(&s); err != nil {
				log.Fatalf("scanning s: %v", err)
			}
			if _, done := alreadyDone[s]; !done {
				todo = append(todo, bigSmall[s])
			}
		}
		rows.Close()
	}
	alreadyDone = nil
	log.Printf("%d vertices remaining to process", len(todo))

	// --- 4. Spin up the worker pool ---
	jobs := make(chan int32, *numWorkers*4)
	results := make(chan Result, *numWorkers*4)
	for w := 0; w < *numWorkers; w++ {
		go worker(w, jobs, results, offsets, neighbors, smallBig, *noWalks, *maxWalkLen)
	}

	// --- 5. Get a raw connection + Appender for bulk inserts ---
	rawConn, err := db.Conn(ctx)
	if err != nil {
		log.Fatalf("getting raw connection: %v", err)
	}
	defer rawConn.Close()

	var appender *duckdb.Appender
	if err := rawConn.Raw(func(dc any) error {
		conn := dc.(driver.Conn)
		a, err := duckdb.NewAppenderFromConn(conn, "", "random_walks")
		if err != nil {
			return err
		}
		appender = a
		return nil
	}); err != nil {
		log.Fatalf("creating appender: %v", err)
	}
	defer appender.Close()

	// --- 6. Main batch loop ---
	batch := 0
	for start := 0; start < len(todo); start += *batchSize {
		end := start + *batchSize
		if end > len(todo) {
			end = len(todo)
		}
		vertices := todo[start:end]

		startTime := time.Now()

		// Feed this batch's jobs in from a separate goroutine so we can
		// drain `results` concurrently without deadlocking on a full
		// buffered channel.
		go func(vs []int32) {
			for _, v := range vs {
				jobs <- v
			}
		}(vertices)

		for i := 0; i < len(vertices); i++ {
			r := <-results
			if err := appender.AppendRow(r.S, r.Walks); err != nil {
				log.Fatalf("appending row: %v", err)
			}
		}
		if err := appender.Flush(); err != nil {
			log.Fatalf("flushing appender: %v", err)
		}

		elapsed := time.Since(startTime)
		log.Printf("batch %d processed in %.2f seconds (%d vertices)", batch, elapsed.Seconds(), len(vertices))

		batch++
		if *nBatches > 0 && batch >= *nBatches {
			break
		}
	}

	close(jobs)
	fmt.Println("done")
}
