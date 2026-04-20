package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"mochilas/internal/convert"
)

type Result struct {
	Input      string `json:"input"`
	Output     string `json:"output"`
	Rows       int    `json:"rows"`
	Bytes      int64  `json:"bytes"`
	DurationMs int64  `json:"duration_ms"`
	Skipped    bool   `json:"skipped,omitempty"`
	Err        string `json:"error,omitempty"`
}

func main() {
	inDir := flag.String("in-dir", "", "directory to scan for .shp files (recursive)")
	outDir := flag.String("out-dir", "", "directory to write .parquet files into")
	workers := flag.Int("workers", runtime.NumCPU(), "number of worker goroutines")
	manifestPath := flag.String("manifest", "", "path to JSONL manifest (default: <out-dir>/manifest.jsonl)")
	skipExisting := flag.Bool("skip-existing", true, "skip inputs whose output already exists")
	failFast := flag.Bool("fail-fast", false, "abort the run on the first error")
	flag.Parse()

	if *inDir == "" || *outDir == "" {
		log.Fatal("usage: shp2parquet-pool -in-dir DIR -out-dir DIR [-workers N] [-manifest PATH]")
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("mkdir out-dir: %v", err)
	}

	if *manifestPath == "" {
		*manifestPath = filepath.Join(*outDir, "manifest.jsonl")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *inDir, *outDir, *manifestPath, *workers, *skipExisting, *failFast); err != nil {
		log.Fatalf("run: %v", err)
	}
}

func run(ctx context.Context, inDir, outDir, manifestPath string, workers int, skipExisting, failFast bool) error {
	mf, err := os.Create(manifestPath)
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	defer mf.Close()
	enc := json.NewEncoder(mf)
	var manifestMu sync.Mutex

	jobs := make(chan string, workers*2)
	results := make(chan Result, workers*2)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(jobs)
		return filepath.WalkDir(inDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.EqualFold(filepath.Ext(path), ".shp") {
				return nil
			}
			select {
			case jobs <- path:
				return nil
			case <-gctx.Done():
				return gctx.Err()
			}
		})
	})

	var wwg sync.WaitGroup
	for range workers {
		wwg.Add(1)
		g.Go(func() error {
			defer wwg.Done()
			for in := range jobs {
				res := processOne(in, inDir, outDir, skipExisting)
				select {
				case results <- res:
				case <-gctx.Done():
					return gctx.Err()
				}
				if failFast && res.Err != "" {
					return fmt.Errorf("%s: %s", res.Input, res.Err)
				}
			}
			return nil
		})
	}

	go func() {
		wwg.Wait()
		close(results)
	}()

	var (
		done, ok, skipped, failed atomic.Int64
		totalRows, totalBytes     atomic.Int64
	)
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		start := time.Now()
		lastLog := start
		for r := range results {
			manifestMu.Lock()
			_ = enc.Encode(r)
			manifestMu.Unlock()

			done.Add(1)
			switch {
			case r.Err != "":
				failed.Add(1)
			case r.Skipped:
				skipped.Add(1)
			default:
				ok.Add(1)
				totalRows.Add(int64(r.Rows))
				totalBytes.Add(r.Bytes)
			}
			if n := done.Load(); n%25 == 0 || time.Since(lastLog) > 2*time.Second {
				lastLog = time.Now()
				log.Printf("progress: done=%d ok=%d skip=%d fail=%d rows=%d bytes=%dMB elapsed=%s",
					n, ok.Load(), skipped.Load(), failed.Load(),
					totalRows.Load(), totalBytes.Load()/(1<<20),
					time.Since(start).Round(time.Second))
			}
		}
	}()

	werr := g.Wait()
	<-collectorDone

	log.Printf("summary: ok=%d skip=%d fail=%d rows=%d bytes=%dMB manifest=%s",
		ok.Load(), skipped.Load(), failed.Load(),
		totalRows.Load(), totalBytes.Load()/(1<<20), manifestPath)

	if werr != nil && !errors.Is(werr, context.Canceled) {
		return werr
	}
	if failed.Load() > 0 && failFast {
		return fmt.Errorf("%d failures", failed.Load())
	}
	return nil
}

func processOne(in, inDir, outDir string, skipExisting bool) (res Result) {
	defer func() {
		if p := recover(); p != nil {
			res = Result{Input: in, Err: fmt.Sprintf("panic: %v", p)}
		}
	}()

	rel, err := filepath.Rel(inDir, in)
	if err != nil {
		rel = filepath.Base(in)
	}
	out := filepath.Join(outDir, strings.TrimSuffix(rel, filepath.Ext(rel))+".parquet")

	if skipExisting {
		if _, err := os.Stat(out); err == nil {
			return Result{Input: in, Output: out, Skipped: true}
		}
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return Result{Input: in, Output: out, Err: fmt.Sprintf("mkdir: %v", err)}
	}

	s, err := convert.Convert(in, out)
	if err != nil {
		return Result{Input: in, Output: out, Err: err.Error()}
	}
	return Result{
		Input:      in,
		Output:     out,
		Rows:       s.Rows,
		Bytes:      s.Bytes,
		DurationMs: s.Duration.Milliseconds(),
	}
}
