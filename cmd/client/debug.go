package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	rpprof "runtime/pprof"
	"sync/atomic"
	"time"
)

// debugConfig configures the optional profiling/observability harness used to
// investigate client memory growth during large backfills. Everything here is
// opt-in (zero value = disabled) and exists purely as a diagnostic aid; it has
// no effect on the client's correctness or on a default run.
type debugConfig struct {
	pprofAddr      string        // e.g. "localhost:6061"; "" disables the HTTP pprof server
	sampleInterval time.Duration // MemStats sample cadence; 0 disables sampling
	profileDir     string        // directory for heap/goroutine dumps; "" => os.TempDir()
	rssLimitMiB    int           // watchdog: dump profiles + exit(0) when RSS exceeds this; 0 disables
}

// startDebug wires up the opt-in profiling harness and returns a stop function.
// It is safe to call with a zero debugConfig (everything stays off). The
// returned stop flushes a final MemStats sample and stops background pollers.
//
// When pprofAddr is set, the listener is bound synchronously so an explicit
// operator request to expose pprof fails loudly (EADDRINUSE, bad address,
// permission denied) instead of silently never starting — a debugging control
// that quietly does nothing is worse than no control at all.
func startDebug(ctx context.Context, cfg debugConfig) (func(), error) {
	if cfg.pprofAddr == "" && cfg.sampleInterval == 0 && cfg.rssLimitMiB == 0 {
		return func() {}, nil
	}

	dir := cfg.profileDir
	if dir == "" {
		dir = os.TempDir()
	}
	_ = os.MkdirAll(dir, 0o755)

	if cfg.pprofAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		// Bind up front so a failure to listen is reported to the caller rather
		// than swallowed inside the serve goroutine. ListenConfig.Listen ties the
		// bind to ctx (the run context), so cancellation during startup is honored.
		var lc net.ListenConfig
		ln, err := lc.Listen(ctx, "tcp", cfg.pprofAddr)
		if err != nil {
			return nil, fmt.Errorf("debug: pprof listen on %q: %w", cfg.pprofAddr, err)
		}
		srv := &http.Server{Handler: mux}
		go func() {
			fmt.Fprintf(os.Stderr, "debug: pprof listening on http://%s/debug/pprof/\n", ln.Addr())
			if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
				fmt.Fprintln(os.Stderr, "debug: pprof server:", err)
			}
		}()
	}

	start := time.Now()
	// dumped guards the one-shot watchdog dump so a racing sampler tick and the
	// stop path cannot both write the profile set.
	var dumped atomic.Bool
	stopCh := make(chan struct{})
	done := make(chan struct{})

	sample := func(label string) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		rss := rssBytes()
		fmt.Fprintf(os.Stderr,
			"debug: %-6s elapsed=%s rss=%dMiB heap_alloc=%dMiB heap_inuse=%dMiB heap_sys=%dMiB heap_idle_rel=%dMiB stack_inuse=%dMiB next_gc=%dMiB sys=%dMiB num_gc=%d goroutines=%d\n",
			label, time.Since(start).Round(time.Second),
			rss/(1<<20), m.HeapAlloc/(1<<20), m.HeapInuse/(1<<20), m.HeapSys/(1<<20),
			(m.HeapIdle-m.HeapReleased)/(1<<20), m.StackInuse/(1<<20), m.NextGC/(1<<20),
			m.Sys/(1<<20), m.NumGC, runtime.NumGoroutine())
	}

	// dumpProfiles writes heap (inuse+alloc) and goroutine profiles once. Used by
	// the watchdog before a safety exit and by the final stop path.
	dumpProfiles := func(tag string) {
		if !dumped.CompareAndSwap(false, true) {
			return
		}
		runtime.GC() // settle inuse_space so the heap profile reflects live data
		writeProfile(filepath.Join(dir, "heap-"+tag+".pprof"), "heap")
		writeProfile(filepath.Join(dir, "goroutine-"+tag+".pprof"), "goroutine")
		writeProfile(filepath.Join(dir, "allocs-"+tag+".pprof"), "allocs")
		fmt.Fprintf(os.Stderr, "debug: wrote heap/goroutine/allocs profiles to %s (tag=%s)\n", dir, tag)
	}

	if cfg.sampleInterval > 0 || cfg.rssLimitMiB > 0 {
		interval := cfg.sampleInterval
		if interval == 0 {
			interval = 2 * time.Second // watchdog still needs a poll cadence
		}
		go func() {
			defer close(done)
			t := time.NewTicker(interval)
			defer t.Stop()
			if cfg.sampleInterval > 0 {
				sample("start")
			}
			for {
				select {
				case <-ctx.Done():
					return
				case <-stopCh:
					return
				case <-t.C:
					if cfg.sampleInterval > 0 {
						sample("tick")
					}
					if cfg.rssLimitMiB > 0 && rssBytes() >= uint64(cfg.rssLimitMiB)<<20 {
						fmt.Fprintf(os.Stderr, "debug: RSS watchdog tripped at %d MiB (limit %d MiB); dumping profiles and exiting cleanly to preserve valid pprof data\n",
							rssBytes()/(1<<20), cfg.rssLimitMiB)
						sample("trip")
						dumpProfiles("watchdog")
						os.Exit(0)
					}
				}
			}
		}()
	} else {
		close(done)
	}

	return func() {
		close(stopCh)
		<-done
		if cfg.sampleInterval > 0 {
			sample("final")
		}
		dumpProfiles("final")
	}, nil
}

// rssBytes returns the process resident set size in bytes, read from
// /proc/self/statm (Linux). Returns 0 if it cannot be read, which disables the
// watchdog rather than guessing.
func rssBytes() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	// Fields are page counts: size resident shared text lib data dt.
	var size, resident uint64
	if _, err := fmt.Sscanf(string(data), "%d %d", &size, &resident); err != nil {
		return 0
	}
	return resident * uint64(os.Getpagesize())
}

func writeProfile(path, name string) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "debug: create profile:", err)
		return
	}
	defer func() { _ = f.Close() }()
	if name == "heap" {
		if err := rpprof.WriteHeapProfile(f); err != nil {
			fmt.Fprintln(os.Stderr, "debug: write heap profile:", err)
		}
		return
	}
	p := rpprof.Lookup(name)
	if p == nil {
		fmt.Fprintln(os.Stderr, "debug: no such profile:", name)
		return
	}
	// debug=0 => protobuf format, consumable by `go tool pprof`.
	if err := p.WriteTo(f, 0); err != nil {
		fmt.Fprintln(os.Stderr, "debug: write profile:", err)
	}
}
