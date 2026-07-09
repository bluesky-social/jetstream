// Command dicttrain retrains the /subscribe-v2 zstd dictionary from live
// firehose traffic. It is a dev/operator-time tool, not part of the server:
//
//	just train-subscribe-dict            # against localhost:8080
//	just train-subscribe-dict host=...   # against another instance
//
// It captures --samples uncompressed frames from ws://<host>/subscribe-v2,
// trains a fastCOVER dictionary via the zstd CLI (which measurably
// outperforms pure-Go builders on this corpus; see
// specs/notes/2026-07-09-subscribe-compression-cpu-analysis.md §7), embeds
// --dict-id (default: today's UTC date as YYYYMMDD, giving each retrain a
// self-documenting version in RFC 8878's unregistered ID range), verifies
// the result round-trips and reports the compression ratio on a held-out
// tail of the capture, then writes the dictionary to --out.
//
// Requires the `zstd` CLI (>= 1.4, for --train-fastcover) on PATH.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/klauspost/compress/zstd"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dicttrain:", err)
		os.Exit(1)
	}
}

func run() error {
	host := flag.String("host", "localhost:8080", "jetstream host serving /subscribe-v2")
	samples := flag.Int("samples", 100_000, "number of events to capture for training")
	holdout := flag.Int("holdout", 20_000, "additional events captured to evaluate the trained dictionary")
	maxDict := flag.Int("max-dict", 65536, "maximum dictionary size in bytes")
	dictID := flag.Uint("dict-id", 0, "dictionary ID to embed (default: today's UTC date as YYYYMMDD)")
	out := flag.String("out", "internal/subscribe/zstd_dictionary_v2", "output path for the trained dictionary")
	flag.Parse()

	if *dictID == 0 {
		*dictID = uint(mustAtoi(time.Now().UTC().Format("20060102")))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	fmt.Printf("capturing %d training + %d holdout events from %s/subscribe-v2...\n", *samples, *holdout, *host)
	train, eval, err := capture(ctx, *host, *samples, *holdout)
	if err != nil {
		return fmt.Errorf("capture: %w", err)
	}

	dict, err := trainDict(train, *maxDict, uint32(*dictID))
	if err != nil {
		return fmt.Errorf("train: %w", err)
	}

	ratio, err := evaluate(eval, dict)
	if err != nil {
		return fmt.Errorf("evaluate: %w", err)
	}
	fmt.Printf("trained %d-byte dictionary (id %d): %.2fx on %d held-out events\n",
		len(dict), *dictID, ratio, len(eval))

	if err := os.WriteFile(*out, dict, 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote %s — rebuild and re-run `just test ./internal/subscribe` to pick it up\n", *out)
	return nil
}

func mustAtoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return n
}

// capture reads train+holdout consecutive uncompressed frames from the
// /subscribe-v2 websocket.
func capture(ctx context.Context, host string, train, holdout int) (trainMsgs, evalMsgs [][]byte, err error) {
	conn, _, err := websocket.Dial(ctx, "ws://"+host+"/subscribe-v2", &websocket.DialOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = conn.CloseNow() }()
	conn.SetReadLimit(20_000_000)

	total := train + holdout
	msgs := make([][]byte, 0, total)
	lastReport := time.Now()
	for len(msgs) < total {
		_, payload, rerr := conn.Read(ctx)
		if rerr != nil {
			return nil, nil, fmt.Errorf("after %d events: %w", len(msgs), rerr)
		}
		msgs = append(msgs, payload)
		if time.Since(lastReport) > 5*time.Second {
			fmt.Printf("  %d/%d\n", len(msgs), total)
			lastReport = time.Now()
		}
	}
	return msgs[:train], msgs[train:], nil
}

// trainDict shells out to the zstd CLI's fastCOVER trainer. Samples are
// written one file per event (the trainer treats each file as one sample);
// -r keeps the argv short.
func trainDict(samples [][]byte, maxDict int, dictID uint32) ([]byte, error) {
	if _, err := exec.LookPath("zstd"); err != nil {
		return nil, fmt.Errorf("the zstd CLI is required on PATH: %w", err)
	}
	dir, err := os.MkdirTemp("", "dicttrain-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	sampleDir := filepath.Join(dir, "samples")
	if err := os.Mkdir(sampleDir, 0o755); err != nil {
		return nil, err
	}
	for i, m := range samples {
		name := filepath.Join(sampleDir, fmt.Sprintf("%07d", i))
		if err := writeFileBuffered(name, m); err != nil {
			return nil, err
		}
	}

	outPath := filepath.Join(dir, "dict.bin")
	cmd := exec.Command("zstd", "--train-fastcover", "-r", sampleDir,
		"--maxdict="+strconv.Itoa(maxDict),
		"--dictID="+strconv.FormatUint(uint64(dictID), 10),
		"-o", outPath, "-f")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("zstd --train: %w", err)
	}
	return os.ReadFile(outPath)
}

func writeFileBuffered(name string, b []byte) error {
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	if _, err := w.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// evaluate compresses the held-out messages with the production encoder
// configuration (SpeedFastest, 128 KiB window, single-goroutine) and
// verifies every frame round-trips through a dictionary-seeded decoder.
func evaluate(msgs [][]byte, dict []byte) (ratio float64, err error) {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderDict(dict),
		zstd.WithWindowSize(1<<17),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return 0, err
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	if err != nil {
		return 0, err
	}
	defer dec.Close()

	var in, out int64
	for _, m := range msgs {
		frame := enc.EncodeAll(m, nil)
		back, derr := dec.DecodeAll(frame, nil)
		if derr != nil {
			return 0, fmt.Errorf("round-trip decode: %w", derr)
		}
		if string(back) != string(m) {
			return 0, fmt.Errorf("round-trip mismatch")
		}
		in += int64(len(m))
		out += int64(len(frame))
	}
	return float64(in) / float64(out), nil
}
