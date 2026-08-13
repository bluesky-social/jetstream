// Connects to the live tip websocket and prints out all events as JSON.
//
// See the documentation for more information https://pkg.go.dev/github.com/bluesky-social/jetstream
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/bluesky-social/jetstream"
)

type CLIArgs struct {
	Host string
}

func main() {
	args := CLIArgs{}
	flag.StringVar(&args.Host, "host", "https://jetstream.us-east.bsky.network", "Jetstream host to which the process will connect")
	flag.Parse()

	if err := run(context.Background(), &args); err != nil {
		slog.Error("an error occurred while running the jetstream example", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args *CLIArgs) error {
	client, err := jetstream.Subscribe(args.Host)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	defer client.Close()

	for batch, err := range client.Events(ctx) {
		if err != nil {
			if errors.Is(err, jetstream.ErrFatal) {
				return fmt.Errorf("received fatal error: %w", err)
			}

			// You should handle other errors gracefully, depending
			// on what your application needs
			fmt.Printf("got an error: %v\n", err)
			continue
		}

		// These are all the events that are downloaded throughout the course
		// of the replay. Do with them what you wish!
		for _, event := range batch.Events() {
			if err := json.NewEncoder(os.Stdout).Encode(event); err != nil {
				return fmt.Errorf("failed to json encode event: %w", err)
			}
		}

		// You should persist this cursor to your database of choice
		// so you can resume later if your process restarts
		// batch.LastCursor()
	}

	return nil
}
