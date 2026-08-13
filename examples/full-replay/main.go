// Performs a full network replay with basic options, printing some sample events as it goes. This may take a while!
//
// Be sure to set the JETSTREAM_API_KEY environment variable if your server requires auth.
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
	"time"

	"github.com/bluesky-social/jetstream"
)

type CLIArgs struct {
	Host   string
	APIKey string
}

func main() {
	args := CLIArgs{}
	flag.StringVar(&args.Host, "host", "https://jetstream.us-east.bsky.network", "Jetstream host to which the process will connect")
	flag.Parse()

	args.APIKey = os.Getenv("JETSTREAM_API_KEY")

	if err := run(context.Background(), &args); err != nil {
		slog.Error("an error occurred while running the jetstream full-network example", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args *CLIArgs) error {
	// Declare our client connection options (see the docs for more)
	opts := []jetstream.Option{
		jetstream.WithAfterSeq(0), // start from the beginning of time
	}

	if args.APIKey != "" {
		opts = append(opts, jetstream.WithAPIToken(args.APIKey))
	}

	// Create a client connection pool
	client, err := jetstream.Subscribe(args.Host, opts...)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	defer client.Close() // Don't forget to clean up!

	// Print an event every so often
	var lastEvent jetstream.Event
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

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

		// These are all the events that are downloaded throughout the course of the replay.
		// Do with them what you wish!
		for _, event := range batch.Events() {
			lastEvent = event
		}

		// You should persist this cursor to your database of choice
		// so you can resume later if your process restarts
		// batch.LastCursor()

		// Print events every so often just to show we're doing something
		select {
		case <-ticker.C:
			if err := json.NewEncoder(os.Stdout).Encode(lastEvent); err != nil {
				return fmt.Errorf("failed to json encode lastEvent: %w", err)
			}
		default:
		}
	}

	return nil
}
