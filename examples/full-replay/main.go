// Performs a full network replay with basic options, printing some sample events as it goes. This may take a while!
//
// See the documentation for more information https://pkg.go.dev/github.com/bluesky-social/jetstream
package main

import (
	"context"
	"encoding/json"
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
	args.Host = *flag.String("host", "https://jetstream.us-east.bsky.network", "Jetstream host to which the process will connect")
	args.APIKey = os.Getenv("JETSTREAM_API_KEY")
	flag.Parse()

	if err := run(context.Background(), &args); err != nil {
		slog.Error("an error occurred while running the jetstream example", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args *CLIArgs) error {
	// Declare our client connection options (see the docs for more)
	opts := []jetstream.Option{
		jetstream.WithAPIToken(args.APIKey), // pass our credential
		jetstream.WithAfterSeq(0),           // start from the beginning of time
	}

	client, err := jetstream.Subscribe(args.Host, opts...)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	defer client.Close()

	// Print an event every so often
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastEvent jetstream.Event
	for batch, err := range client.Events(ctx) {
		if err != nil {
			return fmt.Errorf("failed to receive batch: %w", err)
		}

		for _, event := range batch.Events() {
			// These are all the events that are downloaded throughout the course of the replay.
			// Do with them what you wish!
			lastEvent = event
		}

		// You should persist this cursor to your database of choice
		// so you can resume later if your process restarts
		// batch.LastCursor()

		// Print events every so often just to show we're doing something
		select {
		case <-ticker.C:
			if err := printLastEvent(lastEvent); err != nil {
				return fmt.Errorf("failed to marshal event: %w", err)
			}
		default:
		}
	}

	return nil
}

func printLastEvent(event jetstream.Event) error {
	buf, err := json.Marshal(event)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", buf)
	return nil
}
