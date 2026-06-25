package oracle

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/streaming"
)

// subscribeReposDial returns a jetstreamd LiveDial that dials the simulator's
// real subscribeRepos websocket handler over the in-process pipe client. No
// socket is involved — the websocket upgrade rides the pipe transport — yet the
// simulator's subscribeRepos disconnect faults fire exactly as over a real
// socket, and atmos's reconnect/resume path runs unchanged. This keeps the
// full-bubble run faithful to production for the live firehose path.
func subscribeReposDial(simClient *http.Client) streaming.DialFunc {
	return func(ctx context.Context, rawURL string) (streaming.Conn, *http.Response, error) {
		conn, resp, err := websocket.Dial(ctx, rawURL, &websocket.DialOptions{
			HTTPClient: simClient,
		})
		if err != nil {
			return nil, resp, err
		}
		return conn, resp, nil
	}
}
