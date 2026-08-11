# Subscribe — v1-Compatible Filtering

This historical design specified v1-compatible collection, DID, and maximum-message-size filtering for `/subscribe`, including mid-stream `options_update` handling. Filtering remained per connection in the handler rather than the broadcaster, with strict parsing, close-code behavior, metrics, and fuzz coverage. References were `internal/subscribe/filter.go`, `internal/subscribe/handler.go`, `internal/subscribe/doc.go`, and the [v1 README contract](https://github.com/bluesky-social/jetstream/blob/main/README.md).
