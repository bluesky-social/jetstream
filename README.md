# Jetstream

Full-network archive and streaming service for atproto.

## Getting started

You'll need a recent Go (see `go.mod` for the version) and [`just`](https://github.com/casey/just). Once you have those:

```sh
just install-tools   # one-time: golangci-lint + gotestsum
just                 # lint + test, the default recipe
```

## Running it

```sh
just run serve              # starts the HTTP server on :8080 (debug on :6060)
just run-race serve         # same thing with the race detector on
```

## Tests

```sh
just test                   # everything
just test ./internal/foo    # one package
just test-race              # with -race

just lint # runs the linter
```

## Building a binary

```sh
just build      # drops the binary at ./bin/jetstream
just clean      # nukes ./bin
```
