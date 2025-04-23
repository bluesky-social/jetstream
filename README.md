# Jetstream

Jetstream is a streaming service that consumes an ATProto `com.atproto.sync.subscribeRepos` stream and converts it into lightweight, friendly JSON.

Jetstream converts the CBOR-encoded MST blocks produced by the ATProto firehose and translates them into JSON objects that are easier to interface with using standard tooling available in programming languages.

### Public Instances

As of writing, there are 4 official public Jetstream instances operated by Bluesky.

| Hostname                          | Region  |
| --------------------------------- | ------- |
| `jetstream1.us-east.bsky.network` | US-East |
| `jetstream2.us-east.bsky.network` | US-East |
| `jetstream1.us-west.bsky.network` | US-West |
| `jetstream2.us-west.bsky.network` | US-West |

Connect to these instances over WSS: `wss://jetstream2.us-west.bsky.network/subscribe`

We will monitor and operate these instances and do our best to keep them available for public use by developers.

Feel free to have multiple connections to Jetstream instances if needed. We encourage you to make use of Jetstream wherever you may consider using the `com.atproto.sync.subscribeRepos` firehose if you don't need the features of the full sync protocol.

Because cursors for Jetstream are time-based (unix microseconds), you can use the same cursor for multiple instances to get roughly the same data.

When switching between instances, it may be prudent to rewind your cursor a few seconds for gapless playback if you process events idempotently.

## Running Jetstream

To run Jetstream, make sure you have docker and docker compose installed and run `make up` in the repo root.

This will pull the latest built image from GHCR and start a Jetstream instance at `http://localhost:6008`

- To build Jetstream from source via Docker and start it up, run `make rebuild`

Once started, you can connect to the event stream at: `ws://localhost:6008/subscribe`

Prometheus metrics are exposed at `http://localhost:6009/metrics`

A [Grafana Dashboard](#dashboard-preview) for Jetstream is available at `./grafana-dashboard.json` and should be easy to copy/paste into Grafana's dashboard import prompt.

- This dashboard has a few device-specific graphs for disk and network usage that require NodeExporter and may need to be tuned to your setup.

## Consuming Jetstream

To consume Jetstream you can use any websocket client

Connect to `ws://localhost:6008/subscribe` to start the stream

The following Query Parameters are supported:

- `wantedCollections` - An array of [Collection NSIDs](https://atproto.com/specs/nsid) to filter which records you receive on your stream (default empty = all collections)
  - `wantedCollections` supports NSID path prefixes i.e. `app.bsky.graph.*`, or `app.bsky.*`. The prefix before the `.*` must pass NSID validation and Jetstream **does not** support incomplete prefixes i.e. `app.bsky.graph.fo*`.
  - Regardless of desired collections, all subscribers receive Account and Identity events.
  - You can specify at most 100 wanted collections/prefixes.
- `wantedDids` - An array of Repo DIDs to filter which records you receive on your stream (Default empty = all repos)
  - You can specify at most 10,000 wanted DIDs.
- `maxMessageSizeBytes` - The maximum size of a payload that this client would like to receive. Zero means no limit, negative values are treated as zero. (Default "0" or empty = no maximum size)
- `cursor` - A unix microseconds timestamp cursor to begin playback from
  - An absent cursor or a cursor from the future will result in live-tail operation
  - When reconnecting, use the `time_us` from your most recently processed event and maybe provide a negative buffer (i.e. subtract a few seconds) to ensure gapless playback
- `compress` - Set to `true` to enable `zstd` [compression](#compression)
- `requireHello` - Set to `true` to pause replay/live-tail until the server receives a [`SubscriberOptionsUpdatePayload`](#options-updates) over the socket in a [Subscriber Sourced Message](#subscriber-sourced-messages)

### Examples

A simple example that hits the public instance looks like:

```bash
$ websocat wss://jetstream2.us-east.bsky.network/subscribe\?wantedCollections=app.bsky.feed.post
```

A maximal example using all parameters looks like:

```bash
$ websocat "ws://localhost:6008/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.graph.follow&wantedDids=did:plc:q6gjnaw2blty4crticxkmujt&cursor=1725519626134432"
```

### Example events:

Jetstream events have 3 `kinds`s (so far):

- `commit`: a Commit to a repo which involves either a create, update, or delete of a record
- `identity`: an Identity update for a DID which indicates that you may want to purge an identity cache and revalidate the DID doc and handle
- `account`: an Account event that indicates a change in account status i.e. from `active` to `deactivated`, or to `takendown` if the PDS has taken down the repo.

Jetstream Commits have 3 `operations`:

- `create`: Create a new record with the contents provided
- `update`: Update an existing record and replace it with the contents provided
- `delete`: Delete an existing record with the DID, Collection, and RKey provided

#### A like committed to a repo

```json
{
  "did": "did:plc:eygmaihciaxprqvxpfvl6flk",
  "time_us": 1725911162329308,
  "kind": "commit",
  "commit": {
    "rev": "3l3qo2vutsw2b",
    "operation": "create",
    "collection": "app.bsky.feed.like",
    "rkey": "3l3qo2vuowo2b",
    "record": {
      "$type": "app.bsky.feed.like",
      "createdAt": "2024-09-09T19:46:02.102Z",
      "subject": {
        "cid": "bafyreidc6sydkkbchcyg62v77wbhzvb2mvytlmsychqgwf2xojjtirmzj4",
        "uri": "at://did:plc:wa7b35aakoll7hugkrjtf3xf/app.bsky.feed.post/3l3pte3p2e325"
      }
    },
    "cid": "bafyreidwaivazkwu67xztlmuobx35hs2lnfh3kolmgfmucldvhd3sgzcqi"
  }
}
```

#### A deleted follow record

```json
{
  "did": "did:plc:rfov6bpyztcnedeyyzgfq42k",
  "time_us": 1725516666833633,
  "kind": "commit",
  "commit": {
    "rev": "3l3f6nzl3cv2s",
    "operation": "delete",
    "collection": "app.bsky.graph.follow",
    "rkey": "3l3dn7tku762u"
  }
}
```

#### An identity update

```json
{
  "did": "did:plc:ufbl4k27gp6kzas5glhz7fim",
  "time_us": 1725516665234703,
  "kind": "identity",
  "identity": {
    "did": "did:plc:ufbl4k27gp6kzas5glhz7fim",
    "handle": "yohenrique.bsky.social",
    "seq": 1409752997,
    "time": "2024-09-05T06:11:04.870Z"
  }
}
```

#### An account becoming active

```json
{
  "did": "did:plc:ufbl4k27gp6kzas5glhz7fim",
  "time_us": 1725516665333808,
  "kind": "account",
  "account": {
    "active": true,
    "did": "did:plc:ufbl4k27gp6kzas5glhz7fim",
    "seq": 1409753013,
    "time": "2024-09-05T06:11:04.870Z"
  }
}
```

### Compression

Jetstream supports `zstd`-based compression of messages. Jetstream uses a custom dictionary for compression that can be found in `pkg/models/zstd_dictionary` and is required to decode compressed messages from the server.

`zstd` compressed Jetstream messages are ~56% smaller on average than the raw JSON version of the Jetstream firehose.

The provided client library uses compression by default, using an embedded copy of the Dictionary from the `models` package.

To request a compressed stream, pass the `Socket-Encoding: zstd` header through when initiating the websocket _or_ pass `compress=true` in the query string.

### Subscriber Sourced messages

Subscribers can send Text messages to Jetstream over the websocket using the `SubscriberSourcedMessage` framing below:

```go
type SubscriberSourcedMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}
```

The supported message types are as follows:

- `options_update`

#### Options Updates

A client can update their `wantedCollections` and `wantedDids` after connecting to the socket by sending a Subscriber Sourced Message.

To send an Options Update, provide the string `options_update` in the `type` field and a `SubscriberOptionsUpdatePayload` in the `payload` field.

The shape for a `SubscriberOptionsUpdatePayload` is as follows:

```go
type SubscriberOptionsUpdateMsg struct {
	WantedCollections   []string `json:"wantedCollections"`
	WantedDIDs          []string `json:"wantedDids"`
	MaxMessageSizeBytes int      `json:"maxMessageSizeBytes"`
}
```

If either array is empty, the relevant filter will be disabled (i.e. sending empty `wantedDids` will mean a client gets messages for all DIDs again).

Some limitations apply around the size of the message: right now the message can be at most 10MB in size and can contain up to 100 collection filters _and_ up to 10,000 DID filters.

Additionally, a client can connect with `?requireHello=true` in the query params to pause replay/live-tail until the first Options Update message is sent by the client over the socket.

Invalid Options Updates in `requireHello` mode or normal operating mode will result in the client being disconnected.

An example Subscriber Sourced Message with an Options Update payload is as follows:

```json
{
  "type": "options_update",
  "payload": {
    "wantedCollections": ["app.bsky.feed.post"],
    "wantedDids": ["did:plc:q6gjnaw2blty4crticxkmujt"],
    "maxMessageSizeBytes": 1000000
  }
}
```

The above payload will filter such that a client receives only posts, and only from a the specified DID.

### Dashboard Preview

![A screenshot of the Jetstream Grafana Dashboard](./docs/dash.png)
