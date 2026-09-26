package main

import (
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
)

// The live mix is calibrated against a sample of production segments
// (2026-09-21, 222,724 events): collection shares, about 90% unique DIDs
// in a 4,096-event block, and about 94 compressed bytes per event.
var liveCollections = []struct {
	nsid  string
	share float64
}{
	{"app.bsky.feed.like", 0.720},
	{"app.bsky.feed.post", 0.100},
	{"app.bsky.feed.repost", 0.074},
	{"app.bsky.graph.follow", 0.068},
	{"app.bsky.graph.block", 0.010},
	{"app.bsky.feed.threadgate", 0.008},
	{"app.bsky.feed.postgate", 0.003},
	{"app.bsky.graph.listitem", 0.003},
	{"app.bsky.actor.profile", 0.003},
	{"dev.sensorthings.observationBatch", 0.003},
	{"site.standard.document", 0.002},
	{"app.rocksky.scrobble", 0.001},
	{"app.bsky.actor.status", 0.001},
	{"place.stream.livestream", 0.001},
	{"is.currents.feed.save", 0.001},
	{"fm.teal.feed.play", 0.001},
	{"net.commoninternet.pdsgit.state", 0.001},
}

const (
	// A hot set of accounts takes hotShare of live events; the rest are
	// spread over the whole universe.
	hotAccounts = 2_000
	hotShare    = 0.35
	// Live kinds: the rest are creates.
	deleteShare = 0.06
	updateShare = 0.01
)

var words = strings.Fields(`the a to and of in is it you that for on this was with my but be have
	not are just so at like what all me can if about your one they out get now do up
	how people more time when day good new know think really love today would going
	want see make back still first even much post here some bluesky thread art photo
	game music night week year never always right thing feel over after world life`)

var didEncoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// didFor returns the universe's DID number n: stable, and shaped like a
// did:plc (24 base32 characters).
func didFor(n uint64) string {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], n)
	h := sha256.Sum256(b[:])
	return "did:plc:" + didEncoding.EncodeToString(h[:15])
}

// generator makes synthetic events. It is not safe for concurrent use.
type generator struct {
	rng      *rand.Rand
	universe uint64
	cum      []float64
	clock    uint
}

func newGenerator(seed, universe uint64) *generator {
	g := &generator{rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), universe: universe}
	var total float64
	for _, c := range liveCollections {
		total += c.share
		g.cum = append(g.cum, total)
	}
	return g
}

func (g *generator) did() string {
	if g.rng.Float64() < hotShare {
		return didFor(g.rng.Uint64N(hotAccounts))
	}
	return didFor(g.rng.Uint64N(g.universe))
}

func (g *generator) collection() string {
	x := g.rng.Float64() * g.cum[len(g.cum)-1]
	for i, c := range g.cum {
		if x < c {
			return liveCollections[i].nsid
		}
	}
	return liveCollections[len(liveCollections)-1].nsid
}

func (g *generator) tid(now time.Time) string {
	g.clock++
	return string(atmos.NewTIDFromTime(now.Add(-time.Duration(g.rng.IntN(1000))*time.Millisecond), g.clock%1024))
}

func (g *generator) cid() cbor.CID {
	var b [32]byte
	for i := 0; i < len(b); i += 8 {
		binary.LittleEndian.PutUint64(b[i:], g.rng.Uint64())
	}
	return cbor.ComputeCID(cbor.CodecDagCBOR, b[:])
}

// live returns one live event as the firehose consumer would append it.
// Seq is left for the writer.
func (g *generator) live(now time.Time, upstream int64) segment.Event {
	did := g.did()
	coll := g.collection()
	ev := segment.Event{
		WitnessedAt:         now.UnixMicro(),
		UpstreamRelayCursor: upstream,
		Kind:                segment.KindCreate,
		DID:                 did,
		Collection:          coll,
		Rkey:                g.tid(now),
		Rev:                 g.tid(now),
	}
	switch x := g.rng.Float64(); {
	case x < deleteShare:
		ev.Kind = segment.KindDelete
		return ev
	case x < deleteShare+updateShare:
		ev.Kind = segment.KindUpdate
	}
	ev.Payload = g.record(coll, now)
	return ev
}

// liveDIDLen is the length of a live DID. A bulk repo's DID is shorter, so
// a reader can tell the two apart: UpstreamRelayCursor is not archived.
var liveDIDLen = len(didFor(0))

// isLive reports whether ev came from generator.live.
func isLive(ev *segment.Event) bool { return len(ev.DID) == liveDIDLen }

// repo returns n backfill rows for one fresh repo, as a bulk resync
// appends them: one DID, one rev, no upstream cursor.
func (g *generator) repo(now time.Time, n int) []segment.Event {
	did := "did:plc:" + didEncoding.EncodeToString(binary.LittleEndian.AppendUint64(nil, g.rng.Uint64()))
	rev := g.tid(now)
	out := make([]segment.Event, n)
	for i := range out {
		coll := g.collection()
		out[i] = segment.Event{
			WitnessedAt: now.UnixMicro(),
			Kind:        segment.KindCreate,
			DID:         did,
			Collection:  coll,
			Rkey:        g.tid(now.Add(-time.Duration(g.rng.IntN(1<<30)) * time.Millisecond)),
			Rev:         rev,
			Payload:     g.record(coll, now),
		}
	}
	return out
}

// repoSize draws a repo's record count: most are small, a few are large.
func (g *generator) repoSize() int {
	n := int(20 * (1 + g.rng.ExpFloat64()*30))
	return min(n, 20_000)
}

// record returns a DAG-CBOR record roughly the shape and size of a real
// one of that collection. Map keys follow DAG-CBOR order: length, then
// bytes.
func (g *generator) record(coll string, now time.Time) []byte {
	b := make([]byte, 0, 256)
	created := now.UTC().Format("2006-01-02T15:04:05.000Z")
	switch coll {
	case "app.bsky.feed.like", "app.bsky.feed.repost":
		b = cbor.AppendMapHeader(b, 3)
		b = appendKV(b, "$type", coll)
		b = cbor.AppendTextKey(b, "subject")
		b = g.appendStrongRef(b, now)
		b = appendKV(b, "createdAt", created)
	case "app.bsky.feed.post":
		reply := g.rng.Float64() < 0.4
		n := uint64(4)
		if reply {
			n++
		}
		b = cbor.AppendMapHeader(b, n)
		b = appendKV(b, "text", g.text())
		b = appendKV(b, "$type", coll)
		b = cbor.AppendTextKey(b, "langs")
		b = cbor.AppendArrayHeader(b, 1)
		b = cbor.AppendText(b, "en")
		if reply {
			b = cbor.AppendTextKey(b, "reply")
			b = cbor.AppendMapHeader(b, 2)
			b = cbor.AppendTextKey(b, "root")
			b = g.appendStrongRef(b, now)
			b = cbor.AppendTextKey(b, "parent")
			b = g.appendStrongRef(b, now)
		}
		b = appendKV(b, "createdAt", created)
	case "app.bsky.graph.follow", "app.bsky.graph.block":
		b = cbor.AppendMapHeader(b, 3)
		b = appendKV(b, "$type", coll)
		b = appendKV(b, "subject", g.did())
		b = appendKV(b, "createdAt", created)
	default:
		b = cbor.AppendMapHeader(b, 3)
		b = appendKV(b, "$type", coll)
		b = appendKV(b, "value", g.text())
		b = appendKV(b, "createdAt", created)
	}
	return b
}

func (g *generator) appendStrongRef(b []byte, now time.Time) []byte {
	c := g.cid()
	b = cbor.AppendMapHeader(b, 2)
	b = cbor.AppendTextKey(b, "cid")
	b = cbor.AppendCIDLink(b, &c)
	return appendKV(b, "uri", "at://"+g.did()+"/app.bsky.feed.post/"+g.tid(now))
}

func (g *generator) text() string {
	n := 3 + g.rng.IntN(35)
	var sb strings.Builder
	for i := range n {
		if i > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(words[g.rng.IntN(len(words))])
	}
	return sb.String()
}

func appendKV(b []byte, k, v string) []byte {
	return cbor.AppendText(cbor.AppendTextKey(b, k), v)
}
