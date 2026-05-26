package world

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jcalabro/atmos"
)

// Realistic-distribution constants. Lifting these into named consts
// makes them tunable in one place, per the design doc.
const (
	collPost    = "app.bsky.feed.post"
	collLike    = "app.bsky.feed.like"
	collFollow  = "app.bsky.graph.follow"
	collRepost  = "app.bsky.feed.repost"
	collProfile = "app.bsky.actor.profile"
)

// collectionWeights is the design-doc realistic mix for create ops.
var collectionWeights = []weighted[string]{
	{value: collPost, weight: 60},
	{value: collLike, weight: 20},
	{value: collFollow, weight: 10},
	{value: collRepost, weight: 5},
	{value: collProfile, weight: 5},
}

// chooseCreateCollection picks a collection NSID for a create op.
func chooseCreateCollection(r *rand.Rand) string {
	return weightedChoice(r, collectionWeights)
}

// newRkey generates a fresh TID record key. Real PDSes generate TIDs
// from wall clock + a clock id; here we use the global RNG to keep
// runs deterministic.
func newRkey(r *rand.Rand) string {
	micros := int64(2000) * int64(time.Hour) / int64(time.Microsecond) // arbitrary baseline
	micros += r.Int64N(1 << 40)
	clockID := r.UintN(1024)
	return string(atmos.NewTID(micros, clockID))
}

// generateRecord builds a payload for the given collection. target is
// the DID a like/follow/repost is aimed at (caller picks a random
// other account); ignored for post/profile.
func generateRecord(r *rand.Rand, collection, target string) map[string]any {
	createdAt := time.Unix(0, r.Int64N(1<<60)).UTC().Format(time.RFC3339)
	switch collection {
	case collPost:
		length := logNormalClamped(r, 4.0, 1.0, 1, 3000)
		return map[string]any{
			"$type":     collPost,
			"text":      randomText(r, length),
			"createdAt": createdAt,
			"langs":     []any{"en"},
		}
	case collLike:
		return map[string]any{
			"$type":     collLike,
			"createdAt": createdAt,
			"subject": map[string]any{
				"uri": fmt.Sprintf("at://%s/app.bsky.feed.post/%s", target, newRkey(r)),
				"cid": fakeCIDString(r),
			},
		}
	case collFollow:
		return map[string]any{
			"$type":     collFollow,
			"createdAt": createdAt,
			"subject":   target,
		}
	case collRepost:
		return map[string]any{
			"$type":     collRepost,
			"createdAt": createdAt,
			"subject": map[string]any{
				"uri": fmt.Sprintf("at://%s/app.bsky.feed.post/%s", target, newRkey(r)),
				"cid": fakeCIDString(r),
			},
		}
	case collProfile:
		return map[string]any{
			"$type":       collProfile,
			"displayName": randomText(r, logNormalClamped(r, 2.5, 0.6, 1, 64)),
			"description": randomText(r, logNormalClamped(r, 4.0, 0.8, 0, 256)),
		}
	default:
		// Forward-compat: unknown collection gets a benign empty
		// record. We never produce these in v1, but defensive code is
		// cheap.
		return map[string]any{"$type": collection}
	}
}

// randomText returns a string of length n composed of lowercase
// letters and spaces. Not artistic; the bytes are what matter for
// codec round-trips.
func randomText(r *rand.Rand, n int) string {
	if n <= 0 {
		return ""
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyz "
	out := make([]byte, n)
	for i := range out {
		out[i] = alphabet[r.IntN(len(alphabet))]
	}
	return string(out)
}

// fakeCIDString returns a syntactically-plausible CIDv1 base32 string
// for record subjects in like/repost payloads. Not derived from the
// referenced post; we don't track inter-record references in v1.
func fakeCIDString(r *rand.Rand) string {
	// Real CIDs are way more structured; this is just a deterministic
	// 59-char base32 string starting with 'b' (CIDv1 base32 prefix).
	const alphabet = "abcdefghijklmnopqrstuvwxyz234567"
	out := make([]byte, 59)
	out[0] = 'b'
	for i := 1; i < len(out); i++ {
		out[i] = alphabet[r.IntN(len(alphabet))]
	}
	return string(out)
}
