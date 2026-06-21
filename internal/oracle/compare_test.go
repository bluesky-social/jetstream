package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompareReportsMissingRecord(t *testing.T) {
	t.Parallel()

	key := RecordKey{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1"}
	want := &Model{Accounts: map[string]RepoSnapshot{
		"did:plc:a": {Records: map[RecordKey]RecordValue{key: {Rev: "r1", Payload: []byte("p")}}},
	}}
	got := &Model{Accounts: map[string]RepoSnapshot{"did:plc:a": {Records: map[RecordKey]RecordValue{}}}}

	err := Compare(want, got)
	require.ErrorContains(t, err, "missing")
	require.ErrorContains(t, err, "did:plc:a")
	require.ErrorContains(t, err, "app.bsky.feed.post/r1")
}

func TestCompareReportsPayloadMismatch(t *testing.T) {
	t.Parallel()

	key := RecordKey{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1"}
	want := modelWithRecord(key, RecordValue{Rev: "r1", Payload: []byte("abcd")})
	got := modelWithRecord(key, RecordValue{Rev: "r1", Payload: []byte("abXd")})

	err := Compare(want, got)
	require.ErrorContains(t, err, "payload mismatch")
	require.ErrorContains(t, err, "did:plc:a")
	require.ErrorContains(t, err, "app.bsky.feed.post/r1")
	require.ErrorContains(t, err, "first_diff=2")
	require.ErrorContains(t, err, "want len=4")
	require.ErrorContains(t, err, "got len=4")
	require.ErrorContains(t, err, `rev="r1"`)
	require.ErrorContains(t, err, "61626364")
	require.ErrorContains(t, err, "61625864")
}

// TestCompareIgnoresRevEntirely pins the #104 contract: the final-state
// tier does NOT compare rev. Ground truth cannot populate a correct
// per-record rev (and a #sync-resync collapses all records to the sync
// rev), so a differing rev with equal payload must NOT fail Compare — rev
// correctness is the event-log tier's job. Covers both-sides-set,
// one-side-set, and mismatched-rev cases.
func TestCompareIgnoresRevEntirely(t *testing.T) {
	t.Parallel()

	key := RecordKey{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1"}

	// One side has a rev, the other doesn't.
	want := modelWithRecord(key, RecordValue{Rev: "r1", Payload: []byte("p")})
	got := modelWithRecord(key, RecordValue{Payload: []byte("p")})
	require.NoError(t, Compare(want, got))
	require.NoError(t, Compare(got, want))

	// Both sides have DIFFERENT revs but equal payload: still no error,
	// because rev is owned by the event-log tier, not final-state.
	wantR := modelWithRecord(key, RecordValue{Rev: "r1", Payload: []byte("p")})
	gotR := modelWithRecord(key, RecordValue{Rev: "r2", Payload: []byte("p")})
	require.NoError(t, Compare(wantR, gotR))
	require.NoError(t, Compare(gotR, wantR))
}

func TestCompareReportsExtraRecord(t *testing.T) {
	t.Parallel()

	key := RecordKey{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1"}
	want := &Model{Accounts: map[string]RepoSnapshot{"did:plc:a": {Records: map[RecordKey]RecordValue{}}}}
	got := modelWithRecord(key, RecordValue{Payload: []byte("p")})

	err := Compare(want, got)
	require.ErrorContains(t, err, "extra")
	require.ErrorContains(t, err, "did:plc:a")
	require.ErrorContains(t, err, "app.bsky.feed.post/r1")
}

func TestCompareReportsExtraAccountWithRecords(t *testing.T) {
	t.Parallel()

	key := RecordKey{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1"}
	got := modelWithRecord(key, RecordValue{Payload: []byte("p")})

	err := Compare(&Model{}, got)
	require.ErrorContains(t, err, "extra account")
	require.ErrorContains(t, err, "did:plc:a")
}

func TestCompareIgnoresMissingAndExtraEmptyAccounts(t *testing.T) {
	t.Parallel()

	want := &Model{Accounts: map[string]RepoSnapshot{
		"did:plc:a": {Records: map[RecordKey]RecordValue{}},
	}}
	got := &Model{Accounts: map[string]RepoSnapshot{
		"did:plc:b": {Records: map[RecordKey]RecordValue{}},
	}}

	require.NoError(t, Compare(want, got))
}

func TestCompareNilSafety(t *testing.T) {
	t.Parallel()

	require.NoError(t, Compare(nil, nil))
	require.NoError(t, Compare(&Model{}, &Model{Accounts: map[string]RepoSnapshot{"did:plc:a": {}}}))

	key := RecordKey{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1"}
	err := Compare(nil, modelWithRecord(key, RecordValue{Payload: []byte("p")}))
	require.ErrorContains(t, err, "extra account")

	err = Compare(modelWithRecord(key, RecordValue{Payload: []byte("p")}), nil)
	require.ErrorContains(t, err, "missing")
	require.ErrorContains(t, err, "did:plc:a")
	require.ErrorContains(t, err, "app.bsky.feed.post/r1")
}

func TestCompareRejectsRecordKeyUnderWrongAccount(t *testing.T) {
	t.Parallel()

	key := RecordKey{DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "r1"}
	want := &Model{Accounts: map[string]RepoSnapshot{
		"did:plc:a": {Records: map[RecordKey]RecordValue{key: {Payload: []byte("p")}}},
	}}

	err := Compare(want, &Model{})
	require.ErrorContains(t, err, "want account did:plc:a contains record key for did:plc:b")

	got := &Model{Accounts: map[string]RepoSnapshot{
		"did:plc:a": {Records: map[RecordKey]RecordValue{key: {Payload: []byte("p")}}},
	}}
	err = Compare(&Model{}, got)
	require.ErrorContains(t, err, "got account did:plc:a contains record key for did:plc:b")
}

func modelWithRecord(key RecordKey, value RecordValue) *Model {
	return &Model{Accounts: map[string]RepoSnapshot{
		key.DID: {Records: map[RecordKey]RecordValue{key: value}},
	}}
}
