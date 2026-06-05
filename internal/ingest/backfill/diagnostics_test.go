package backfill

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/stretchr/testify/require"
)

func TestNormalizeHostBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{name: "https default port", in: "https://Bsky.Social:443", want: "bsky.social", ok: true},
		{name: "http default port", in: "http://pds.example.test:80", want: "pds.example.test", ok: true},
		{name: "explicit nondefault port", in: "https://pds.example.test:8443", want: "pds.example.test:8443", ok: true},
		{name: "missing host", in: "https:///x", ok: false},
		{name: "unsupported scheme", in: "ftp://pds.example.test", ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := normalizeHostBucket(tc.in)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestClassifyBackfillError(t *testing.T) {
	t.Parallel()

	require.Equal(t, ErrorClassDIDResolution, classifyBackfillError(errors.New("identity: DID not found")))
	require.Equal(t, ErrorClassInvalidPDS, classifyBackfillError(errors.New("missing AtprotoPersonalDataServer endpoint")))
	require.Equal(t, ErrorClassHTTP429, classifyBackfillError(errors.New("xrpc: HTTP 429: rate limited")))
	require.Equal(t, ErrorClassHTTP5xx, classifyBackfillError(errors.New("xrpc: HTTP 503: unavailable")))
	require.Equal(t, ErrorClassTimeout, classifyBackfillError(context.DeadlineExceeded))
	require.Equal(t, ErrorClassCAR, classifyBackfillError(errors.New("car: invalid header")))
	require.Equal(t, ErrorClassVerification, classifyBackfillError(errors.New("verify commit: signature mismatch")))
	require.Equal(t, ErrorClassLocalWrite, classifyBackfillError(errors.New("flush before complete: disk full")))
	require.Equal(t, ErrorClassUnknown, classifyBackfillError(errors.New("other")))
	require.Equal(t, ErrorClassUnknown, classifyBackfillError(errors.New("not actually HTTP 5")))
}

func TestIsRepoNotFoundError(t *testing.T) {
	t.Parallel()

	repoNotFound := &xrpc.Error{
		StatusCode: 400,
		Name:       "RepoNotFound",
		Message:    "Could not find repo for DID: did:plc:missing",
	}
	require.True(t, isRepoNotFoundError(repoNotFound))
	require.True(t, isRepoNotFoundError(fmt.Errorf("wrapped: %w", repoNotFound)))
	require.False(t, shouldLogBackfillError(repoNotFound))

	require.False(t, isRepoNotFoundError(&xrpc.Error{StatusCode: 400, Name: "InvalidRequest"}))
	require.False(t, isRepoNotFoundError(errors.New("xrpc 400 RepoNotFound: text only")))
	require.True(t, shouldLogBackfillError(&xrpc.Error{StatusCode: 400, Name: "InvalidRequest"}))
}

func TestHostStatus_AddErrorSampleKeepsLatestFive(t *testing.T) {
	t.Parallel()

	var hs HostStatus
	base := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	for i := range 7 {
		hs.addErrorSample(HostErrorSample{
			DID:         atmos.DID("did:plc:sample"),
			AttemptedAt: base.Add(time.Duration(i) * time.Minute),
			Class:       ErrorClassHTTP5xx,
			Error:       "boom",
		})
	}

	require.Len(t, hs.RecentErrors, 5)
	require.True(t, hs.RecentErrors[0].AttemptedAt.Equal(base.Add(6*time.Minute)))
	require.True(t, hs.RecentErrors[4].AttemptedAt.Equal(base.Add(2*time.Minute)))
	require.Equal(t, "boom", hs.LatestError)
	require.Equal(t, ErrorClassHTTP5xx, hs.LatestErrorClass)
	require.Equal(t, uint64(7), hs.ErrorClassCounts[ErrorClassHTTP5xx])
}

func TestHostStatus_RoundTripIncludesDiagnosticMetadata(t *testing.T) {
	t.Parallel()

	attemptedAt := time.Date(2026, 6, 4, 12, 30, 0, 0, time.UTC)
	in := &HostStatus{
		Host:             "pds.example.test",
		Total:            11,
		Active:           10,
		NotStarted:       1,
		Complete:         8,
		Failed:           2,
		LastAttemptedAt:  attemptedAt,
		LatestError:      "xrpc: HTTP 503: unavailable",
		LatestErrorClass: ErrorClassHTTP5xx,
		ErrorClassCounts: map[ErrorClass]uint64{
			ErrorClassHTTP429: 1,
			ErrorClassHTTP5xx: 2,
		},
		RecentErrors: []HostErrorSample{{
			DID:         atmos.DID("did:plc:sample"),
			AttemptedAt: attemptedAt,
			Class:       ErrorClassHTTP5xx,
			Error:       "xrpc: HTTP 503: unavailable",
		}},
	}

	enc, err := encodeHostStatus(in)
	require.NoError(t, err)
	out, err := decodeHostStatus(enc)
	require.NoError(t, err)

	require.Equal(t, in, out)
	require.NotNil(t, out.ErrorClassCounts)
}

func TestHostStatusDecodeInitializesErrorClassCounts(t *testing.T) {
	t.Parallel()

	got, err := decodeHostStatus([]byte(`{"host":"pds.example.test"}`))
	require.NoError(t, err)
	require.NotNil(t, got.ErrorClassCounts)
	require.Empty(t, got.ErrorClassCounts)
}

func TestHostStatusLoadNotFoundInitializesErrorClassCounts(t *testing.T) {
	t.Parallel()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	got, ok, err := loadHostStatus(st, "pds.example.test")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "pds.example.test", got.Host)
	require.NotNil(t, got.ErrorClassCounts)
	require.Empty(t, got.ErrorClassCounts)
}

func TestHostStatusRejectsEmptyHost(t *testing.T) {
	t.Parallel()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	_, _, err = loadHostStatus(st, " \t ")
	require.Error(t, err)

	batch := st.NewBatch()
	defer func() { _ = batch.Close() }()
	require.Error(t, stageHostStatus(batch, &HostStatus{Host: " \t "}))
}

func TestHostStatusNormalizesPersistenceHost(t *testing.T) {
	t.Parallel()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	batch := st.NewBatch()
	require.NoError(t, stageHostStatus(batch, &HostStatus{
		Host:             " PDS.Example.TEST ",
		Total:            3,
		ErrorClassCounts: map[ErrorClass]uint64{},
	}))
	require.NoError(t, st.Commit(batch, store.SyncWrites))
	require.NoError(t, batch.Close())

	got, ok, err := loadHostStatus(st, "pds.example.test")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "pds.example.test", got.Host)
	require.Equal(t, uint64(3), got.Total)

	got, ok, err = loadHostStatus(st, " PDS.EXAMPLE.TEST ")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "pds.example.test", got.Host)

	_, closer, err := st.Get([]byte(hostKeyPrefix + "PDS.Example.TEST"))
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestTruncateErrorString(t *testing.T) {
	t.Parallel()

	got := truncateErrorString(string(make([]byte, maxStoredErrorBytes+100)))
	require.Len(t, got, maxStoredErrorBytes)
}

func TestHandleIndexRoundTrip(t *testing.T) {
	t.Parallel()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, saveHandleIndex(st, "Alice.Example.COM", atmos.DID("did:plc:alice")))
	got, ok, err := lookupDIDByHandle(st, "alice.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, atmos.DID("did:plc:alice"), got)

	require.NoError(t, deleteHandleIndex(st, "alice.example.com"))
	_, ok, err = lookupDIDByHandle(st, "alice.example.com")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestHandleIndexBlankHandleNoops(t *testing.T) {
	t.Parallel()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, saveHandleIndex(st, "", atmos.DID("did:plc:alice")))
	_, closer, err := st.Get([]byte(handleKeyPrefix))
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.ErrorIs(t, err, store.ErrNotFound)

	require.NoError(t, st.Set([]byte(handleKeyPrefix), []byte("sentinel"), store.SyncWrites))
	require.NoError(t, saveHandleIndex(st, " \t ", atmos.DID("did:plc:bob")))
	val, closer, err := st.Get([]byte(handleKeyPrefix))
	require.NoError(t, err)
	require.Equal(t, "sentinel", string(val))
	require.NoError(t, closer.Close())

	got, ok, err := lookupDIDByHandle(st, " \t ")
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, got)

	require.NoError(t, deleteHandleIndex(st, " \t "))
	val, closer, err = st.Get([]byte(handleKeyPrefix))
	require.NoError(t, err)
	require.Equal(t, "sentinel", string(val))
	require.NoError(t, closer.Close())
}

func TestHandleIndexLookupInvalidDIDErrors(t *testing.T) {
	t.Parallel()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, st.Set([]byte(handleKeyPrefix+"bad.example"), []byte("not-a-did"), store.SyncWrites))
	_, ok, err := lookupDIDByHandle(st, "bad.example")
	require.Error(t, err)
	require.False(t, ok)
}
