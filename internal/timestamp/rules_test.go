package timestamp_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/timestamp"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

const (
	rulesDID        = "did:plc:alice"
	rulesCollection = "app.bsky.feed.post"
	rulesRkey       = "r1"
)

func TestRuleStoreStampAllVersions(t *testing.T) {
	t.Parallel()

	store := newRuleStore(t)
	importRuleCSV(t, store, "uri,timestamp,scope,cid",
		"at://"+rulesDID+"/"+rulesCollection+"/"+rulesRkey+",2021-12-20T11:33:20Z,all_versions,")

	ev := segment.Event{
		Kind:       segment.KindCreate,
		DID:        rulesDID,
		Collection: rulesCollection,
		Rkey:       rulesRkey,
		Payload:    []byte{0xa0},
	}
	require.NoError(t, store.Stamp(context.Background(), &ev))
	require.EqualValues(t, 1_640_000_000_000_000, ev.IndexedAt)
}

func TestRuleStoreSpecificVersionPrecedenceAndFallback(t *testing.T) {
	t.Parallel()

	store := newRuleStore(t)
	payloadV1 := []byte("payload-v1")
	payloadV2 := []byte("payload-v2")
	cidV1 := cbor.ComputeCID(cbor.CodecDagCBOR, payloadV1).String()

	importRuleCSV(t, store, "uri,timestamp,scope,cid",
		"at://"+rulesDID+"/"+rulesCollection+"/"+rulesRkey+",2021-12-20T11:33:20Z,all_versions,",
		"at://"+rulesDID+"/"+rulesCollection+"/"+rulesRkey+",2022-01-02T03:04:05Z,specific_version,"+cidV1)

	specific := segment.Event{
		Kind:       segment.KindCreate,
		DID:        rulesDID,
		Collection: rulesCollection,
		Rkey:       rulesRkey,
		Payload:    payloadV1,
	}
	require.NoError(t, store.Stamp(context.Background(), &specific))
	require.EqualValues(t, 1_641_092_645_000_000, specific.IndexedAt, "specific CID match wins over all_versions")

	fallback := segment.Event{
		Kind:       segment.KindUpdate,
		DID:        rulesDID,
		Collection: rulesCollection,
		Rkey:       rulesRkey,
		Payload:    payloadV2,
	}
	require.NoError(t, store.Stamp(context.Background(), &fallback))
	require.EqualValues(t, 1_640_000_000_000_000, fallback.IndexedAt, "CID miss falls back to all_versions")
}

func TestRuleStoreSkipsUnimportedAndNonMaterializationEvents(t *testing.T) {
	t.Parallel()

	store := newRuleStore(t)
	importRuleCSV(t, store, "uri,timestamp,scope,cid",
		"at://"+rulesDID+"/"+rulesCollection+"/"+rulesRkey+",2021-12-20T11:33:20Z,all_versions,")

	otherCollection := segment.Event{
		Kind:       segment.KindCreate,
		DID:        rulesDID,
		Collection: "app.bsky.feed.like",
		Rkey:       rulesRkey,
		Payload:    []byte{0xa0},
	}
	require.NoError(t, store.Stamp(context.Background(), &otherCollection))
	require.Zero(t, otherCollection.IndexedAt)

	deleteEvent := segment.Event{
		Kind:       segment.KindDelete,
		DID:        rulesDID,
		Collection: rulesCollection,
		Rkey:       rulesRkey,
	}
	require.NoError(t, store.Stamp(context.Background(), &deleteEvent))
	require.Zero(t, deleteEvent.IndexedAt)
}

func TestRuleStoreDuplicateKeyLastRowWinsAcrossChunks(t *testing.T) {
	t.Parallel()

	store := newRuleStore(t)
	result := importRuleCSVWithChunkRows(t, store, 2, "uri,timestamp,scope,cid",
		"at://"+rulesDID+"/"+rulesCollection+"/"+rulesRkey+",2021-12-20T11:33:20Z,all_versions,",
		"at://"+rulesDID+"/"+rulesCollection+"/"+rulesRkey+",2022-01-02T03:04:05Z,all_versions,",
		"at://"+rulesDID+"/"+rulesCollection+"/"+rulesRkey+",2023-03-04T05:06:07Z,all_versions,")
	require.EqualValues(t, 3, result.Rules)
	require.GreaterOrEqual(t, result.SSTables, 2, "small chunks should exercise sequential ingest overwrite")
	require.Greater(t, result.BytesIngest, uint64(0))

	ev := segment.Event{
		Kind:       segment.KindCreate,
		DID:        rulesDID,
		Collection: rulesCollection,
		Rkey:       rulesRkey,
		Payload:    []byte{0xa0},
	}
	require.NoError(t, store.Stamp(context.Background(), &ev))
	require.EqualValues(t, 1_677_906_367_000_000, ev.IndexedAt)
}

func TestRuleStoreParseFailureDoesNotIngestPartialChunks(t *testing.T) {
	t.Parallel()

	store := newRuleStore(t)
	path := filepath.Join(t.TempDir(), "bad-rules.csv")
	src := "uri,timestamp,scope,cid\n" +
		"at://" + rulesDID + "/" + rulesCollection + "/" + rulesRkey + ",2021-12-20T11:33:20Z,all_versions,\n" +
		"\"unterminated\n"
	require.NoError(t, os.WriteFile(path, []byte(src), 0o644))

	_, err := store.ImportRulesFromCSV(context.Background(), timestamp.RuleIngestConfig{
		CSVPath:    path,
		ScratchDir: filepath.Join(t.TempDir(), "scratch"),
		ChunkRows:  2,
	})
	require.Error(t, err)
	require.False(t, store.HasRules(), "failed parse must not activate collection metadata")

	ev := segment.Event{
		Kind:       segment.KindCreate,
		DID:        rulesDID,
		Collection: rulesCollection,
		Rkey:       rulesRkey,
		Payload:    []byte{0xa0},
	}
	require.NoError(t, store.Stamp(context.Background(), &ev))
	require.Zero(t, ev.IndexedAt, "failed parse must not leave a partial rule active")
}

func TestRuleStoreReloadsCollectionsOnReopen(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	store, err := timestamp.OpenRuleStore(timestamp.RuleStoreConfig{DataDir: dataDir})
	require.NoError(t, err)
	importRuleCSV(t, store, "uri,timestamp,scope,cid",
		"at://"+rulesDID+"/"+rulesCollection+"/"+rulesRkey+",2021-12-20T11:33:20Z,all_versions,")
	require.True(t, store.HasRules())
	require.NoError(t, store.Close())

	reopened, err := timestamp.OpenRuleStore(timestamp.RuleStoreConfig{DataDir: dataDir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.True(t, reopened.HasRules(), "collection filter must reload from durable keys")

	ev := segment.Event{
		Kind:       segment.KindCreate,
		DID:        rulesDID,
		Collection: rulesCollection,
		Rkey:       rulesRkey,
		Payload:    []byte{0xa0},
	}
	require.NoError(t, reopened.Stamp(context.Background(), &ev))
	require.EqualValues(t, 1_640_000_000_000_000, ev.IndexedAt)
}

func newRuleStore(t *testing.T) *timestamp.RuleStore {
	t.Helper()
	store, err := timestamp.OpenRuleStore(timestamp.RuleStoreConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func importRuleCSV(t *testing.T, store *timestamp.RuleStore, header string, rows ...string) timestamp.RuleIngestResult {
	t.Helper()
	return importRuleCSVWithChunkRows(t, store, 0, header, rows...)
}

func importRuleCSVWithChunkRows(t *testing.T, store *timestamp.RuleStore, chunkRows int, header string, rows ...string) timestamp.RuleIngestResult {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rules.csv")
	var b strings.Builder
	b.WriteString(header)
	b.WriteByte('\n')
	for _, row := range rows {
		b.WriteString(row)
		b.WriteByte('\n')
	}
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
	result, err := store.ImportRulesFromCSV(context.Background(), timestamp.RuleIngestConfig{
		CSVPath:    path,
		ScratchDir: filepath.Join(t.TempDir(), "scratch"),
		ChunkRows:  chunkRows,
	})
	require.NoError(t, err)
	return result
}
