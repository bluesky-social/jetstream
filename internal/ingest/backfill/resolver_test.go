package backfill

import (
	"context"
	"errors"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

func TestRecordingResolver_RecordsResolvedHostAndDeclaredHandle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	did := atmos.DID("did:plc:alice")
	bs := newTestStore(t)
	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))

	rr := &recordingResolver{
		inner: &fakeResolver{
			doc: &identity.DIDDocument{
				ID:          string(did),
				AlsoKnownAs: []string{"at://Alice.Example.COM"},
				Service: []identity.Service{
					{Type: "AtprotoPersonalDataServer", ServiceEndpoint: "https://PDS.Example.COM:443"},
				},
			},
		},
		store: bs,
	}

	doc, err := rr.ResolveDID(ctx, did)
	require.NoError(t, err)
	require.Equal(t, string(did), doc.ID)

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, "alice.example.com", rs.Handle)
	require.Equal(t, "https://PDS.Example.COM:443", rs.PDS)
	require.Equal(t, "pds.example.com", rs.Host)

	got, ok, err := lookupDIDByHandle(bs.db, "alice.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, did, got)

	hs, ok, err := loadHostStatus(bs.db, "pds.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "pds.example.com", hs.Host)
}

func TestRecordingResolver_RecordsUnresolvedBucketOnResolveDIDError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	did := atmos.DID("did:plc:missing")
	bs := newTestStore(t)
	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))

	rr := &recordingResolver{
		inner: &fakeResolver{err: errors.New("resolution failed")},
		store: bs,
	}

	_, err := rr.ResolveDID(ctx, did)
	require.ErrorContains(t, err, "resolution failed")
	require.False(t, errors.Is(err, errIdentityDiagnosticsPersistence))

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, HostBucketUnresolved, rs.Host)
}

func TestRecordingResolver_DoesNotRecordUnresolvedBucketOnCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	did := atmos.DID("did:plc:cancelled")
	bs := newTestStore(t)
	require.NoError(t, bs.OnDiscover(context.Background(), atmossync.ListReposEntry{DID: did, Active: true}))
	require.NoError(t, bs.recordIdentityResolution(context.Background(), did, IdentityResolution{
		PDS:  "https://pds.example.com",
		Host: "pds.example.com",
	}))

	rr := &recordingResolver{
		inner: &fakeResolver{err: context.Canceled},
		store: bs,
	}

	_, err := rr.ResolveDID(ctx, did)
	require.ErrorIs(t, err, context.Canceled)

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, "pds.example.com", rs.Host)
}

func TestRecordingResolver_RecordsInvalidPDSWhenDocumentHasNoUsableEndpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	did := atmos.DID("did:plc:nopds")
	bs := newTestStore(t)
	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))

	rr := &recordingResolver{
		inner: &fakeResolver{
			doc: &identity.DIDDocument{
				ID: string(did),
				Service: []identity.Service{
					{Type: "AtprotoPersonalDataServer", ServiceEndpoint: "mailto:pds@example.com"},
				},
			},
		},
		store: bs,
	}

	doc, err := rr.ResolveDID(ctx, did)
	require.NoError(t, err)
	require.Equal(t, string(did), doc.ID)

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, HostBucketInvalidPDS, rs.Host)
}

func TestDirectoryWithRecordingResolver_DoesNotMutateOriginalDirectory(t *testing.T) {
	t.Parallel()

	inner := &fakeResolver{}
	cache := &fakeIdentityCache{}
	dir := &identity.Directory{
		Resolver:               inner,
		Cache:                  cache,
		SkipHandleVerification: false,
	}
	bs := newTestStore(t)

	copy := directoryWithRecordingResolver(dir, bs, nil)

	require.Same(t, inner, dir.Resolver)
	require.Same(t, cache, dir.Cache)
	require.False(t, dir.SkipHandleVerification)
	require.Same(t, cache, copy.Cache)
	require.True(t, copy.SkipHandleVerification)

	rr, ok := copy.Resolver.(*recordingResolver)
	require.True(t, ok)
	require.Same(t, inner, rr.inner)
	require.Same(t, bs, rr.store)
}

type fakeResolver struct {
	doc *identity.DIDDocument
	err error
}

var _ identity.Resolver = (*fakeResolver)(nil)

func (r *fakeResolver) ResolveDID(_ context.Context, _ atmos.DID) (*identity.DIDDocument, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.doc, nil
}

func (r *fakeResolver) ResolveHandle(_ context.Context, _ atmos.Handle) (atmos.DID, error) {
	return "", identity.ErrHandleNotFound
}

type fakeIdentityCache struct{}

var _ identity.Cache = (*fakeIdentityCache)(nil)

func (c *fakeIdentityCache) Get(context.Context, string) (*identity.Identity, bool) {
	return nil, false
}

func (c *fakeIdentityCache) Set(context.Context, string, *identity.Identity) {}

func (c *fakeIdentityCache) Delete(context.Context, string) {}
