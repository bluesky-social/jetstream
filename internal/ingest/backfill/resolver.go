package backfill

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
)

type recordingResolver struct {
	inner              identity.Resolver
	store              *Store
	onPersistenceError func(error)
}

var errIdentityDiagnosticsPersistence = errors.New("backfill: identity diagnostics persistence failed")

var _ identity.Resolver = (*recordingResolver)(nil)

func (r *recordingResolver) ResolveDID(ctx context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	doc, err := r.inner.ResolveDID(ctx, did)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			return nil, err
		}
		recordErr := r.store.recordIdentityResolution(ctx, did, IdentityResolution{Host: HostBucketUnresolved})
		if recordErr != nil {
			return nil, r.persistenceError("record unresolved DID %s: %w", did, errors.Join(err, recordErr))
		}
		return nil, err
	}
	if doc == nil {
		recordErr := r.store.recordIdentityResolution(ctx, did, IdentityResolution{Host: HostBucketInvalidPDS})
		if recordErr != nil {
			return nil, r.persistenceError("record invalid PDS for nil DID document %s: %w", did, recordErr)
		}
		return nil, fmt.Errorf("backfill: resolve DID %s: resolver returned nil document", did)
	}

	resolution := IdentityResolution{
		Handle: declaredHandle(doc),
		Host:   HostBucketInvalidPDS,
	}
	for _, svc := range doc.Service {
		if svc.Type != "AtprotoPersonalDataServer" {
			continue
		}
		resolution.PDS = svc.ServiceEndpoint
		if host, ok := normalizeHostBucket(svc.ServiceEndpoint); ok {
			resolution.Host = host
		}
		break
	}

	if err := r.store.recordIdentityResolution(ctx, did, resolution); err != nil {
		return nil, r.persistenceError("record identity resolution %s: %w", did, err)
	}
	return doc, nil
}

func (r *recordingResolver) ResolveHandle(ctx context.Context, handle atmos.Handle) (atmos.DID, error) {
	return r.inner.ResolveHandle(ctx, handle)
}

func (r *recordingResolver) persistenceError(format string, args ...any) error {
	err := fmt.Errorf("%w: "+format, append([]any{errIdentityDiagnosticsPersistence}, args...)...)
	if r.onPersistenceError != nil {
		r.onPersistenceError(err)
	}
	return err
}

func declaredHandle(doc *identity.DIDDocument) string {
	for _, aka := range doc.AlsoKnownAs {
		if !strings.HasPrefix(aka, "at://") {
			continue
		}
		handle, err := atmos.ParseHandle(strings.TrimPrefix(aka, "at://"))
		if err == nil {
			return string(handle.Normalize())
		}
	}
	return ""
}

func directoryWithRecordingResolver(dir *identity.Directory, st *Store, onPersistenceError func(error)) *identity.Directory {
	return &identity.Directory{
		Resolver: &recordingResolver{
			inner:              dir.Resolver,
			store:              st,
			onPersistenceError: onPersistenceError,
		},
		Cache:                  dir.Cache,
		SkipHandleVerification: true,
	}
}
