package protocol

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/sync/errgroup"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/obs"
)

var tracer = obs.Tracer("objstore")

// DefaultUploadConcurrency matches JETSTREAM_S3_UPLOAD_CONCURRENCY's default.
const DefaultUploadConcurrency = 8

// maxUploadRounds bounds §7.3's "retry from step 3 with a new key". A store
// that keeps returning the wrong bytes for fresh keys is broken, not flaky.
const maxUploadRounds = 3

// errStale is the skip-PUT rule firing.
var errStale = errors.New("upload stalled past orphan_age/2 since its uploading row committed; skipping the PUT")

// UploaderConfig configures an Uploader.
type UploaderConfig struct {
	Blob      objstore.Blob
	ArchiveID [16]byte
	// GCDelay and OrphanAge are JETSTREAM_GC_DELAY and JETSTREAM_GC_ORPHAN_AGE.
	// Dedup only reuses objects unreferenced for less than GCDelay/2, and a
	// PUT is skipped once OrphanAge/2 has passed since its row committed.
	GCDelay   time.Duration
	OrphanAge time.Duration
	// Concurrency bounds the PUT and read-back pairs in flight per Upload.
	// Zero means DefaultUploadConcurrency.
	Concurrency int
	Metrics     *Metrics
}

// Uploader runs the §7.3 upload protocol. It is safe for concurrent use.
type Uploader struct {
	cfg UploaderConfig
}

// NewUploader validates cfg and returns an Uploader.
func NewUploader(cfg UploaderConfig) (*Uploader, error) {
	if cfg.Blob == nil {
		return nil, errors.New("protocol: uploader needs a Blob")
	}
	if cfg.GCDelay <= 0 || cfg.OrphanAge <= 0 {
		return nil, fmt.Errorf("protocol: gc delay %v and orphan age %v must be positive", cfg.GCDelay, cfg.OrphanAge)
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultUploadConcurrency
	}
	return &Uploader{cfg: cfg}, nil
}

// Upload stores objs and returns one reference per input, in order (§7.3
// steps 1 to 5). A deduplicated object comes back ready to reference. A
// fresh upload comes back Pending: the caller either hands the ref to the
// transaction that first references it, which makes it available (step 6
// folded in), or calls Session.MarkAvailable. Identical inputs share one
// object.
//
// The uploading rows for the whole batch commit in one transaction before
// any PUT. Any failure ends s, as §7.3 requires of every caller: the
// returned error wraps catalog.ErrSessionEnded, or is a CorruptionError.
// Leftover uploading rows and keys are GC's to reclaim.
func (u *Uploader) Upload(ctx context.Context, s *catalog.Session, objs [][]byte) ([]catalog.ObjectRef, error) {
	ctx, span := tracer.Start(ctx, "objstore.Upload")
	defer span.End()
	span.SetAttributes(attribute.Int("objstore.objects", len(objs)))

	refs, err := u.upload(ctx, s, objs)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "upload failed")
		return nil, err
	}
	ids := make([]string, len(refs))
	for i, r := range refs {
		ids[i] = strconv.FormatUint(r.ID, 10)
	}
	span.SetAttributes(attribute.StringSlice("objstore.object_ids", ids))
	return refs, nil
}

// Put uploads one object and makes it available in its own transaction. It
// returns the ID to reference: its own row, a deduplicated object, or the
// row of a concurrent upload of the same bytes that became available first.
// Callers that can fold step 6 into their referencing transaction should use
// Upload instead and save the transaction.
func (u *Uploader) Put(ctx context.Context, s *catalog.Session, data []byte) (catalog.ObjectRef, error) {
	refs, err := u.Upload(ctx, s, [][]byte{data})
	if err != nil {
		return catalog.ObjectRef{}, err
	}
	ref := refs[0]
	if !ref.Pending {
		return ref, nil
	}
	id, err := s.MarkAvailable(ctx, ref)
	if err != nil {
		return catalog.ObjectRef{}, err
	}
	return catalog.ObjectRef{ID: id, SHA256: ref.SHA256}, nil
}

// pendingUpload is one distinct object still needing a verified copy.
type pendingUpload struct {
	data    []byte
	sha     [32]byte
	lastErr error
}

func (u *Uploader) upload(ctx context.Context, s *catalog.Session, objs [][]byte) ([]catalog.ObjectRef, error) {
	if err := s.Err(); err != nil {
		return nil, err
	}
	// Distinct objects by content; at[i] is input i's object.
	var todo []*pendingUpload
	bySHA := make(map[[32]byte]int)
	at := make([]int, len(objs))
	for i, data := range objs {
		sha := sha256.Sum256(data)
		j, ok := bySHA[sha]
		if !ok {
			j = len(todo)
			bySHA[sha] = j
			todo = append(todo, &pendingUpload{data: data, sha: sha})
		}
		at[i] = j
	}

	done := make([]catalog.ObjectRef, len(todo))
	remaining := make([]int, len(todo))
	for j := range todo {
		remaining[j] = j
	}
	for range maxUploadRounds {
		var err error
		if remaining, err = u.round(ctx, s, todo, remaining, done); err != nil {
			return nil, err
		}
		if len(remaining) == 0 {
			refs := make([]catalog.ObjectRef, len(objs))
			for i, j := range at {
				refs[i] = done[j]
			}
			return refs, nil
		}
	}
	p := todo[remaining[0]]
	return nil, s.End("upload", fmt.Errorf("object sha256 %x: read-back failed %d times with fresh keys: %w",
		p.sha, maxUploadRounds, p.lastErr))
}

// round runs steps 2 to 5 for the objects in remaining, filling done, and
// returns the objects whose read-back failed and need a fresh key.
func (u *Uploader) round(ctx context.Context, s *catalog.Session, todo []*pendingUpload, remaining []int, done []catalog.ObjectRef) ([]int, error) {
	reqs := make([]catalog.UploadRequest, len(remaining))
	for k, j := range remaining {
		reqs[k] = catalog.UploadRequest{Key: objstore.NewUUID(), SHA256: todo[j].sha, Length: int64(len(todo[j].data))}
	}
	slots, committed, err := s.BeginUploads(ctx, reqs, u.cfg.GCDelay/2)
	if err != nil {
		return nil, err
	}

	retry := make([]bool, len(remaining))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(u.cfg.Concurrency)
	for k, j := range remaining {
		p := todo[j]
		if slots[k].Dedup {
			done[j] = catalog.ObjectRef{ID: slots[k].ObjectID, SHA256: p.sha}
			continue
		}
		g.Go(func() error {
			key := objstore.Key(u.cfg.ArchiveID, reqs[k].Key)
			err := u.putVerify(gctx, key, p, committed)
			switch {
			case err == nil:
				done[j] = catalog.ObjectRef{ID: slots[k].ObjectID, SHA256: p.sha, Pending: true}
			case errors.Is(err, objstore.ErrCorrupt) || errors.Is(err, objstore.ErrNotFound):
				// Step 5: the row stays uploading for GC; retry from step 3.
				u.cfg.Metrics.verifyFailed(pathUpload)
				p.lastErr = fmt.Errorf("object %d key %s: %w", slots[k].ObjectID, key, err)
				retry[k] = true
			default:
				return fmt.Errorf("object %d key %s: %w", slots[k].ObjectID, key, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, s.End("upload", err)
	}
	var next []int
	for k, j := range remaining {
		if retry[k] {
			next = append(next, j)
		}
	}
	return next, nil
}

// putVerify is steps 4 and 5 for one key. Transient store errors were
// already retried for RetryTimeout inside the Blob.
func (u *Uploader) putVerify(ctx context.Context, key string, p *pendingUpload, committed time.Time) error {
	// committed carries a monotonic reading, so a wall clock step cannot
	// fire or suppress the rule.
	if age := time.Since(committed); age > u.cfg.OrphanAge/2 {
		return fmt.Errorf("%w (%v)", errStale, age)
	}
	if err := u.cfg.Blob.PutKey(ctx, key, p.data); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	got, err := u.cfg.Blob.GetKey(ctx, key)
	if err != nil {
		return fmt.Errorf("read back: %w", err)
	}
	return objstore.Verify(got, int64(len(p.data)), p.sha)
}
