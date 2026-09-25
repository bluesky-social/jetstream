// Package local is the catalog over a local data directory: each namespace
// is a directory of segment files, a sealed segment's metadata is its
// header and footer, and an active segment's durable blocks come from the
// ingest writer that owns it (catalog.ActiveSource).
//
// The ingest writers tell the catalog about seals as they happen
// (ingest.SegmentCatalog). Refresh rescans the directories for changes made
// behind the catalog's back: compaction rewrites, merge cleanup, and a data
// directory opened with no writer attached.
package local

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"sync"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
)

// Config configures a Catalog.
type Config struct {
	// FS is the filesystem holding the segment directories. Nil uses the
	// host OS filesystem.
	FS vfs.FS

	// Dirs maps each namespace to its segment directory. A namespace
	// without a directory holds no segments. A directory that does not
	// exist yet is empty.
	Dirs map[catalog.Namespace]string
}

// SealedHook hears about each sealed segment the catalog learns of from a
// writer, with the segment's file path. It runs under the writer mutex; see
// ingest.SegmentCatalog.Sealed.
type SealedHook func(v catalog.SegmentView, path string) error

// Catalog is the local catalog. It is safe for concurrent use.
type Catalog struct {
	fs   vfs.FS
	dirs map[catalog.Namespace]string

	mu  sync.Mutex
	rev uint64
	// sealed holds each namespace's sealed segments.
	sealed map[catalog.Namespace]catalog.SegmentList
	// active is each namespace's attached writer.
	active map[catalog.Namespace]catalog.ActiveSource
	// scannedActive is each namespace's unsealed tail as Refresh read it
	// from disk. Snapshot uses it only when no writer is attached, so a
	// catalog opened over a directory nobody is writing still sees the
	// tail's durable blocks.
	scannedActive map[catalog.Namespace]catalog.SegmentView
	hooks         map[catalog.Namespace][]SealedHook
}

var (
	_ catalog.Catalog       = (*Catalog)(nil)
	_ ingest.SegmentCatalog = (*Catalog)(nil)
)

// New returns an empty catalog. Call Refresh to load what is on disk.
func New(cfg Config) (*Catalog, error) {
	for ns := range cfg.Dirs {
		if !ns.Valid() {
			return nil, fmt.Errorf("catalog/local: unknown namespace %q", ns)
		}
	}
	fs := cfg.FS
	if fs == nil {
		fs = vfs.Default
	}
	return &Catalog{
		fs:            fs,
		dirs:          maps(cfg.Dirs),
		sealed:        map[catalog.Namespace]catalog.SegmentList{},
		active:        map[catalog.Namespace]catalog.ActiveSource{},
		scannedActive: map[catalog.Namespace]catalog.SegmentView{},
		hooks:         map[catalog.Namespace][]SealedHook{},
	}, nil
}

func maps(in map[catalog.Namespace]string) map[catalog.Namespace]string {
	out := make(map[catalog.Namespace]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// Path returns the file path of segment idx in ns, or "" if ns has no
// directory.
func (c *Catalog) Path(ns catalog.Namespace, idx uint64) string {
	dir, ok := c.dirs[ns]
	if !ok {
		return ""
	}
	return filepath.Join(dir, ingest.SegmentFilename(idx))
}

// OnSealed registers fn to hear about segments sealed in ns. Register hooks
// before the writers open.
func (c *Catalog) OnSealed(ns catalog.Namespace, fn SealedHook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hooks[ns] = append(c.hooks[ns], fn)
}

// AttachActive implements ingest.SegmentCatalog.
func (c *Catalog) AttachActive(ns catalog.Namespace, src catalog.ActiveSource) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active[ns] = src
}

// Sealed implements ingest.SegmentCatalog: it records v and runs the hooks
// for its namespace. The view is recorded first, so a hook's failure does
// not hide a segment that is durably sealed on disk.
func (c *Catalog) Sealed(v catalog.SegmentView) error {
	if _, ok := c.dirs[v.Namespace]; !ok {
		return fmt.Errorf("catalog/local: sealed segment %d in namespace %q with no directory", v.Index, v.Namespace)
	}
	c.mu.Lock()
	err := c.putSealedLocked(v)
	hooks := slices.Clone(c.hooks[v.Namespace])
	c.mu.Unlock()
	if err != nil {
		return err
	}

	path := c.Path(v.Namespace, v.Index)
	for _, fn := range hooks {
		if err := fn(v, path); err != nil {
			return err
		}
	}
	return nil
}

// putSealedLocked inserts or replaces v in its namespace's sealed list and
// bumps the revision. Putting a generation the list already holds is a
// no-op, so a Reload racing the writer's own publish does not churn the
// revision.
func (c *Catalog) putSealedLocked(v catalog.SegmentView) error {
	list := c.sealed[v.Namespace]
	if cur, ok := list.Get(v.Index); ok && cur.State == catalog.Sealed && cur.Generation == v.Generation {
		return nil
	}
	next, err := list.Put(v)
	if err != nil {
		return fmt.Errorf("catalog/local: publish segment %d: %w", v.Index, err)
	}
	c.sealed[v.Namespace] = next
	if sa, ok := c.scannedActive[v.Namespace]; ok && sa.Index <= v.Index {
		delete(c.scannedActive, v.Namespace)
	}
	c.rev++
	return nil
}

// Reload re-reads sealed segment idx of ns from disk, for a caller that
// rewrote it (compaction) and wants the next Snapshot to see the new
// generation without a full Refresh. A missing file drops the segment.
func (c *Catalog) Reload(ns catalog.Namespace, idx uint64) error {
	path := c.Path(ns, idx)
	if path == "" {
		return fmt.Errorf("catalog/local: reload segment %d in namespace %q with no directory", idx, ns)
	}
	v, err := loadSealed(c.fs, ns, idx, path)
	if oserror.IsNotExist(err) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.dropSealedLocked(ns, idx)
		return nil
	}
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.putSealedLocked(v)
}

func (c *Catalog) dropSealedLocked(ns catalog.Namespace, idx uint64) {
	list := c.sealed[ns]
	if _, ok := list.Get(idx); !ok {
		return
	}
	c.sealed[ns] = list.Delete(idx)
	c.rev++
}

// Refresh rescans every namespace directory. A sealed segment whose header
// checksum is unchanged keeps its loaded view; a changed or new one is
// re-read and its checksum verified. The highest unsealed file is scanned
// for its durable blocks, for use when no writer is attached. Lower
// unsealed files are ignored, as the manifest ignores them: a writer only
// ever appends to the highest index.
func (c *Catalog) Refresh(ctx context.Context) error {
	for _, ns := range catalog.Namespaces {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.refreshNamespace(ns); err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) refreshNamespace(ns catalog.Namespace) error {
	dir, ok := c.dirs[ns]
	if !ok {
		return nil
	}
	// What the catalog knows is sampled before the directory is listed, so
	// every change that lands during the scan is newer than this sample.
	c.mu.Lock()
	known := make(map[uint64]catalog.SegmentView, c.sealed[ns].Len())
	for _, v := range c.sealed[ns].Segments() {
		known[v.Index] = v
	}
	c.mu.Unlock()

	files, err := ingest.SegmentFilesFS(c.fs, dir)
	if err != nil {
		if _, statErr := c.fs.Stat(dir); oserror.IsNotExist(statErr) {
			files = nil
		} else {
			return err
		}
	}

	var (
		sealed  []catalog.SegmentView
		tail    catalog.SegmentView
		hasTail bool
	)
	for i, f := range files {
		gen, err := readGeneration(c.fs, f.Path)
		if err != nil {
			return fmt.Errorf("catalog/local: %s: %w", f.Path, err)
		}
		if gen == 0 {
			if i != len(files)-1 {
				continue
			}
			blocks, err := segment.ActiveBlocksFS(c.fs, f.Path)
			if err != nil {
				return fmt.Errorf("catalog/local: scan active %s: %w", f.Path, err)
			}
			tail, hasTail = catalog.SegmentView{Namespace: ns, Index: f.Idx, State: catalog.Active, Blocks: blocks}, true
			continue
		}
		if v, ok := known[f.Idx]; ok && v.Generation == gen {
			sealed = append(sealed, v)
			continue
		}
		v, err := loadSealed(c.fs, ns, f.Idx, f.Path)
		if err != nil {
			return err
		}
		sealed = append(sealed, v)
	}

	list, err := catalog.NewSegmentList(sealed)
	if err != nil {
		return fmt.Errorf("catalog/local: %s: %w", dir, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// A writer may have sealed, or a Reload re-read, a segment while the
	// directory was being scanned, and the scan may have read that file
	// before the change. Those views are at least as new as the scan's, so
	// they survive; only what the catalog already knew when the scan began
	// is the scan's to replace or drop.
	for _, v := range c.sealed[ns].Segments() {
		if k, ok := known[v.Index]; ok && sameGeneration(k, v) {
			continue
		}
		if list, err = list.Put(v); err != nil {
			return fmt.Errorf("catalog/local: %s: %w", dir, err)
		}
		sealed = list.Segments()
	}
	if !slices.EqualFunc(sealed, c.sealed[ns].Segments(), sameGeneration) {
		c.sealed[ns] = list
		c.rev++
	}
	if hasTail {
		c.scannedActive[ns] = tail
	} else {
		delete(c.scannedActive, ns)
	}
	return nil
}

func sameGeneration(a, b catalog.SegmentView) bool {
	return a.Index == b.Index && a.Generation == b.Generation
}

// DropNamespace forgets every segment of ns and detaches its writer, for a
// caller that removed the namespace's directory (merge cleanup).
func (c *Catalog) DropNamespace(ns catalog.Namespace) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sealed[ns].Len() > 0 {
		c.rev++
	}
	delete(c.sealed, ns)
	delete(c.active, ns)
	delete(c.scannedActive, ns)
}

// Snapshot implements catalog.Catalog. Each namespace's writer is sampled
// before the sealed list: a writer publishes a seal before it moves to the
// next index, so a sample that still names the old index finds that index
// sealed in the list read after it, and the sealed view wins.
func (c *Catalog) Snapshot() catalog.CatalogView {
	actives := make(map[catalog.Namespace]catalog.SegmentView)
	c.mu.Lock()
	srcs := make(map[catalog.Namespace]catalog.ActiveSource, len(c.active))
	for ns, src := range c.active {
		srcs[ns] = src
	}
	c.mu.Unlock()
	for ns, src := range srcs {
		if v, ok := src.ActiveSegment(); ok {
			actives[ns] = v
		}
	}

	c.mu.Lock()
	rev := c.rev
	lists := make(map[catalog.Namespace]catalog.SegmentList, len(c.dirs))
	tails := make(map[catalog.Namespace]catalog.SegmentView, len(c.dirs))
	for ns := range c.dirs {
		list := c.sealed[ns]
		lists[ns] = list
		av, ok := actives[ns]
		if _, attached := srcs[ns]; !attached {
			av, ok = c.scannedActive[ns]
		}
		if last, sealed := list.Last(); ok && (!sealed || av.Index > last.Index) {
			tails[ns] = av
		}
	}
	c.mu.Unlock()

	view, err := catalog.NewView(rev, lists, tails, func(v catalog.SegmentView, i int) catalog.Locator {
		b := v.Blocks[i]
		return catalog.FileBlock{
			Path:   c.Path(v.Namespace, v.Index),
			Offset: b.Offset,
			Length: b.CompressedSize,
		}
	})
	if err != nil {
		// Every view comes from seq-monotonic writers or from files they
		// wrote, so an overlap means the archive itself is corrupt.
		panic(fmt.Sprintf("catalog/local: %v", err))
	}
	return view
}

// loadSealed reads and verifies a sealed segment's metadata.
func loadSealed(fs vfs.FS, ns catalog.Namespace, idx uint64, path string) (catalog.SegmentView, error) {
	r, err := segment.Open(segment.ReaderConfig{Path: path, FS: fs})
	if err != nil {
		return catalog.SegmentView{}, fmt.Errorf("catalog/local: open sealed %s: %w", path, err)
	}
	defer func() { _ = r.Close() }()
	hdr := r.Header()
	info, err := fs.Stat(path)
	if err != nil {
		return catalog.SegmentView{}, fmt.Errorf("catalog/local: stat %s: %w", path, err)
	}
	return catalog.SegmentView{
		Namespace:  ns,
		Index:      idx,
		State:      catalog.Sealed,
		Generation: hdr.Checksum,
		Header:     hdr,
		Blocks:     r.Blocks(),
		Size:       info.Size(),
	}, nil
}

// readGeneration returns the checksum at header offset 4: zero while a
// segment is active, the sealed checksum after.
func readGeneration(fs vfs.FS, path string) (uint64, error) {
	f, err := fs.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	var buf [12]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		return 0, fmt.Errorf("read header: %w", err)
	}
	return binary.LittleEndian.Uint64(buf[4:12]), nil
}

// Fetcher returns the Fetcher for FileBlock refs.
func (c *Catalog) Fetcher() catalog.Fetcher { return fetcher{c: c} }

type fetcher struct {
	c *Catalog
}

// Fetch reads a FileBlock's frame after checking that the file still holds
// the ref's generation. A compaction rewrite or a seal changes the header
// checksum, and a merge cleanup removes the file; both mean the ref's
// offsets no longer describe the file, so Fetch reports ErrStaleRef.
//
// A stale sealed ref also reloads its segment. The compactor tells the
// catalog about a rewrite only after the rename, so without this a reader
// in that window would retry against the same stale view. A stale active
// ref needs no reload: the writer publishes the seal before it releases the
// lock a fresh Snapshot waits on.
func (f fetcher) Fetch(ctx context.Context, ref catalog.BlockRef) ([]byte, error) {
	frame, err := f.fetch(ref)
	if errors.Is(err, catalog.ErrStaleRef) && ref.Generation != 0 {
		if rerr := f.c.Reload(ref.Namespace, ref.Segment); rerr != nil {
			return nil, errors.Join(err, rerr)
		}
	}
	return frame, err
}

func (f fetcher) fetch(ref catalog.BlockRef) ([]byte, error) {
	loc, ok := ref.Loc.(catalog.FileBlock)
	if !ok {
		return nil, fmt.Errorf("catalog/local: unsupported locator %T", ref.Loc)
	}
	file, err := f.c.fs.Open(loc.Path)
	if oserror.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %s is gone", catalog.ErrStaleRef, loc.Path)
	}
	if err != nil {
		return nil, fmt.Errorf("catalog/local: open %s: %w", loc.Path, err)
	}
	defer func() { _ = file.Close() }()

	var head [12]byte
	if _, err := file.ReadAt(head[:], 0); err != nil {
		return nil, fmt.Errorf("catalog/local: read header %s: %w", loc.Path, err)
	}
	if gen := binary.LittleEndian.Uint64(head[4:12]); gen != ref.Generation {
		return nil, fmt.Errorf("%w: %s generation %x, ref %x", catalog.ErrStaleRef, loc.Path, gen, ref.Generation)
	}
	buf := make([]byte, 8+int(loc.Length))
	if _, err := file.ReadAt(buf, int64(loc.Offset)); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: %s block %d at %d past EOF", segment.ErrCorruptSegment, loc.Path, ref.Block, loc.Offset)
		}
		return nil, fmt.Errorf("catalog/local: read %s block %d: %w", loc.Path, ref.Block, err)
	}
	if n := binary.LittleEndian.Uint64(buf[:8]); n != uint64(loc.Length) {
		return nil, fmt.Errorf("%w: %s block %d prefix %d, index %d", segment.ErrCorruptSegment, loc.Path, ref.Block, n, loc.Length)
	}
	return buf[8:], nil
}
