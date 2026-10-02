package local

import (
	"fmt"
	"strings"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/errors/oserror"
)

// This file holds the filesystem work the orchestrator does on the local
// archive: compaction rewrites, stale temp cleanup, and removing a
// namespace after merge. A disaggregated backend does the same jobs
// against object storage and the database.

// RewriteSegment rewrites sealed segment idx of ns in place through
// segment.Rewrite, then reloads it so the next Snapshot sees the new
// generation. opts.FS is ignored; the catalog's filesystem is used. The
// returned view is the reloaded segment when the result says it was
// rewritten, and zero otherwise.
func (c *Catalog) RewriteSegment(ns catalog.Namespace, idx uint64, decide func(*segment.Event) segment.RowDecision, opts segment.RewriteOptions) (segment.RewriteResult, catalog.SegmentView, error) {
	path := c.Path(ns, idx)
	if path == "" {
		return segment.RewriteResult{}, catalog.SegmentView{}, fmt.Errorf("catalog/local: rewrite segment %d in namespace %q with no directory", idx, ns)
	}
	opts.FS = c.fs
	res, err := segment.Rewrite(path, decide, opts)
	if err != nil || !res.Rewritten {
		return res, catalog.SegmentView{}, err
	}
	v, ok, err := c.reload(ns, idx)
	if err != nil {
		return res, catalog.SegmentView{}, fmt.Errorf("catalog/local: reload rewritten segment %d: %w", idx, err)
	}
	if !ok {
		return res, catalog.SegmentView{}, fmt.Errorf("catalog/local: rewritten segment %s is gone", path)
	}
	return res, v, nil
}

// RemoveStaleTemps deletes the *.jss.tmp files a crashed rewrite leaves in
// ns's directory. Only call it while no rewrite is running. A missing
// directory is fine: no writer has created it yet.
func (c *Catalog) RemoveStaleTemps(ns catalog.Namespace) error {
	dir, ok := c.dirs[ns]
	if !ok {
		return nil
	}
	entries, err := c.fs.List(dir)
	if err != nil {
		if oserror.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("catalog/local: readdir tmp cleanup: %w", err)
	}

	for _, name := range entries {
		path := c.fs.PathJoin(dir, name)
		info, statErr := c.fs.Stat(path)
		if statErr != nil {
			if oserror.IsNotExist(statErr) {
				continue
			}
			return fmt.Errorf("catalog/local: stat tmp %s: %w", name, statErr)
		}
		if info.IsDir() || !strings.HasSuffix(name, ".jss.tmp") {
			continue
		}
		if err := c.fs.Remove(path); err != nil && !oserror.IsNotExist(err) {
			return fmt.Errorf("catalog/local: remove tmp %s: %w", name, err)
		}
	}
	return nil
}

// DeleteNamespace removes ns's root tree and forgets its segments. The
// removal is durable when it returns: the root's parent directory is
// fsynced, so a caller may then commit metadata that assumes the
// namespace is gone. Without the fsync a power loss could keep that commit
// and roll the removal back. Deleting a namespace that is already gone
// still fsyncs, because seeing the tree missing does not prove an earlier
// process's removal reached stable storage.
func (c *Catalog) DeleteNamespace(ns catalog.Namespace) error {
	root, ok := c.roots[ns]
	if !ok {
		return fmt.Errorf("catalog/local: delete namespace %q with no directory", ns)
	}
	if err := c.fs.RemoveAll(root); err != nil {
		return fmt.Errorf("catalog/local: remove %s: %w", root, err)
	}
	if err := c.syncDir(c.fs.PathDir(root)); err != nil {
		return err
	}
	c.DropNamespace(ns)
	return nil
}

func (c *Catalog) syncDir(path string) error {
	dir, err := c.fs.OpenDir(path)
	if err != nil {
		return fmt.Errorf("catalog/local: open dir %s for sync: %w", path, err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return fmt.Errorf("catalog/local: fsync dir %s: %w", path, err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("catalog/local: close dir %s after sync: %w", path, err)
	}
	return nil
}
