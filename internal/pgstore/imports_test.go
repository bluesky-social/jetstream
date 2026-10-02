package pgstore_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Core code reaches PostgreSQL and S3 only through the storage interfaces
// (plan ground rule 2), so the drivers stay confined to the backend packages.
// A package that imports a driver directly would bypass the fakes the oracle
// runs against.
var storageDrivers = []struct {
	prefix  string
	allowed []string // repo-relative dirs; a trailing "/..." includes subdirs
}{
	{
		prefix: "github.com/jackc/pgx",
		allowed: []string{
			"internal/pgstore/...",
			"internal/metastore/pg",
			"internal/catalog/pg",
			"internal/leader",
		},
	},
	{
		prefix:  "github.com/aws/",
		allowed: []string{"internal/objstore/s3"},
	},
}

func dirAllowed(rel string, allowed []string) bool {
	for _, a := range allowed {
		if base, ok := strings.CutSuffix(a, "/..."); ok {
			if rel == base || strings.HasPrefix(rel, base+"/") {
				return true
			}
			continue
		}
		if rel == a {
			return true
		}
	}
	return false
}

func TestStorageDriverImportBoundary(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs("../..")
	require.NoError(t, err)

	fset := token.NewFileSet()
	var offenders []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		for _, imp := range f.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			for _, drv := range storageDrivers {
				if strings.HasPrefix(p, drv.prefix) && !dirAllowed(rel, drv.allowed) {
					offenders = append(offenders, path+": "+p)
				}
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, offenders, "only the storage backend packages may import pgx or the AWS SDK")
}

func TestDirAllowed(t *testing.T) {
	t.Parallel()
	allowed := []string{"internal/pgstore/...", "internal/leader"}
	require.True(t, dirAllowed("internal/pgstore", allowed))
	require.True(t, dirAllowed("internal/pgstore/pgtest", allowed))
	require.False(t, dirAllowed("internal/pgstorex", allowed))
	require.True(t, dirAllowed("internal/leader", allowed))
	require.False(t, dirAllowed("internal/leader/lockertest", allowed))
	require.False(t, dirAllowed("internal/ingest", allowed))
}
