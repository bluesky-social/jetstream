package memstore_test

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/internal/metastore/storetest"
)

func TestContract(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(*testing.T) metastore.Store { return memstore.New() })
}
