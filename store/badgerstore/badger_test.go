package badgerstore

import (
	"testing"

	"github.com/ahmadmuzakkir/dag/store"
	"github.com/dgraph-io/badger"
)

const (
	testBadgerDir = "/tmp/badger_test"
)

func getBadgerDataStore(size int, t *testing.T) (*BadgerStore, func()) {
	opts := badger.DefaultOptions
	opts.Dir = testBadgerDir
	opts.ValueDir = testBadgerDir
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open badger: %s", err)
	}

	var ds = NewBadgerStore(db)

	graph := store.GenerateGraph(size)

	err = ds.Insert(graph)
	if err != nil {
		t.Fatalf("failed to insert graph: %s", err)
	}

	return ds, func() { db.Close() }
}

func TestDAG(t *testing.T) {
	size := 100000
	edges := size - 1

	ds, teardown := getBadgerDataStore(size, t)
	defer teardown()

	graph, err := ds.Get()
	if err != nil {
		t.Fatalf("failed to get graph: %s", err)
	}

	foundSize := graph.CountVertex()
	if foundSize != size {
		t.Fatalf("expected vertices count %d, fount %d", size, foundSize)
	}

	foundEdges := graph.CountEdge()
	if foundEdges != edges {
		t.Fatalf("expected edges count %d, fount %d", edges, foundEdges)
	}
}
