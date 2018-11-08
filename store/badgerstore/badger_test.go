package badgerstore

import (
	"math/rand"
	"testing"
	"time"

	"github.com/ahmadmuzakkir/dag/model"
	"github.com/dgraph-io/badger"
)

const (
	testBadgerDir = "/tmp/badger"
)

func getBadgerDataStore(size int, t *testing.T) (*BadgerStore, func()) {
	rand.Seed(time.Now().UnixNano())

	opts := badger.DefaultOptions
	opts.Dir = testBadgerDir
	opts.ValueDir = testBadgerDir
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open badger: %s", err)
	}

	var ds = NewBadgerStore(db)

	graph := model.GenerateGraph(size)

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
		t.Fatalf("expected vertices count %d, found %d", size, foundSize)
	}

	foundEdges := graph.CountEdge()
	if foundEdges != edges {
		t.Fatalf("expected edges count %d, found %d", edges, foundEdges)
	}
}
