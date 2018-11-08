package boltstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ahmadmuzakkir/dag/model"
	"github.com/boltdb/bolt"
)

const (
	testBoltPath = "/tmp/bolt/graph_test.db"
)

func getBoltDataStore(size int, t *testing.T) (*BoltStore, func()) {
	// Create directory if it does not exist
	if _, err := os.Stat(testBoltPath); os.IsNotExist(err) {
		if err2 := os.MkdirAll(filepath.Dir(testBoltPath), os.ModePerm); err2 != nil {
			t.Fatalf("failed to create bolt directory: %s", err)
		}
	}

	db, err := bolt.Open(testBoltPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt: %s", err)
	}

	var ds = NewBoltStore(db)

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

	ds, teardown := getBoltDataStore(size, t)
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
