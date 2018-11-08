package badgerstore

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ahmadmuzakkir/dag/model"
	"github.com/dgraph-io/badger"
)

const (
	testBadgerDir = "/tmp/badger_test"
)

func TestDAG(t *testing.T) {
	size := 100000
	edges := size - 1

	ds, teardown, err := getBadgerDataStore(size)
	defer teardown()
	if err != nil {
		t.Fatal(err)
	}

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

func BenchmarkReach(t *testing.B) {
	size := 100000

	graph, teardown, err := getGraph(size)
	defer teardown()
	if err != nil {
		t.Fatal(err)
	}

	v, err := getVertex(graph)
	if err != nil {
		t.Fatal(err)
	}

	graph.Reach(v.ID)
}

func BenchmarkConditionalReach(t *testing.B) {
	size := 100000

	graph, teardown, err := getGraph(size)
	defer teardown()
	if err != nil {
		t.Fatal(err)
	}

	v, err := getVertex(graph)
	if err != nil {
		t.Fatal(err)
	}

	graph.ConditionalReach(v.ID, false)
}

func BenchmarkList(t *testing.B) {
	size := 100000

	graph, teardown, err := getGraph(size)
	defer teardown()
	if err != nil {
		t.Fatal(err)
	}

	v, err := getVertex(graph)
	if err != nil {
		t.Fatal(err)
	}

	graph.List(v.ID)
}

func BenchmarkConditionalList(t *testing.B) {
	size := 100000

	graph, teardown, err := getGraph(size)
	defer teardown()
	if err != nil {
		t.Fatal(err)
	}

	v, err := getVertex(graph)
	if err != nil {
		t.Fatal(err)
	}

	graph.ConditionalList(v.ID, false)
}

func getBadgerDataStore(size int) (*BadgerStore, func(), error) {
	rand.Seed(time.Now().UnixNano())

	opts := badger.DefaultOptions
	opts.Dir = testBadgerDir
	opts.ValueDir = testBadgerDir
	db, err := badger.Open(opts)
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to open badger: %s", err)
	}
	teardown := func() {
		db.Close()
	}

	var ds = NewBadgerStore(db)

	graph := model.GenerateGraph(size)

	err = ds.Insert(graph)
	if err != nil {
		return nil, teardown, fmt.Errorf("failed to insert graph: %s", err)
	}

	return ds, teardown, nil
}

func getGraph(size int) (*model.DAG, func(), error) {
	ds, teardown, err := getBadgerDataStore(size)
	if err != nil {
		return nil, teardown, err
	}

	g, err := ds.Get()
	if err != nil {
		return nil, teardown, err
	}

	return g, teardown, nil
}

func getVertex(graph *model.DAG) (*model.Vertex, error) {
	var index int = 0
	// index := rand.Intn(graph.CountVertex())

	v, err := graph.GetVertexByIndex(index)
	if err != nil {
		return v, err
	}

	return v, nil
}
