package badgerstore

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/ahmadmuzakkir/dag/model"
	"github.com/ahmadmuzakkir/dag/store"
	"github.com/dgraph-io/badger"
)

const (
	testBadgerDir = "/tmp/badger_test"
	testGraphSize = 100000
)

// Run the tests for both BFS and DFS
var tesalgos = []testalgo{
	testalgo{name: "BFS", algo: store.ALGO_BFS},
	testalgo{name: "DFS", algo: store.ALGO_DFS},
}

// Initiate the database and insert a new graph.
func init() {
	ds, teardown, err := getBadgerDataStore()
	defer teardown()
	if err != nil {
		log.Fatal(err)
	}

	graph := model.GenerateGraph(testGraphSize)

	err = ds.Insert(graph)
	if err != nil {
		log.Fatal(err)
	}
}

func TestDAG(t *testing.T) {
	edges := testGraphSize - 1

	ds, teardown, err := getBadgerDataStore()
	defer teardown()
	if err != nil {
		t.Fatal(err)
	}

	graph, err := ds.Get()
	if err != nil {
		t.Fatalf("failed to get graph: %s", err)
	}

	foundSize := graph.CountVertex()
	if foundSize != testGraphSize {
		t.Fatalf("expected vertices count %d, found %d", testGraphSize, foundSize)
	}

	foundEdges := graph.CountEdge()
	if foundEdges != edges {
		t.Fatalf("expected edges count %d, found %d", edges, foundEdges)
	}
}

func BenchmarkReach(t *testing.B) {
	ds, teardown, err := getBadgerDataStore()
	defer teardown()
	if err != nil {
		t.Fatal(err)
	}

	// Choose a random vertex
	v, err := ds.GetVertexByPosition(rand.Intn(testGraphSize))
	if err != nil {
		t.Fatal(err)
	}

	t.ResetTimer()

	for _, algo := range tesalgos {
		t.Run(algo.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if _, err := ds.Reach(algo.algo, v.ID); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkConditionalReach(t *testing.B) {
	ds, teardown, err := getBadgerDataStore()
	defer teardown()
	if err != nil {
		t.Fatal(err)
	}

	// Choose a random vertex
	v, err := ds.GetVertexByPosition(rand.Intn(testGraphSize))
	if err != nil {
		t.Fatal(err)
	}

	t.ResetTimer()

	for _, algo := range tesalgos {
		t.Run(algo.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if _, err := ds.ConditionalReach(algo.algo, v.ID, true); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkList(t *testing.B) {
	ds, teardown, err := getBadgerDataStore()
	defer teardown()
	if err != nil {
		t.Fatal(err)
	}

	// Choose a random vertex
	v, err := ds.GetVertexByPosition(rand.Intn(testGraphSize))
	if err != nil {
		t.Fatal(err)
	}

	t.ResetTimer()

	for _, algo := range tesalgos {
		t.Run(algo.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if _, err := ds.List(algo.algo, v.ID); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkConditionalList(t *testing.B) {
	ds, teardown, err := getBadgerDataStore()
	defer teardown()
	if err != nil {
		t.Fatal(err)
	}

	// Choose a random vertex
	v, err := ds.GetVertexByPosition(rand.Intn(testGraphSize))
	if err != nil {
		t.Fatal(err)
	}

	t.ResetTimer()

	for _, algo := range tesalgos {
		t.Run(algo.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if _, err := ds.ConditionalList(algo.algo, v.ID, true); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func getBadgerDataStore() (*BadgerStore, func(), error) {
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

	return ds, teardown, nil
}

type testalgo struct {
	name string
	algo store.Algo
}
