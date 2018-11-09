package boltstore

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/ahmadmuzakkir/dag/model"
	"github.com/ahmadmuzakkir/dag/store"
	"github.com/boltdb/bolt"
)

const (
	testBoltPath = "/tmp/bolt/graph_test.db"

	testGraphSize = 100000
)

// Run the tests for both BFS and DFS
var tesalgos = []testalgo{
	testalgo{name: "BFS", algo: store.ALGO_BFS},
	testalgo{name: "DFS", algo: store.ALGO_DFS},
}

// Initiate the database and insert a new graph.
func init() {
	ds, teardown, err := getBoltDataStore()
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

	ds, teardown, err := getBoltDataStore()
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
	ds, teardown, err := getBoltDataStore()
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
	ds, teardown, err := getBoltDataStore()
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
	ds, teardown, err := getBoltDataStore()
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
	ds, teardown, err := getBoltDataStore()
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

func getBoltDataStore() (*BoltStore, func(), error) {
	// Create directory if it does not exist
	if _, err := os.Stat(testBoltPath); os.IsNotExist(err) {
		if err2 := os.MkdirAll(filepath.Dir(testBoltPath), os.ModePerm); err2 != nil {
			return nil, func() {}, fmt.Errorf("failed to create bolt directory: %s", err)
		}
	}

	db, err := bolt.Open(testBoltPath, 0600, nil)
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to open bolt: %s", err)
	}
	teardown := func() {
		db.Close()
	}

	var ds = NewBoltStore(db)

	return ds, teardown, nil
}

type testalgo struct {
	name string
	algo store.Algo
}
