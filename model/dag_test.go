package model

import (
	"math/rand"
	"testing"
)

const (
	testSize = 100000
)

func TestGenerateDAG(t *testing.T) {
	size := testSize
	edges := size - 1

	graph := GenerateGraph(size)

	foundSize := graph.CountVertex()
	if foundSize != size {
		t.Fatalf("expected vertices count %d, found %d", size, foundSize)
	}

	foundEdges := graph.CountEdge()
	if foundEdges != edges {
		t.Fatalf("expected edges count %d, found %d", edges, foundEdges)
	}

	// Check:
	// - vertex with rank 0 must not have any parents
	// - vertex with rank other than 0 must have at least one parent
	for _, v := range graph.vertices {
		if v.Rank == 0 {
			if len(v.Parents) != 0 {
				t.Fatal("vertex with rank 0 must not have any parents")
			}
			continue
		}
		if len(v.Parents) == 0 {
			t.Fatal("vertex with rank other than 0 must have at least one parent")
		}
	}
}

func BenchmarkGenerateDAG(t *testing.B) {
	size := testSize

	for n := 0; n < t.N; n++ {
		_ = GenerateGraph(size)
	}
}

func BenchmarkReach(t *testing.B) {
	size := testSize
	graph := GenerateGraph(size)

	v := getVertex(graph, t)

	for n := 0; n < t.N; n++ {
		graph.Reach(v.ID)
	}
}

func BenchmarkConditionalReach(t *testing.B) {
	size := testSize
	graph := GenerateGraph(size)

	v := getVertex(graph, t)

	for n := 0; n < t.N; n++ {
		graph.ConditionalReach(v.ID, false)
	}
}

func BenchmarkList(t *testing.B) {
	size := testSize
	graph := GenerateGraph(size)

	v := getVertex(graph, t)

	for n := 0; n < t.N; n++ {
		graph.List(v.ID)
	}
}

func BenchmarkConditionalList(t *testing.B) {
	size := testSize
	graph := GenerateGraph(size)

	v := getVertex(graph, t)

	for n := 0; n < t.N; n++ {
		graph.ConditionalList(v.ID, false)
	}
}

func getVertex(graph *DAG, t *testing.B) *Vertex {
	// var index int = 0
	index := rand.Intn(graph.CountVertex())

	v, err := graph.GetVertexByPosition(index)
	if err != nil {
		t.Fatal(err)
	}

	return v
}
