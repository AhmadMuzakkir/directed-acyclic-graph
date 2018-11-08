package store

import "testing"

func TestGenerateDAG(t *testing.T) {
	size := 100000
	edges := size - 1

	graph := GenerateGraph(size)

	foundSize := graph.CountVertex()
	if foundSize != size {
		t.Fatalf("expected vertices count %d, fount %d", size, foundSize)
	}

	foundEdges := graph.CountEdge()
	if foundEdges != edges {
		t.Fatalf("expected edges count %d, fount %d", edges, foundEdges)
	}
}
