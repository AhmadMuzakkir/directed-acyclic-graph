package main

import (
	"testing"

	"github.com/ahmadmuzakkir/dag/cmd"
	"github.com/ahmadmuzakkir/dag/model"
)

func getGraph(t *testing.B) (*model.DAG, func()) {
	ds, teardown, err := cmd.GetDataStore()
	if err != nil {
		t.Fatal(err)
	}

	// Uncomment below, if you want regenerate the data for every run.
	// Otherwise, it will use the existing data.
	// Warning, this will be a lot slower.

	// err = ds.Insert(store.GenerateGraph(100000))
	// if err != nil {
	// 	t.Fatal(err)
	// }

	g, err := ds.Get()
	if err != nil {
		t.Fatal(err)
	}

	return g, teardown
}

func getVertex(graph *model.DAG, t *testing.B) *model.Vertex {
	var index int = 0
	// index := rand.Intn(graph.CountVertex())

	v, err := graph.GetVertexByIndex(index)
	if err != nil {
		t.Fatal(err)
	}

	return v
}

func BenchmarkGet(t *testing.B) {
	ds, teardown, err := cmd.GetDataStore()
	if err != nil {
		t.Fatal(err)
	}
	defer teardown()

	_, err = ds.Get()
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkReach(t *testing.B) {
	graph, teardown := getGraph(t)
	defer teardown()

	v := getVertex(graph, t)
	graph.Reach(v.ID)
}

func BenchmarkConditionalReach(t *testing.B) {
	graph, teardown := getGraph(t)
	defer teardown()

	v := getVertex(graph, t)

	graph.ConditionalReach(v.ID, false)
}

func BenchmarkList(t *testing.B) {
	graph, teardown := getGraph(t)
	defer teardown()

	v := getVertex(graph, t)

	graph.List(v.ID)
}

func BenchmarkConditionalList(t *testing.B) {
	graph, teardown := getGraph(t)
	defer teardown()

	v := getVertex(graph, t)

	graph.ConditionalList(v.ID, false)
}
