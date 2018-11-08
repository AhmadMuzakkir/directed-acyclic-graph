package main

import (
	"testing"

	"github.com/ahmadmuzakkir/dag/cmd"
	"github.com/ahmadmuzakkir/dag/store"
)

func getGraph(t *testing.B) (*store.DAG, func()) {
	// ds, teardown, err := cmd.GetBadgerDataStore(cmd.BadgerDirPath)
	ds, teardown, err := cmd.GetBoltDataStore(cmd.BoltPath)
	if err != nil {
		t.Fatal(err)
	}

	// If you want regenerate the data for every run, uncomment below.
	// Otherwise, it will use existing data.

	graph := store.GenerateGraph(100000)
	err = ds.Insert(graph)
	if err != nil {
		t.Fatal(err)
	}

	g, err := ds.Get()
	if err != nil {
		t.Fatal(err)
	}

	return g, teardown
}

func getVertex(graph *store.DAG, t *testing.B) *store.Vertex {
	var index int = 0
	// index := rand.Intn(graph.CountVertex())

	v, err := graph.GetVertexByIndex(index)
	if err != nil {
		t.Fatal(err)
	}

	return v
}

func BenchmarkGet(t *testing.B) {
	ds, teardown, err := cmd.GetBoltDataStore(cmd.BoltPath)
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
