package main

// Used to generate the graph and insert them into the DB

import (
	"log"
	"math/rand"
	"time"

	"github.com/ahmadmuzakkir/dag/cmd"
	"github.com/ahmadmuzakkir/dag/store"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	ds, teardown, err := cmd.GetDataStore()
	if err != nil {
		log.Fatal(err)
	}

	defer teardown()

	graph := store.GenerateGraph(100000)

	err = ds.Insert(graph)
	if err != nil {
		log.Fatal("insert error: ", err)
	}
}
