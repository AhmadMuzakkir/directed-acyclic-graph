package main

import (
	"log"

	"github.com/ahmadmuzakkir/dag/cmd"
	"github.com/ahmadmuzakkir/dag/store"
)

func main() {
	var ds store.DataStore
	var teardown func()
	var err error

	if cmd.DBType == 1 {
		ds, teardown, err = cmd.GetBoltDataStore(cmd.BoltPath)
	} else {
		ds, teardown, err = cmd.GetBadgerDataStore(cmd.BadgerDirPath)
	}
	if err != nil {
		log.Fatal(err)
	}

	defer teardown()

	graph, err := ds.Get()
	if err != nil {
		log.Fatal(err)
	}

	highestAncestorsCount := 0
	for i := 0; i < 10; i++ {
		v, _ := graph.GetVertexByIndex(i)
		if err != nil {
			log.Fatal(err)
		}

		a := graph.AncestorsBFS(v.ID, nil)
		if len(a) > highestAncestorsCount {
			highestAncestorsCount = len(a)
		}
	}
	log.Printf("Highest ancestors count: %v", highestAncestorsCount)
}
