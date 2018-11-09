package main

import (
	"log"

	"github.com/ahmadmuzakkir/dag/cmd"
)

func main() {
	ds, teardown, err := cmd.GetDataStore()
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
		v, _ := graph.GetVertexByPosition(i)
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
