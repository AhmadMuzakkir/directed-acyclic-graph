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

	graph := store.GenerateGraph(100000)

	err = ds.Insert(graph)
	if err != nil {
		log.Fatal("insert error: ", err)
	}
}
