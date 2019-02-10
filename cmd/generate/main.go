package main

// Used to generate the graph and insert them into the DB

import (
	"bufio"
	"github.com/ahmadmuzakkir/dag/cmd"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/ahmadmuzakkir/dag/model"
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

	graph := model.GenerateGraph(100)

	err = ds.Insert(graph)
	if err != nil {
		log.Fatal("insert error: ", err)
	}

	dotFilePath := "test.dot"
	if _, err := os.Stat(dotFilePath); os.IsNotExist(err) {
		if err2 := os.MkdirAll(filepath.Dir(dotFilePath), os.ModePerm); err2 != nil {
			log.Fatal("create file error: ", err2)
		}
	}

	f, err := os.Create(dotFilePath)
	if err != nil {
		log.Fatal("open file error: ", err)
	}

	w := bufio.NewWriter(f)

	err = graph.DOT(w)
	if err != nil {
		log.Fatal("failed to generate DOT: ", err)
	}

	err = w.Flush()
	if err != nil {
		log.Fatal("flush writer error: ", err)
	}
}
