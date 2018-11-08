package store

import (
	"encoding/json"
	"log"
	"time"

	"github.com/ahmadmuzakkir/dag/model"
)

func StartTimer(name string) func() {
	t := time.Now()
	log.Println(name, "started")
	return func() {
		d := time.Now().Sub(t)
		log.Println(name, "took", d)
	}
}

func LogDAGAsJSON(graph *model.DAG) {
	type vertex struct {
		ID      string   `json:"id"`
		Parents []string `json:"parents"`
		Flag    bool     `json:"flag"`
		Rank    int      `json:"rank"`
	}
	var list []*vertex
	for _, v := range graph.Vertices() {
		var parents []string
		for p, _ := range v.Parents {
			parents = append(parents, p)
		}
		list = append(list, &vertex{
			ID:      v.ID,
			Parents: parents,
			Flag:    v.Flag,
			Rank:    v.Rank,
		})
	}

	b, err := json.Marshal(list)
	if err != nil {
		log.Println("error:", err)
	}
	log.Println(string(b))
}
