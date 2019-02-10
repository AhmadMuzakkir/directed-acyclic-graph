package model

import (
	"fmt"
	"io"
)

type Vertex struct {
	ID             string
	Index          int
	Flag           bool
	Rank           int
	Parents        map[string]struct{}
	Children       map[string]struct{}
}

func NewVertex(id string, flag bool, rank int) *Vertex {
	v := &Vertex{
		ID:             id,
		Parents:        make(map[string]struct{}),
		Children:       make(map[string]struct{}),
		Flag:           flag,
		Rank:           rank,
	}

	return v
}
func (v *Vertex) String() string {
	result := fmt.Sprintf("ID: %s - Parents: %d - Children: %d - Flag: %v\n", v.ID, len(v.Parents), len(v.Children), v.Flag)

	return result
}

func (v *Vertex) DOT(w io.Writer, graph *DAG) error {
	for p, _ := range v.Parents {
		pv, _ := graph.GetVertex(p)

		_, err := fmt.Fprintf(w, "\t%d -> %d;\n", v.Index, pv.Index)
		if err != nil {
			return err
		}
	}

	return nil
}
