package store

import (
	"fmt"
)

type Vertex struct {
	ID       string
	Flag     bool
	Rank     int
	Parents  map[string]struct{}
	Children map[string]struct{}
}

func NewVertex(id string, flag bool, rank int) *Vertex {
	v := &Vertex{
		ID:       id,
		Parents:  make(map[string]struct{}),
		Children: make(map[string]struct{}),
		Flag:     flag,
		Rank:     rank,
	}

	return v
}
func (v *Vertex) String() string {
	result := fmt.Sprintf("ID: %s - Parents: %d - Children: %d - Flag: %v\n", v.ID, len(v.Parents), len(v.Children), v.Flag)

	return result
}
