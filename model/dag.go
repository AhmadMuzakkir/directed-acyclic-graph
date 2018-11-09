package model

import (
	"fmt"
)

// DAG type implements a Directed Acyclic Graph data structure.
type DAG struct {
	vertices map[string]*Vertex
}

func NewDAG() *DAG {
	d := &DAG{
		vertices: make(map[string]*Vertex),
	}

	return d
}

func (d *DAG) AddVertex(v *Vertex) {
	d.vertices[v.ID] = v
}

func (d *DAG) DeleteVertex(vertex *Vertex) error {
	if _, ok := d.vertices[vertex.ID]; !ok {
		return fmt.Errorf("vertex with ID %v does not exist", vertex.ID)
	}

	delete(d.vertices, vertex.ID)

	return nil
}

func (d *DAG) AddEdge(parent *Vertex, child *Vertex) error {
	if _, ok := d.vertices[parent.ID]; !ok {
		return fmt.Errorf("vertex %v does not exist", parent.ID)
	}

	if _, ok := d.vertices[child.ID]; !ok {
		return fmt.Errorf("vertex ID %v does not exist", child.ID)
	}

	if _, ok := parent.Children[child.ID]; ok {
		return fmt.Errorf("edge (%v,%v) already exists", parent.ID, child.ID)
	}

	// Add edge.
	parent.Children[child.ID] = struct{}{}
	child.Parents[parent.ID] = struct{}{}

	return nil
}

func (d *DAG) DeleteEdge(parent *Vertex, child *Vertex) {
	if _, ok := parent.Children[child.ID]; ok {
		delete(parent.Children, child.ID)
	}
}

func (d *DAG) GetVertex(id string) (*Vertex, error) {
	vertex, found := d.vertices[id]
	if !found {
		return vertex, fmt.Errorf("vertex %s does not exist", id)
	}

	return vertex, nil
}

func (d *DAG) Vertices() map[string]*Vertex {
	return d.vertices
}

func (d *DAG) CountVertex() int {
	return len(d.vertices)
}

func (d *DAG) CountEdge() int {
	numEdges := 0
	for _, vertex := range d.vertices {
		numEdges = numEdges + len(vertex.Parents)
	}

	return numEdges
}

func (d *DAG) String() string {
	result := fmt.Sprintf("DAG Vertices: %d - Edges: %d\n", d.CountVertex(), d.CountEdge())
	result += fmt.Sprintf("Vertices:\n")
	for _, vertex := range d.vertices {
		result += fmt.Sprintf("%s", vertex)
	}

	return result
}

// BFS
func (d *DAG) AncestorsBFS(id string, filter func(*Vertex) bool) []*Vertex {
	var list []*Vertex

	q := []string{id}
	visited := make(map[string]struct{})
	visited[id] = struct{}{}

	for len(q) != 0 {
		u := q[0]
		q = q[1:len(q):len(q)]

		v, _ := d.GetVertex(u)

		for p, _ := range v.Parents {
			if _, ok := visited[p]; !ok {
				q = append(q, p)
				visited[p] = struct{}{}

				pv, _ := d.GetVertex(p)
				if filter == nil || filter(pv) {
					list = append(list, pv)
				}
			}
		}
	}

	return list
}

// DFS
func (d *DAG) AncestorsDFS(id string, filter func(*Vertex) bool) []*Vertex {
	var list []*Vertex

	s := []string{id}
	visited := make(map[string]struct{})

	for len(s) != 0 {

		u := s[len(s)-1]
		s = s[: len(s)-1 : len(s)-1]

		if _, ok := visited[u]; !ok {
			visited[u] = struct{}{}

			v, _ := d.GetVertex(u)

			if filter == nil || filter(v) {
				list = append(list, v)
			}

			for p, _ := range v.Parents {
				if _, ok := visited[p]; !ok {
					s = append(s, p)
				}
			}
		}
	}

	return list
}

func (d *DAG) Reach(id string) int {
	return len(d.AncestorsBFS(id, nil))
}

func (d *DAG) ConditionalReach(id string, flag bool) int {
	return len(d.AncestorsBFS(id, func(v *Vertex) bool {
		return v.Flag == flag
	}))
}

func (d *DAG) List(id string) []*Vertex {
	return d.AncestorsBFS(id, nil)
}

func (d *DAG) ConditionalList(id string, flag bool) []*Vertex {
	return d.AncestorsBFS(id, func(v *Vertex) bool {
		return v.Flag == flag
	})
}

func (d *DAG) Insert(v *Vertex) {
	d.AddVertex(v)
}

func (d *DAG) GetVertexByPosition(pos int) (*Vertex, error) {
	i := 0
	for _, v := range d.vertices {
		if i == pos {
			return v, nil
		}
		i++
	}

	return nil, fmt.Errorf("vertex at position %v does not exist", pos)
}
