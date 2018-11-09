package store

import "github.com/ahmadmuzakkir/dag/model"

type GraphStore interface {
	Get() (*model.DAG, error)

	// Get the vertex found at the index in the database.
	GetVertexByPosition(position int) (*model.Vertex, error)

	// Insert will clear existing graph first, before inserting the new graph
	Insert(g *model.DAG) error

	Reach(id string) (int, error)

	ConditionalReach(id string, flag bool) (int, error)

	List(id string) ([]*model.Vertex, error)

	ConditionalList(id string, flag bool) ([]*model.Vertex, error)
}
