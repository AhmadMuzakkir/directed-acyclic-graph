package memorystore

import (
	"github.com/ahmadmuzakkir/dag/model"
	"github.com/ahmadmuzakkir/dag/store"
)

var _ store.GraphStore = (*DataMock)(nil)

type DataMock struct {
	dag *model.DAG
}

func (d *DataMock) Get() (*model.DAG, error) {
	return d.dag, nil
}

func (d *DataMock) GetVertexByPosition(position int) (*model.Vertex, error) {
	return nil, nil
}

func (d *DataMock) Insert(g *model.DAG) error {
	d.dag = g
	return nil
}

func (d *DataMock) Reach(id string) (int, error) {
	// NOT IMPLEMENTED
	return 0, nil
}
func (d *DataMock) ConditionalReach(id string, flag bool) (int, error) {
	// NOT IMPLEMENTED
	return 0, nil
}
func (d *DataMock) List(id string) ([]*model.Vertex, error) {
	// NOT IMPLEMENTED
	return nil, nil
}
func (d *DataMock) ConditionalList(id string, flag bool) ([]*model.Vertex, error) {
	// NOT IMPLEMENTED
	return nil, nil
}
