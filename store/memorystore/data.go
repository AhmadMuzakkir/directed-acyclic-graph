package memorystore

import (
	"github.com/ahmadmuzakkir/dag/model"
	"github.com/ahmadmuzakkir/dag/store"
)

var _ store.DataStore = (*DataMock)(nil)

type DataMock struct {
	dag *model.DAG
}

func (d *DataMock) Get() (*model.DAG, error) {
	return d.dag, nil
}

func (d *DataMock) Insert(g *model.DAG) error {
	d.dag = g
	return nil
}
