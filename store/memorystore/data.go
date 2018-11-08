package memorystore

import (
	"github.com/ahmadmuzakkir/dag/store"
)

var _ store.DataStore = (*DataMock)(nil)

type DataMock struct {
	dag *store.DAG
}

func (d *DataMock) Get() (*store.DAG, error) {
	return d.dag, nil
}

func (d *DataMock) Insert(g *store.DAG) error {
	d.dag = g
	return nil
}
