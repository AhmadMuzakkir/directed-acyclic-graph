package store

import "github.com/ahmadmuzakkir/dag/model"

type DataStore interface {
	Get() (*model.DAG, error)
	Insert(g *model.DAG) error
}
