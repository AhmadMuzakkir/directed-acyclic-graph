package store

type DataStore interface {
	Get() (*DAG, error)
	Insert(g *DAG) error
}
