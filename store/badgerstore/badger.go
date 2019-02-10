package badgerstore

import (
	"encoding/json"

	"github.com/ahmadmuzakkir/dag/model"
	"github.com/ahmadmuzakkir/dag/store"
	"github.com/dgraph-io/badger"
)

var _ store.GraphStore = (*BadgerStore)(nil)

type BadgerStore struct {
	db *badger.DB
}

func NewBadgerStore(db *badger.DB) *BadgerStore {
	return &BadgerStore{
		db: db,
	}
}

func (b *BadgerStore) Get() (*model.DAG, error) {
	raw, err := b.get()
	if err != nil {
		return nil, err
	}

	var m = make(map[string]*model.Vertex)
	graph := model.NewDAG()
	for _, v := range raw {
		vertex := model.NewVertex(v.ID, v.Flag, v.Rank)
		for _, parent := range v.Parents {
			vertex.Parents[parent] = struct{}{}
		}

		for _, children := range v.Children {
			vertex.Children[children] = struct{}{}
		}

		m[v.ID] = vertex
		graph.AddVertex(vertex)
	}

	// for _, v := range raw {
	// 	for _, c := range v.Parents {
	// 		graph.AddEdge(m[c], m[v.ID])
	// 	}
	// }

	return graph, nil
}

func (b *BadgerStore) Insert(g *model.DAG) error {
	// Convert each vertex into the internal representation of vertex.
	vertices := g.Vertices()

	var data []*badgerVertex

	for _, vertex := range vertices {
		v := &badgerVertex{
			ID:   vertex.ID,
			Flag: vertex.Flag,
			Rank: vertex.Rank,
			Index: vertex.Index,
		}

		for parentID := range vertex.Parents {
			v.Parents = append(v.Parents, parentID)
		}

		for childrenID := range vertex.Children {
			v.Children = append(v.Children, childrenID)
		}
		data = append(data, v)
	}

	// Clear the old data first.
	if err := b.clear(); err != nil {
		return err
	}

	return b.insert(data)
}
func (b *BadgerStore) AncestorsBFS(id string, filter func(*model.Vertex) bool) ([]*model.Vertex, error) {
	var list []*model.Vertex
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := b.getByID(txn, id)
		if err != nil {
			return err
		}

		q := []string{id}
		visited := make(map[string]struct{})
		visited[id] = struct{}{}

		for len(q) != 0 {
			u := q[0]
			q = q[1:len(q):len(q)]

			v, err := b.getByID(txn, u)
			if err != nil {
				return err
			}

			for p, _ := range v.Parents {
				if _, ok := visited[p]; !ok {
					q = append(q, p)
					visited[p] = struct{}{}

					pv, err := b.getByID(txn, p)
					if err != nil {
						return err
					}

					if filter == nil || filter(pv) {
						list = append(list, pv)
					}
				}
			}
		}

		return nil
	})

	return list, err
}

func (b *BadgerStore) AncestorsDFS(id string, filter func(*model.Vertex) bool) ([]*model.Vertex, error) {
	var list []*model.Vertex
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := b.getByID(txn, id)
		if err != nil {
			return err
		}

		s := []string{id}
		visited := make(map[string]struct{})

		for len(s) != 0 {
			u := s[len(s)-1]
			s = s[: len(s)-1 : len(s)-1]

			if _, ok := visited[u]; !ok {
				visited[u] = struct{}{}

				v, err := b.getByID(txn, u)
				if err != nil {
					return err
				}

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

		return nil
	})

	return list, err
}

func (b *BadgerStore) Reach(algo store.Algo, id string) (int, error) {
	var list []*model.Vertex
	var err error

	if algo == store.ALGO_DFS {
		list, err = b.AncestorsDFS(id, nil)
	} else {
		list, err = b.AncestorsBFS(id, nil)
	}
	if err != nil {
		return 0, err
	}

	return len(list), nil
}

func (b *BadgerStore) ConditionalReach(algo store.Algo, id string, flag bool) (int, error) {
	var list []*model.Vertex
	var err error

	if algo == store.ALGO_DFS {
		list, err = b.AncestorsDFS(id, func(v *model.Vertex) bool {
			return v.Flag == flag
		})
	} else {
		list, err = b.AncestorsBFS(id, func(v *model.Vertex) bool {
			return v.Flag == flag
		})
	}

	if err != nil {
		return 0, err
	}

	return len(list), nil
}

func (b *BadgerStore) List(algo store.Algo, id string) ([]*model.Vertex, error) {
	var list []*model.Vertex
	var err error

	if algo == store.ALGO_DFS {
		list, err = b.AncestorsDFS(id, nil)
	} else {
		list, err = b.AncestorsBFS(id, nil)
	}
	if err != nil {
		return nil, err
	}

	return list, nil
}

func (b *BadgerStore) ConditionalList(algo store.Algo, id string, flag bool) ([]*model.Vertex, error) {
	var list []*model.Vertex
	var err error

	if algo == store.ALGO_DFS {
		list, err = b.AncestorsDFS(id, func(v *model.Vertex) bool {
			return v.Flag == flag
		})
	} else {
		list, err = b.AncestorsBFS(id, func(v *model.Vertex) bool {
			return v.Flag == flag
		})
	}

	if err != nil {
		return nil, err
	}
	return list, nil
}

func (b *BadgerStore) GetVertexByPosition(index int) (*model.Vertex, error) {
	var vertex *model.Vertex

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 1000
		it := txn.NewIterator(opts)
		defer it.Close()

		i := 0
		for it.Rewind(); it.Valid(); it.Next() {
			if index == i {
				item := it.Item()
				data, err := item.Value()
				if err != nil {
					return err
				}
				if vertex, err = b.unmarshal(data); err != nil {
					return err
				}
			}
			i++
		}
		return nil
	})

	return vertex, err
}

func (b *BadgerStore) get() ([]*badgerVertex, error) {
	var list []*badgerVertex

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 1000
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			data, err := item.Value()
			if err != nil {
				return err
			}

			var vertex badgerVertex
			if err = json.Unmarshal(data, &vertex); err != nil {
				return err
			}
			list = append(list, &vertex)
		}
		return nil
	})

	return list, err
}

// Insert the vertices into the database
func (b *BadgerStore) insert(list []*badgerVertex) error {
	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	for i := range list {
		data, err := json.Marshal(list[i])
		if err != nil {
			return err
		}
		if err := txn.Set([]byte(list[i].ID), data); err != nil {
			if err != badger.ErrTxnTooBig {
				return err
			}
			if err := txn.Commit(nil); err != nil {
				return err
			}

			txn = b.db.NewTransaction(true)
			if err := txn.Set([]byte(list[i].ID), data); err != nil {
				return err
			}
		}
	}

	// Commit the transaction and check for error.
	if err := txn.Commit(nil); err != nil {
		return err
	}

	return nil
}

// Delete the existing data
func (b *BadgerStore) clear() error {
	var keys [][]byte
	b.db.Size()
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}

		return nil
	})
	if err != nil {
		return err
	}

	txn := b.db.NewTransaction(true)
	defer txn.Discard()

	for i := range keys {
		if err := txn.Delete(keys[i]); err != nil {
			if err != badger.ErrTxnTooBig {
				if err := txn.Commit(nil); err != nil {
					return err
				}
				return err
			}
			if err := txn.Commit(nil); err != nil {
				return err
			}

			txn = b.db.NewTransaction(true)
			if err := txn.Delete(keys[i]); err != nil {
				return err
			}
		}
	}

	// Commit the transaction and check for error.
	if err := txn.Commit(nil); err != nil {
		return err
	}

	return nil
}

func (b *BadgerStore) getByID(txn *badger.Txn, id string) (*model.Vertex, error) {
	item, err := txn.Get([]byte(id))
	if err != nil {
		return nil, err
	}

	data, err := item.Value()
	if err != nil {
		return nil, err
	}

	return b.unmarshal(data)
}

func (b *BadgerStore) unmarshal(data []byte) (*model.Vertex, error) {
	var v badgerVertex
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}

	vertex := model.NewVertex(v.ID, v.Flag, v.Rank)
	for _, parent := range v.Parents {
		vertex.Parents[parent] = struct{}{}
	}

	for childrenID := range vertex.Children {
		v.Children = append(v.Children, childrenID)
	}

	return vertex, nil
}

type badgerVertex struct {
	ID       string   `json:"id"`
	Parents  []string `json:"parents"`
	Children []string `json:"children"`
	Flag     bool     `json:"flag"`
	Rank     int      `json:"rank"`
	Index    int      `json:"index"`
}
