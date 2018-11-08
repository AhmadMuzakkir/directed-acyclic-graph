package badgerstore

import (
	"encoding/json"

	"github.com/ahmadmuzakkir/dag/model"
	"github.com/ahmadmuzakkir/dag/store"
	"github.com/dgraph-io/badger"
)

var _ store.DataStore = (*BadgerStore)(nil)

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
	t := store.StartTimer("[Badger] insert")
	defer t()

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

type badgerVertex struct {
	ID       string   `json:"id"`
	Parents  []string `json:"parents"`
	Children []string `json:"children"`
	Flag     bool     `json:"flag"`
	Rank     int      `json:"rank"`
}
