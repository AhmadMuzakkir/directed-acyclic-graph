package boltstore

import (
	"encoding/json"
	"fmt"

	"github.com/ahmadmuzakkir/dag/model"
	"github.com/ahmadmuzakkir/dag/store"
	"github.com/boltdb/bolt"
)

var _ store.DataStore = (*BoltStore)(nil)

type BoltStore struct {
	db *bolt.DB
}

func NewBoltStore(db *bolt.DB) *BoltStore {
	return &BoltStore{
		db: db,
	}
}

func (b *BoltStore) Get() (*model.DAG, error) {
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

		for childrenID := range vertex.Children {
			v.Children = append(v.Children, childrenID)
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

func (b *BoltStore) Insert(g *model.DAG) error {
	// Convert each vertex into the internal representation of vertex.
	vertices := g.Vertices()
	var data []*boltVertex

	for _, vertex := range vertices {
		v := &boltVertex{
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

	return b.insert(data)
}

func (b *BoltStore) get() ([]*boltVertex, error) {
	var list []*boltVertex

	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("graph"))
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}

		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var vertex boltVertex
			if err := json.Unmarshal(v, &vertex); err != nil {
				return err
			}
			list = append(list, &vertex)
		}

		return nil
	})

	return list, err
}

// Insert the vertices into the database
func (b *BoltStore) insert(data []*boltVertex) error {
	t := store.StartTimer("[Bolt] insert")
	defer t()

	// Clear the old data first.
	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("graph"))
		if bucket == nil {
			return nil
		}
		return tx.DeleteBucket([]byte("graph"))
	})
	if err != nil {
		return err
	}

	// insert
	err = b.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("graph"))
		if err != nil {
			return err
		}

		for i := range data {
			b, err := json.Marshal(data[i])
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte(data[i].ID), b); err != nil {
				return err
			}
		}

		return nil
	})
	return err
}

// The internal representation of the vertex.
type boltVertex struct {
	ID       string   `json:"id"`
	Parents  []string `json:"parents"`
	Children []string `json:"children"`
	Flag     bool     `json:"flag"`
	Rank     int      `json:"rank"`
}
