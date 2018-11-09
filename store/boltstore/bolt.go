package boltstore

import (
	"encoding/json"
	"fmt"

	"github.com/ahmadmuzakkir/dag/model"
	"github.com/ahmadmuzakkir/dag/store"
	"github.com/boltdb/bolt"
)

var _ store.GraphStore = (*BoltStore)(nil)

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

func (b *BoltStore) GetVertexByPosition(position int) (*model.Vertex, error) {
	var vertex *model.Vertex
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("graph"))
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}

		i := 0
		c := bucket.Cursor()
		var err error

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if i == position {
				if vertex, err = b.unmarshal(v); err != nil {
					return err
				}
			}
			i++
		}

		return nil
	})

	return vertex, err
}

func (b *BoltStore) AncestorsBFS(id string, filter func(*model.Vertex) bool) ([]*model.Vertex, error) {
	var list []*model.Vertex

	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("graph"))
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}

		v := bucket.Get([]byte(id))
		if v == nil {
			return fmt.Errorf("vertex with id %s does not exist", id)
		}

		q := []string{id}
		visited := make(map[string]struct{})
		visited[id] = struct{}{}

		for len(q) != 0 {
			u := q[0]
			q = q[1:len(q):len(q)]

			v, err := b.getByID(bucket, u)
			if err != nil {
				return err
			}

			for p, _ := range v.Parents {
				if _, ok := visited[p]; !ok {
					q = append(q, p)
					visited[p] = struct{}{}

					pv, err := b.getByID(bucket, p)
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

func (b *BoltStore) AncestorsDFS(id string, filter func(*model.Vertex) bool) ([]*model.Vertex, error) {
	var list []*model.Vertex

	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("graph"))
		if bucket == nil {
			return fmt.Errorf("bucket does not exist")
		}

		_, err := b.getByID(bucket, id)
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

				v, err := b.getByID(bucket, u)
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

func (b *BoltStore) Reach(id string) (int, error) {
	list, err := b.AncestorsDFS(id, nil)
	if err != nil {
		return 0, err
	}
	return len(list), nil
}

func (b *BoltStore) ConditionalReach(id string, flag bool) (int, error) {
	list, err := b.AncestorsDFS(id, func(v *model.Vertex) bool {
		return v.Flag == flag
	})

	if err != nil {
		return 0, err
	}
	return len(list), nil
}

func (b *BoltStore) List(id string) ([]*model.Vertex, error) {
	list, err := b.AncestorsDFS(id, nil)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (b *BoltStore) ConditionalList(id string, flag bool) ([]*model.Vertex, error) {
	list, err := b.AncestorsDFS(id, func(v *model.Vertex) bool {
		return v.Flag == flag
	})

	if err != nil {
		return nil, err
	}
	return list, nil
}

func (b *BoltStore) getByID(bucket *bolt.Bucket, id string) (*model.Vertex, error) {
	data := bucket.Get([]byte(id))
	if data == nil {
		return nil, fmt.Errorf("vertex with id %s does not exist", id)
	}

	return b.unmarshal(data)
}

func (b *BoltStore) unmarshal(data []byte) (*model.Vertex, error) {
	var v boltVertex
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("unmarshal error: %s", err)
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
