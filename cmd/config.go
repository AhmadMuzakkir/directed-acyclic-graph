package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ahmadmuzakkir/dag/store"
	"github.com/ahmadmuzakkir/dag/store/badgerstore"
	"github.com/ahmadmuzakkir/dag/store/boltstore"
	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
)

// File path to the bolt db
const BoltPath = "/tmp/bolt/graph_bolt.db"

// Folder path to the directory to store badger files
const BadgerDirPath = "/tmp/badger"

// 1 = Bolt, 2 = Badger
const DBType = 2

func GetBoltDataStore(path string) (*boltstore.BoltStore, func(), error) {
	// Create directory if it does not exist
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err2 := os.MkdirAll(filepath.Dir(path), os.ModePerm); err2 != nil {
			return nil, func() {}, err2
		}
	}

	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, func() {}, err
	}

	var ds = boltstore.NewBoltStore(db)
	if err != nil {
		return nil, func() { db.Close() }, err
	}

	return ds, func() { db.Close() }, nil
}

func GetBadgerDataStore(dir string) (*badgerstore.BadgerStore, func(), error) {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		return nil, func() {}, err
	}

	var ds = badgerstore.NewBadgerStore(db)
	return ds, func() { db.Close() }, nil
}

func GetDataStore() (store.DataStore, func(), error) {
	if DBType == 1 {
		return GetBoltDataStore(BoltPath)
	} else if DBType == 2 {
		return GetBadgerDataStore(BadgerDirPath)
	} else {
		return nil, func() {}, fmt.Errorf("unknown DBType %v", DBType)
	}
}
