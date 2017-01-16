package main

import (
	ds "github.com/ipfs/go-datastore"
	ldb "github.com/syndtr/goleveldb/leveldb"
)

type location struct {
	offset uint64
	size   uint64
}

type index interface {
	Lookup(key ds.Key) (location, error)
	Allocate(key ds.Key, size uint64) (location, error)
	Finalize(key ds.Key) error
	Free(key ds.Key) error
}

type indexLDB struct {
	db *ldb.DB
}

func newLDBIndex(path string) (*indexLDB, error) {
	db, err := ldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	return &indexLDB{
		db: db,
	}, nil
}

func (*indexLDB) Lookup(key ds.Key) (location, error) {
	panic("not implemented")
}

func (*indexLDB) Allocate(key ds.Key, size uint64) (location, error) {
	panic("not implemented")
}

func (*indexLDB) Finalize(key ds.Key) error {
	panic("not implemented")
}

func (*indexLDB) Free(key ds.Key) error {
	panic("not implemented")
}

var _ index = (*indexLDB)(nil)
