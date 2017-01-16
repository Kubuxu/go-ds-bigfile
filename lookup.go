package main

import (
	"encoding/binary"
	"errors"

	ds "github.com/ipfs/go-datastore"
	ldb "github.com/syndtr/goleveldb/leveldb"
	ldbErrors "github.com/syndtr/goleveldb/leveldb/errors"
)

const (
	BLOCK_SIZE = 4092
)

type location struct {
	offset int64
	size   int64
}

type index interface {
	Lookup(key ds.Key) (location, error)
	Allocate(key ds.Key, size int64) (*location, error)
	Finalize(key ds.Key) error
	Free(key ds.Key) error
}

type indexLDB struct {
	db          *ldb.DB
	totalBlocks int64
}

func newLDBIndex(path string, fileSize int64) (*indexLDB, error) {
	db, err := ldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	return &indexLDB{
		db:          db,
		totalBlocks: fileSize / BLOCK_SIZE,
	}, nil
}

func (*indexLDB) Lookup(key ds.Key) (location, error) {
	panic("not implemented")
}

func (idb *indexLDB) Allocate(key ds.Key, size int64) (*location, error) {
	var allocatedBlocks int64 = 0
	allocatedBlocksBytes, err := idb.db.Get([]byte("allocated"), nil)
	if err == ldbErrors.ErrNotFound {
		allocatedBlocks = 0
	} else if err != nil {
		return nil, err
	} else {
		allocatedBlocks = int64(binary.LittleEndian.Uint64(allocatedBlocksBytes) / BLOCK_SIZE)
	}

	minBlocks := (size + (BLOCK_SIZE - 1)) / BLOCK_SIZE
	if minBlocks > idb.totalBlocks-allocatedBlocks {
		return nil, errors.New("Run out of space")
	}

	binary.LittleEndian.PutUint64(allocatedBlocksBytes, allocatedBlocks + minBlocks)
	err := idb.db.Put([]byte("allocated"), allocatedBlocksBytes, nil)
	if err != nil {
		return nil, err
	}

	//TODO: Put Hash

	return &location{
		offset: allocatedBlocks * BLOCK_SIZE
		size: size
	}, nil
}

func (*indexLDB) Finalize(key ds.Key) error {
	panic("not implemented")
}

func (*indexLDB) Free(key ds.Key) error {
	panic("not implemented")
}

var _ index = (*indexLDB)(nil)
