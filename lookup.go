package main

import (
	"encoding/binary"
	"errors"

	ds "github.com/ipfs/go-datastore"
	ldb "github.com/syndtr/goleveldb/leveldb"
	ldbErrors "github.com/syndtr/goleveldb/leveldb/errors"
)

const (
	BLOCK_SIZE    = 4 << 10
	IDB_FINALIZED = 0x1
)

type location struct {
	offset int64
	size   int64
	flags  int32
}

type index interface {
	Lookup(key ds.Key) (*location, error)
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

func (idb *indexLDB) Lookup(key ds.Key) (*location, error) {
	locationBuf, err := idb.db.Get([]byte("k/"+key.String()), nil)
	if err != nil {
		return nil, err
	}

	return &location{
		offset: int64(binary.LittleEndian.Uint64(locationBuf)),
		size:   int64(binary.LittleEndian.Uint64(locationBuf[8:])),
		flags:  int32(binary.LittleEndian.Uint32(locationBuf[16:])),
	}, nil
}

func (idb *indexLDB) Allocate(key ds.Key, size int64) (*location, error) {
	//TODO: Locking
	//See https://github.com/golang/leveldb/blob/master/db/file_lock_test.go

	var allocatedBlocks int64 = 0
	allocatedBlocksBytes, err := idb.db.Get([]byte("i/allocated"), nil)
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

	transaction, err := idb.db.OpenTransaction()
	if err != nil {
		return nil, err
	}

	allocatedBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(allocatedBuf, uint64(allocatedBlocks+minBlocks))
	err = transaction.Put([]byte("i/allocated"), allocatedBuf, nil)
	if err != nil {
		return nil, err
	}

	flags := int32(0)
	keyLocationBuf := make([]byte, 20)
	binary.LittleEndian.PutUint64(keyLocationBuf, uint64(allocatedBlocks))
	binary.LittleEndian.PutUint64(keyLocationBuf[8:], uint64(size))
	binary.LittleEndian.PutUint32(keyLocationBuf[16:], uint32(flags))
	err = transaction.Put([]byte("k/"+key.String()), keyLocationBuf, nil)
	if err != nil {
		return nil, err
	}

	err = transaction.Commit()
	if err != nil {
		return nil, err
	}

	return &location{
		offset: allocatedBlocks * BLOCK_SIZE,
		size:   size,
		flags:  flags,
	}, nil
}

func (idb *indexLDB) Finalize(key ds.Key) error {
	locationBuf, err := idb.db.Get([]byte("k/"+key.String()), nil)
	if err != nil {
		return err
	}

	flags := int32(binary.LittleEndian.Uint32(locationBuf[16:])) | IDB_FINALIZED
	binary.LittleEndian.PutUint32(locationBuf[16:], uint32(flags))
	return idb.db.Put([]byte("k/"+key.String()), locationBuf, nil)
}

func (*indexLDB) Free(key ds.Key) error {
	panic("not implemented")
}

var _ index = (*indexLDB)(nil)
