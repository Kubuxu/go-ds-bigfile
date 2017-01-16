package main

import (
	"errors"
	"os"
	"path"

	"github.com/tysonmote/gommap"
)

const (
	BIGFILE_NAME = "bigfile.bin"
)

var (
	ErrDoesNotExit = errors.New("the bigfile does not exit")
)

type Datastore struct {
	mem   gommap.MMap
	bigfd *os.File

	index index
}

func checkDir(p string) error {
	fi, err := os.Lstat(p)
	if err != nil {
		return err
	}
	if !fi.IsDir() {

		return errors.New("path doesn't point to directory")
	}
	return nil
}

// Opens the BigFile datastore, path should be a path to directory
func Open(dsPath string) (*Datastore, error) {
	var err error

	ds := &Datastore{}

	bigPath := path.Join(dsPath, BIGFILE_NAME)
	ds.bigfd, err = os.OpenFile(bigPath, os.O_RDWR, 0666)

	if os.IsNotExist(err) {
		return nil, ErrDoesNotExit
	}
	if err != nil {
		return nil, err
	}

	ds.mem, err = gommap.Map(ds.bigfd.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	ds.index, err = newLDBIndex(path.Join(dsPath, "ldbindex"), int64(len(ds.mem)))
	if err != nil {
		return nil, err
	}

	return ds, nil
}

func Create(dsPath string, size int64) error {
	err := checkDir(dsPath)
	if err != nil {
		return err
	}

	fPath := path.Join(dsPath, BIGFILE_NAME)

	_, err = os.Stat(fPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if !os.IsNotExist(err) {
		return errors.New("the database already exists")
	}

	f, err := os.Create(fPath)
	if err != nil {
		return err
	}

	err = f.Truncate(size)
	if err != nil {
		return err
	}

	return nil

}
