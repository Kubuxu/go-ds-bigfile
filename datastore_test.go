package main

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
)

const isSixtyFour = uint64(^uint(0)) == ^uint64(0)

func TestIsSixtyFour(t *testing.T) {
	if !isSixtyFour {
		t.Fatal("expecting 64bit int")
	}
}

func TestOpenEmptyDir(t *testing.T) {
	dirName, err := ioutil.TempDir("", "bigfile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirName)

	ds, err := Open(dirName)
	if err != ErrDoesNotExit {
		t.Fatalf("expected ErrDoesNotExit, got %s", err)
	}
	if ds != nil {
		t.Fatal("datastore should have been nil")
	}
}

func TestCreate(t *testing.T) {
	size := int64(10 << 10)
	dirName, err := ioutil.TempDir("", "bigfile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirName)

	err = Create(dirName, size)
	if err != nil {
		t.Fatal(err)
	}

	fi, err := os.Stat(path.Join(dirName, BIGFILE_NAME))
	if err != nil {
		t.Fatal(err)
	}

	if fi.Size() != size {
		t.Fatalf("bad size, expected: %d, got %d", size, fi.Size())
	}
}

func TestCreateOpen(t *testing.T) {
	size := int64(10 << 10)
	dirName, err := ioutil.TempDir("", "bigfile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirName)

	err = Create(dirName, size)
	if err != nil {
		t.Fatal(err)
	}

	ds, err := Open(dirName)
	if err != nil {
		t.Fatal(err)
	}

	if int64(len(ds.mem)) != size {
		t.Fatalf("bad size, expected: %d, got %d", size, len(ds.mem))
	}
}
