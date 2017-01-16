package main

import (
	"io/ioutil"
	"os"
	"testing"
)

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
	dirName, err := ioutil.TempDir("", "bigfile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirName)

	err = Create(dirName, 10<<10)
	if err != nil {
		t.Fatal(err)
	}
}
