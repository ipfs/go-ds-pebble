package pebbleds

import (
	"io/ioutil"
	"os"
	"testing"

	dstest "github.com/ipfs/go-datastore/test"
)

func TestPebbleDatastore(t *testing.T) {
	ds, cleanup := newDatastore(t)
	defer cleanup()

	dstest.SubtestAll(t, ds)
}

func newDatastore(t *testing.T) (*Datastore, func()) {
	t.Helper()

	path, err := ioutil.TempDir(os.TempDir(), "testing_pebble_")
	if err != nil {
		t.Fatal(err)
	}

	d, err := NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	return d, func() {
		_ = d.Close()
		_ = os.RemoveAll(path)
	}
}
