package pebbleds

import (
	"io/ioutil"
	"os"
	"testing"

	dstest "github.com/ipfs/go-datastore/test"
)

func TestPebbleDatastore(t *testing.T) {
	dstest.SubtestAll(t, newDatastore(t))
}

func newDatastore(t *testing.T) *Datastore {
	t.Helper()

	path, err := ioutil.TempDir(os.TempDir(), "testing_pebble_")
	if err != nil {
		t.Fatal(err)
	}

	d, err := NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = d.Close()
		_ = os.RemoveAll(path)
	})

	return d
}
