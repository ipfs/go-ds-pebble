package pebbleds

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
)

func TestPebbleDatastore(t *testing.T) {
	ds, cleanup := newDatastore(t)
	defer cleanup()

	dstest.SubtestAll(t, ds)
}

func newDatastore(t *testing.T) (*Datastore, func()) {
	t.Helper()

	path, err := os.MkdirTemp(os.TempDir(), "testing_pebble_")
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

func newDatastoreWithPebbleDB(t *testing.T) (*Datastore, func()) {
	t.Helper()

	path, err := os.MkdirTemp(os.TempDir(), "testing_pebble_with_db")
	if err != nil {
		t.Fatal(err)
	}

	db, err := pebble.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	d, err := NewDatastore(path, nil, WithPebbleDB(db))
	if err != nil {
		t.Fatal(err)
	}

	return d, func() {
		_ = d.Close()
		_ = os.RemoveAll(path)
	}
}

func TestGet(t *testing.T) {
	ds, cleanup := newDatastore(t)
	defer cleanup()

	testDatastore(t, ds)
}

func TestGetWithPebbleDB(t *testing.T) {
	ds, cleanup := newDatastoreWithPebbleDB(t)
	defer cleanup()

	testDatastore(t, ds)
}

func testDatastore(t *testing.T, ds *Datastore) {
	ctx := context.Background()
	k := datastore.NewKey("a")
	v := []byte("val")
	err := ds.Put(ctx, k, v)
	if err != nil {
		t.Fatal(err)
	}

	err = ds.Put(ctx, datastore.NewKey("aa"), v)
	if err != nil {
		t.Fatal(err)
	}

	err = ds.Put(ctx, datastore.NewKey("ac"), v)
	if err != nil {
		t.Fatal(err)
	}

	has, err := ds.Has(ctx, datastore.NewKey("ab"))
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Fatal("should not have key")
	}

	val, err := ds.Get(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, val) {
		t.Error("not equal", string(val))
	}
}
