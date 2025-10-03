package pebbleds

import (
	"bytes"
	"context"
	"encoding/base32"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/cockroachdb/pebble/v2"
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

	d, err := NewDatastore(path)
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

	d, err := NewDatastore(path, WithPebbleDB(db))
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

func TestBatch(t *testing.T) {
	var ds datastore.Batching
	dstore, cleanup := newDatastore(t)
	defer cleanup()
	ds = dstore

	ctx := context.Background()

	batch, err := ds.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var seed [32]byte
	cc8 := rand.NewChaCha8(seed)
	var blocks [][]byte
	var keys []datastore.Key
	for i := 0; i < 20; i++ {
		blk := make([]byte, 256*1024)
		cc8.Read(blk)
		blocks = append(blocks, blk)

		key := datastore.NewKey(base32.StdEncoding.EncodeToString(blk[:8]))
		keys = append(keys, key)

		err := batch.Put(ctx, key, blk)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Ensure they are not in the datastore before committing
	for _, k := range keys {
		_, err := ds.Get(ctx, k)
		if err == nil {
			t.Fatal("should not have found this block")
		}
	}

	// commit, write them to the datastore
	err = batch.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// second commit should do nothing, but should not cause an error
	err = batch.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		blk := make([]byte, 256*1024)
		cc8.Read(blk)
		blocks = append(blocks, blk)

		key := datastore.NewKey(base32.StdEncoding.EncodeToString(blk[:8]))
		keys = append(keys, key)

		err := batch.Put(ctx, key, blk)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = batch.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for i, k := range keys {
		blk, err := ds.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(blk, blocks[i]) {
			t.Fatal("blocks not correct!")
		}
	}
}
