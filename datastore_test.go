package pebbleds

import (
	"bytes"
	"context"
	"encoding/base32"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
)

func TestPebbleDatastore(t *testing.T) {
	ds := newDatastore(t)

	dstest.SubtestAll(t, ds)
}

func newDatastore(t *testing.T, options ...Option) *Datastore {
	t.Helper()

	path := t.TempDir()

	d, err := NewDatastore(path, options...)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = d.Close()
	})

	return d
}

func newDatastoreWithPebbleDB(t *testing.T) *Datastore {
	t.Helper()

	path := t.TempDir()

	db, err := pebble.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	d, err := NewDatastore(path, WithPebbleDB(db))
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = d.Close()
	})

	return d
}

func TestGet(t *testing.T) {
	ds := newDatastore(t)

	testDatastore(t, ds)
}

func TestGetWithPebbleDB(t *testing.T) {
	ds := newDatastoreWithPebbleDB(t)

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
	dstore := newDatastore(t)

	var ds datastore.Batching
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
	for range 20 {
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

	// second commit should return a ErrBatchCommitted error.
	err = batch.Commit(ctx)
	if !errors.Is(err, ErrBatchCommitted) {
		t.Fatalf("expected error: %s, got %v", ErrBatchCommitted, err)
	}

	batch, err = ds.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for range 20 {
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

func TestPebbleWriteOptions(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		dstore := newDatastore(t)

		if dstore.writeOptions != pebble.NoSync {
			t.Fatalf("incorrect write options: expected %v, got %v", pebble.NoSync, dstore.writeOptions)
		}

		batch, err := dstore.Batch(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		if batch.(*Batch).writeOptions != pebble.NoSync {
			t.Fatalf("incorrect batch write options: expected %v, got %v", pebble.NoSync, batch.(*Batch).writeOptions)
		}
	})

	t.Run("pebble.Sync", func(t *testing.T) {
		dstore := newDatastore(t, WithPebbleWriteOptions(pebble.Sync))

		if dstore.writeOptions != pebble.Sync {
			t.Fatalf("incorrect write options: expected %v, got %v", pebble.Sync, dstore.writeOptions)
		}

		batch, err := dstore.Batch(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		if batch.(*Batch).writeOptions != pebble.Sync {
			t.Fatalf("incorrect batch write options: expected %v, got %v", pebble.Sync, batch.(*Batch).writeOptions)
		}
	})
}
