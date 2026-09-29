package pebbleds

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

func TestQueryReadErrors(t *testing.T) {
	for _, tc := range []struct {
		name           string
		q              query.Query
		failFirst      bool
		failAfterEntry bool
		wantError      bool
		wantEntries    int
	}{
		{name: "first read", failFirst: true, wantError: true},
		{name: "forward", failAfterEntry: true, wantError: true, wantEntries: 1},
		{name: "reverse", q: query.Query{Orders: []query.Order{query.OrderByKeyDescending{}}}, failAfterEntry: true, wantError: true, wantEntries: 1},
		{name: "offset", q: query.Query{Offset: 2}, failAfterEntry: true, wantError: true},
		{name: "keys only", q: query.Query{KeysOnly: true}, failAfterEntry: true, wantError: true, wantEntries: 1},
		{name: "value order", q: query.Query{Orders: []query.Order{query.OrderByValue{}}}, failAfterEntry: true, wantError: true, wantEntries: 1},
		{name: "limit reached", q: query.Query{Limit: 1}, failAfterEntry: true, wantEntries: 1},
		{name: "healthy", wantEntries: 50},
		{name: "empty prefix", q: query.Query{Prefix: "missing"}},
		{name: "offset past end", q: query.Query{Offset: 50}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var failReads atomic.Bool
			var failures atomic.Int32
			fs := errorfs.Wrap(vfs.Default, errorfs.InjectorFunc(func(op errorfs.Op) error {
				if failReads.Load() && op.Kind == errorfs.OpFileReadAt && strings.HasSuffix(op.Path, ".sst") {
					failures.Add(1)
					return errorfs.ErrInjected
				}
				return nil
			}))
			cache := pebble.NewCache(0)
			t.Cleanup(cache.Unref)
			opts := &pebble.Options{
				FS:                          fs,
				Cache:                       cache,
				DisableAutomaticCompactions: true,
				DisableTableStats:           true,
				Levels:                      [7]pebble.LevelOptions{{BlockSize: 128}},
			}
			path := t.TempDir()
			d, err := NewDatastore(path, WithPebbleOpts(opts))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				failReads.Store(false)
				if d != nil {
					if err := d.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			for i := 0; i < 50; i++ {
				if err := d.Put(context.Background(), ds.NewKey(fmt.Sprintf("key/%03d", i)), []byte(strings.Repeat("x", 256))); err != nil {
					t.Fatal(err)
				}
			}
			if err := d.DB.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			// Reopen so the table data must be read from the filesystem.
			d, err = NewDatastore(path, WithPebbleOpts(opts))
			if err != nil {
				t.Fatal(err)
			}
			if tc.failAfterEntry {
				tc.q.Filters = []query.Filter{armReadFailure{&failReads}}
			}
			failReads.Store(tc.failFirst)
			results, err := d.Query(context.Background(), tc.q)
			entries, errorResults := 0, 0
			if err == nil {
				for result := range results.Next() {
					if result.Error != nil {
						errorResults++
						if result.Key != "" || result.Value != nil {
							t.Error("error result contains an entry")
						}
						if !errors.Is(result.Error, errorfs.ErrInjected) {
							t.Errorf("unexpected error: %v", result.Error)
						}
						err = result.Error
					} else {
						entries++
					}
				}
				if e := results.Close(); e != nil {
					t.Fatal(e)
				}
			}
			failReads.Store(false)
			t.Logf("entries=%d errorResults=%d injected=%d err=%v", entries, errorResults, failures.Load(), err)
			if tc.wantError {
				if !errors.Is(err, errorfs.ErrInjected) || failures.Load() == 0 {
					t.Fatalf("expected injected read error, got %v (%d injected)", err, failures.Load())
				}
				if errorResults > 1 {
					t.Fatalf("got %d error results", errorResults)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if failures.Load() != 0 {
					t.Fatalf("unexpected read past query limit: %d injected", failures.Load())
				}
			}
			if entries != tc.wantEntries {
				t.Errorf("got %d entries, want %d", entries, tc.wantEntries)
			}
		})
	}
}

// armReadFailure lets the first entry through, then fails later filesystem reads.
type armReadFailure struct{ enabled *atomic.Bool }

func (f armReadFailure) Filter(query.Entry) bool {
	f.enabled.Store(true)
	return true
}
