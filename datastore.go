package pebbleds

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/jbenet/goprocess"
)

type Datastore struct {
	db      *pebble.DB
	status  int32
	closing chan struct{}
	wg      sync.WaitGroup
}

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.Batching = (*Datastore)(nil)

// NewDatastore creates a pebble-backed datastore.
func NewDatastore(path string, opts *pebble.Options) (*Datastore, error) {
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble database: %w", err)
	}

	store := &Datastore{
		db:      db,
		closing: make(chan struct{}),
	}

	return store, nil
}

func (d *Datastore) Get(key ds.Key) (value []byte, err error) {
	val, closer, err := d.db.Get(key.Bytes())
	switch err {
	case pebble.ErrNotFound:
		return nil, ds.ErrNotFound
	case nil:
		_ = closer.Close()
		return val, nil
	default:
		return nil, fmt.Errorf("pebble error during get: %w", err)
	}
}

func (d *Datastore) Has(key ds.Key) (exists bool, _ error) {
	_, err := d.Get(key)
	switch err {
	case ds.ErrNotFound:
		return false, nil
	case nil:
		return true, nil
	default:
		return false, err
	}
}

func (d *Datastore) GetSize(key ds.Key) (size int, _ error) {
	val, err := d.Get(key)
	if err != nil {
		return 0, err
	}
	return len(val), nil
}

func (d *Datastore) Query(q query.Query) (query.Results, error) {
	var (
		prefix      = q.Prefix
		limit       = q.Limit
		offset      = q.Offset
		orders      = q.Orders
		filters     = q.Filters
		keysOnly    = q.KeysOnly
		_           = q.ReturnExpirations // pebble doesn't support TTL; noop
		returnSizes = q.ReturnsSizes
	)

	opts := &pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix),
	}

	iter := d.db.NewIter(opts)
	defer iter.Close()

	var move func() bool
	switch l := len(orders); l {
	case 0:
		iter.First()
		move = iter.Next
	case 1:
		switch o := orders[0]; o.(type) {
		case query.OrderByKey, *query.OrderByKey:
			iter.First()
			move = iter.Next
		case query.OrderByKeyDescending, *query.OrderByKeyDescending:
			iter.Last()
			move = iter.Prev
		default:
			return nil, fmt.Errorf("unsupported order criterium of type: %T", o)
		}
	default:
		return nil, fmt.Errorf("pebble query supports at most 1 order criterium; provided: %d", l)
	}

	if iter.Valid() {
		// there are no valid results.
		return query.ResultsWithEntries(q, []query.Entry{}), nil
	}

	// filterFn takes an Entry and tells us if we should return it.
	filterFn := func(entry query.Entry) bool {
		for _, f := range filters {
			if !f.Filter(entry) {
				return false
			}
		}
		return true
	}
	doFilter := false
	if len(filters) > 0 {
		doFilter = true
	}

	createEntry := func() query.Entry {
		entry := query.Entry{Key: string(iter.Key())}
		if !keysOnly {
			entry.Value = iter.Value()
		}
		if returnSizes {
			entry.Size = len(iter.Value())
		}
		return entry
	}

	d.wg.Add(1)
	results := query.ResultsWithProcess(q, func(proc goprocess.Process, outCh chan<- query.Result) {
		defer d.wg.Done()
		defer close(outCh)
		defer func() {
			switch r := recover(); r {
			case nil, "interrupted":
				// nothing, or flow interrupted; all ok.
			default:
				panic(r) // a genuine panic, propagate.
			}
		}()

		sendOrInterrupt := func(r query.Result) {
			select {
			case outCh <- r:
				return
			case <-d.closing:
			case <-proc.Closed():
			case <-proc.Closing(): // client told us to close early
			}

			// we are closing; try to send a closure error to the client.
			// but do not halt because they might have stopped receiving.
			select {
			case outCh <- query.Result{Error: fmt.Errorf("close requested")}:
			default:
			}
			panic("interrupted")
		}

		// skip over 'offset' entries; if a filter is provided, only entries
		// that match the filter will be counted as a skipped entry.
		for skipped := 0; skipped < offset && iter.Valid(); move() {
			if err := iter.Error(); err != nil {
				sendOrInterrupt(query.Result{Error: err})
			}
			if doFilter || !filterFn(createEntry()) {
				// if we have a filter, and this entry doesn't match it,
				// don't count it.
				continue
			}
			skipped++
		}

		// start sending results, capped at limit (if > 0)
		for sent := 0; (limit <= 0 || sent < limit) && iter.Valid(); move() {
			if err := iter.Error(); err != nil {
				sendOrInterrupt(query.Result{Error: err})
			}
			entry := createEntry()
			if doFilter && !filterFn(entry) {
				// if we have a filter, and this entry doesn't match it,
				// do not sendOrInterrupt it.
				continue
			}
			sendOrInterrupt(query.Result{Entry: entry})
			sent++
		}
	})
	return results, nil
}

func (d *Datastore) Put(key ds.Key, value []byte) error {
	err := d.db.Set(key.Bytes(), value, nil)
	if err != nil {
		return fmt.Errorf("pebble error during set: %w", err)
	}
	return nil
}

func (d *Datastore) Delete(key ds.Key) error {
	err := d.db.Delete(key.Bytes(), nil)
	if err != nil {
		return fmt.Errorf("pebble error during delete: %w", err)
	}
	return nil
}

func (d *Datastore) Sync(_ ds.Key) error {
	// pebble provides a Flush operation, but it writes the memtables to stable
	// storage. That's not what Sync is supposed to do. Sync is supposed to
	// guarantee that previously performed write operations will survive a machine
	// crash. In pebble this is done by fsyncing the WAL, which can be done while
	// performing write operations. But there is no separate operation to fsync
	// only. The closest is LogData, which actually writes a log entry on the WAL.
	err := d.db.LogData(nil, &pebble.WriteOptions{Sync: true})
	if err != nil {
		return fmt.Errorf("pebble error during sync: %w", err)
	}
	return nil
}

func (d *Datastore) Batch() (ds.Batch, error) {
	panic("implement me")
}

func (d *Datastore) Close() error {
	if !atomic.CompareAndSwapInt32(&d.status, 0, 1) {
		// already closed, or closing.
		d.wg.Wait()
		return nil
	}
	close(d.closing)
	d.wg.Wait()
	return d.db.Close()
}
