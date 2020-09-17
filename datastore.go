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

// Datastore is a pebble-backed github.com/ipfs/go-datastore.Datastore.
//
// It supports batching. It does not support TTL or transactions, because pebble
// doesn't have those features.
//
// The query capabilities are limited in this regard:
//
//  * query.Orders: zero or one order criteria are accepted. If provided, it
//    must be OrderByKey or OrderByKeyDescending. Custom order functions are
//    not supported.
//  * query.ReturnExpirations is not supported, and is ignored.
//
// Get, GetSize, Has all perform an underying store Get, as pebble doesn't
// support fetching values lazily.
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
		return -1, err
	}
	return len(val), nil
}

func (d *Datastore) Query(q query.Query) (query.Results, error) {
	var (
		prefix      = ds.NewKey(q.Prefix).String()
		limit       = q.Limit
		offset      = q.Offset
		orders      = q.Orders
		filters     = q.Filters
		keysOnly    = q.KeysOnly
		_           = q.ReturnExpirations // pebble doesn't support TTL; noop
		returnSizes = q.ReturnsSizes
	)

	if prefix != "/" {
		prefix = prefix + "/"
	}

	opts := &pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: func() []byte {
			// if the prefix is 0x01..., we want 0x02 as an upper bound.
			// if the prefix is 0x0000ff..., we want 0x0001 as an upper bound.
			// if the prefix is 0x0000ff01..., we want 0x0000ff02 as an upper bound.
			// if the prefix is 0xffffff..., we don't want an upper bound.
			// if the prefix is 0xff..., we don't want an upper bound.
			// if the prefix is empty, we don't want an upper bound.
			// basically, we want to find the last byte that can be lexicographically incremented.
			var upper []byte
			for i := len(prefix) - 1; i >= 0; i-- {
				b := prefix[i]
				if b == 0xff {
					continue
				}
				upper = make([]byte, i+1)
				copy(upper, prefix)
				upper[i] = b + 1
				break
			}
			return upper
		}(),
	}

	iter := d.db.NewIter(opts)

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
			return d.inefficientOrderQuery(q, nil)
		}
	default:
		var baseOrder query.Order
		for _, o := range orders {
			if baseOrder != nil {
				return nil, fmt.Errorf("incompatible orders passed: %+v", orders)
			}
			switch o.(type) {
			case query.OrderByKey, query.OrderByKeyDescending, *query.OrderByKey, *query.OrderByKeyDescending:
				baseOrder = o
			}
		}
		return d.inefficientOrderQuery(q, baseOrder)
	}

	if !iter.Valid() {
		_ = iter.Close()
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
		// iter.Key and iter.Value may change on the next call to iter.Next.
		// string conversion takes a copy
		entry := query.Entry{Key: string(iter.Key())}
		if !keysOnly {
			// take a copy.
			cpy := make([]byte, len(iter.Value()))
			copy(cpy, iter.Value())
			entry.Value = cpy
		}
		if returnSizes {
			entry.Size = len(iter.Value())
		}
		return entry
	}

	d.wg.Add(1)
	results := query.ResultsWithProcess(q, func(proc goprocess.Process, outCh chan<- query.Result) {
		defer d.wg.Done()
		defer iter.Close()

		const interrupted = "interrupted"

		defer func() {
			switch r := recover(); r {
			case nil, interrupted:
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
			panic(interrupted)
		}

		// skip over 'offset' entries; if a filter is provided, only entries
		// that match the filter will be counted as a skipped entry.
		for skipped := 0; skipped < offset && iter.Valid(); move() {
			if err := iter.Error(); err != nil {
				sendOrInterrupt(query.Result{Error: err})
			}
			if doFilter && !filterFn(createEntry()) {
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
	err := d.db.Set(key.Bytes(), value, pebble.NoSync)
	if err != nil {
		return fmt.Errorf("pebble error during set: %w", err)
	}
	return nil
}

func (d *Datastore) Delete(key ds.Key) error {
	err := d.db.Delete(key.Bytes(), pebble.NoSync)
	if err != nil {
		return fmt.Errorf("pebble error during delete: %w", err)
	}
	return nil
}

func (d *Datastore) Sync(_ ds.Key) error {
	// pebble provides a Flush operation, but it writes the memtables to stable
	// storage. That's not what Sync is supposed to do. Sync is supposed to
	// guarantee that previously performed write operations will survive a machine
	// crash. In pebble this is done by fsyncing the WAL, which can be requested when
	// performing write operations. But there is no separate operation to fsync
	// only. The closest is LogData, which actually writes a log entry on the WAL.
	err := d.db.LogData(nil, pebble.Sync)
	if err != nil {
		return fmt.Errorf("pebble error during sync: %w", err)
	}
	return nil
}

func (d *Datastore) Batch() (ds.Batch, error) {
	return &Batch{d.db.NewBatch()}, nil
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

func (d *Datastore) inefficientOrderQuery(q query.Query, baseOrder query.Order) (query.Results, error) {
	// Ok, we have a weird order we can't handle. Let's
	// perform the _base_ query (prefix, filter, etc.), then
	// handle sort/offset/limit later.

	// Skip the stuff we can't apply.
	baseQuery := q
	baseQuery.Limit = 0
	baseQuery.Offset = 0
	baseQuery.Orders = nil
	if baseOrder != nil {
		baseQuery.Orders = []query.Order{baseOrder}
	}

	// perform the base query.
	res, err := d.Query(baseQuery)
	if err != nil {
		return nil, err
	}

	// fix the query
	res = query.ResultsReplaceQuery(res, q)

	// Remove the parts we've already applied.
	naiveQuery := q
	naiveQuery.Prefix = ""
	naiveQuery.Filters = nil

	// Apply the rest of the query
	return query.NaiveQueryApply(naiveQuery, res), nil
}

type Batch struct {
	batch *pebble.Batch
}

var _ ds.Batch = (*Batch)(nil)

func (b *Batch) Put(key ds.Key, value []byte) error {
	err := b.batch.Set(key.Bytes(), value, pebble.NoSync)
	if err != nil {
		return fmt.Errorf("pebble error during set within batch: %w", err)
	}
	return nil
}

func (b *Batch) Delete(key ds.Key) error {
	err := b.batch.Delete(key.Bytes(), pebble.NoSync)
	if err != nil {
		return fmt.Errorf("pebble error during delete within batch: %w", err)
	}
	return nil
}

func (b *Batch) Commit() error {
	return b.batch.Commit(nil)
}
