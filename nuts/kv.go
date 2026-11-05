package nuts

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

// Bucket is a typed view of an existing key-value bucket. It never creates one.
type Bucket[T any] struct {
	rt   *Runtime
	name string
	mu   sync.Mutex
	kv   jetstream.KeyValue
}

// NewBucket returns a Bucket that binds on first use.
func NewBucket[T any](r *Runtime, name string) *Bucket[T] {
	return &Bucket[T]{rt: r, name: name}
}

func (b *Bucket[T]) bind(ctx context.Context) (jetstream.KeyValue, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.kv == nil {
		kv, err := b.rt.js.KeyValue(ctx, b.name)
		if err != nil {
			return nil, fmt.Errorf("nuts: bind bucket %s: %w", b.name, err)
		}
		b.kv = kv
	}
	return b.kv, nil
}

func (b *Bucket[T]) do(ctx context.Context, key string, fn func(context.Context, jetstream.KeyValue) error) error {
	c := &Call{Kind: KindKV, Name: b.name, Subject: key}
	return b.rt.do(ctx, c, func(ctx context.Context) error {
		kv, err := b.bind(ctx)
		if err != nil {
			return err
		}
		return fn(ctx, kv)
	})
}

// Get returns a key's value and revision.
func (b *Bucket[T]) Get(ctx context.Context, key string) (*T, uint64, error) {
	var out *T
	var rev uint64
	err := b.do(ctx, key, func(ctx context.Context, kv jetstream.KeyValue) error {
		e, err := kv.Get(ctx, key)
		if err != nil {
			return err
		}
		out = new(T)
		rev = e.Revision()
		if err := json.Unmarshal(e.Value(), out); err != nil {
			return fmt.Errorf("nuts: decode %s/%s: %w", b.name, key, err)
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return out, rev, nil
}

func (b *Bucket[T]) write(ctx context.Context, key string, v T, fn func(context.Context, jetstream.KeyValue, []byte) (uint64, error)) (uint64, error) {
	data, err := encode(v)
	if err != nil {
		return 0, err
	}
	var rev uint64
	err = b.do(ctx, key, func(ctx context.Context, kv jetstream.KeyValue) error {
		var err error
		rev, err = fn(ctx, kv, data)
		return err
	})
	return rev, err
}

// Put writes a value and returns its revision.
func (b *Bucket[T]) Put(ctx context.Context, key string, v T) (uint64, error) {
	return b.write(ctx, key, v, func(ctx context.Context, kv jetstream.KeyValue, data []byte) (uint64, error) {
		return kv.Put(ctx, key, data)
	})
}

// Create writes a value only if the key does not exist.
func (b *Bucket[T]) Create(ctx context.Context, key string, v T) (uint64, error) {
	return b.write(ctx, key, v, func(ctx context.Context, kv jetstream.KeyValue, data []byte) (uint64, error) {
		return kv.Create(ctx, key, data)
	})
}

// Update writes a value only if the key is still at the given revision.
func (b *Bucket[T]) Update(ctx context.Context, key string, v T, revision uint64) (uint64, error) {
	return b.write(ctx, key, v, func(ctx context.Context, kv jetstream.KeyValue, data []byte) (uint64, error) {
		return kv.Update(ctx, key, data, revision)
	})
}

// Delete removes a key.
func (b *Bucket[T]) Delete(ctx context.Context, key string) error {
	return b.do(ctx, key, func(ctx context.Context, kv jetstream.KeyValue) error {
		return kv.Delete(ctx, key)
	})
}

// Entry is one change seen by Watch. Value is nil when Deleted is true.
type Entry[T any] struct {
	Key      string
	Value    *T
	Revision uint64
	Deleted  bool
}

// Watch calls fn with the latest value of each key and then with every change.
// Values that cannot be decoded are skipped.
func (b *Bucket[T]) Watch(ctx context.Context, fn func(Entry[T])) (stop func(), err error) {
	kv, err := b.bind(ctx)
	if err != nil {
		return nil, err
	}
	w, err := kv.WatchAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("nuts: watch %s: %w", b.name, err)
	}
	go func() {
		for e := range w.Updates() {
			if e == nil {
				continue // nil marks the end of the initial values
			}
			out := Entry[T]{Key: e.Key(), Revision: e.Revision(), Deleted: e.Operation() != jetstream.KeyValuePut}
			if !out.Deleted {
				out.Value = new(T)
				if json.Unmarshal(e.Value(), out.Value) != nil {
					continue
				}
			}
			fn(out)
		}
	}()
	return func() { _ = w.Stop() }, nil
}
