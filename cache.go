package pglistener

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/lib/pq"
)

type LoadFunc[K comparable, V Keyer[K]] func(ctx context.Context, key K) (V, error)

type Cache[K comparable, V Keyer[K]] struct {
	listener *pq.Listener
	loadFunc LoadFunc[K, V]
	cache    map[K]V
	mu       sync.RWMutex
}

func NewInMemoryCache[K comparable, V Keyer[K]](ctx context.Context, listener *pq.Listener, loadFunc LoadFunc[K, V]) *Cache[K, V] {
	cache := &Cache[K, V]{
		listener: listener,
		loadFunc: loadFunc,
		cache:    make(map[K]V),
		mu:       sync.RWMutex{},
	}
	go cache.listen(ctx)
	return cache
}

func (c *Cache[K, V]) listen(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.listener.Close()
			slog.Error(fmt.Errorf("cache context is done: %v", ctx.Err()).Error())
			return
		case n := <-c.listener.Notify:
			var event ChangeDataCaptureEvent[K, V]
			if err := json.Unmarshal([]byte(n.Extra), &event); err != nil {
				slog.Error(fmt.Errorf("json unmarshalling error: %v", err).Error())
				continue
			}
			slog.Info("received cache invalidation", "table", event.Table, "action", event.Action, "key", event.Data.Key())
			switch event.Action {
			case "INSERT", "UPDATE":
				c.mu.Lock()
				c.cache[event.Data.Key()] = event.Data
				c.mu.Unlock()
			case "DELETE":
				c.mu.Lock()
				if _, ok := c.cache[event.Data.Key()]; ok {
					delete(c.cache, event.Data.Key())
				}
				c.mu.Unlock()
			}
		}
	}
}

func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, error) {
	c.mu.RLock()

	v, ok := c.cache[key]
	if ok {
		c.mu.RUnlock()
		slog.Info("fetching user form cache", "id", key)
		return v, nil
	}
	c.mu.RUnlock()

	v, err := c.loadFunc(ctx, key)
	if err != nil {
		return v, fmt.Errorf("failed to get %v : %v\n", key, err)
	}
	c.mu.Lock()
	c.cache[key] = v
	c.mu.Unlock()
	return v, nil
}
