package common

import (
	"container/list"
	"sync"
	"time"
)

// LRUCache is a thread-safe LRU cache with TTL support.
type LRUCache struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	items    map[string]*list.Element
	order    *list.List
}

type lruEntry struct {
	key       string
	value     interface{}
	expiresAt time.Time
}

// NewLRUCache creates a new LRU cache with the given capacity and TTL.
func NewLRUCache(capacity int, ttl time.Duration) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		ttl:      ttl,
		items:    make(map[string]*list.Element),
		order:    list.New(),
	}
}

// Get retrieves a value from the cache. Returns (value, true) on hit, (nil, false) on miss.
func (c *LRUCache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}

	entry := elem.Value.(*lruEntry)
	if time.Now().After(entry.expiresAt) {
		c.removeLocked(elem)
		return nil, false
	}

	c.order.MoveToFront(elem)
	return entry.value, true
}

// Set adds or updates a value in the cache.
func (c *LRUCache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.order.MoveToFront(elem)
		entry := elem.Value.(*lruEntry)
		entry.value = value
		entry.expiresAt = time.Now().Add(c.ttl)
		return
	}

	if c.order.Len() >= c.capacity {
		c.evictLocked()
	}

	entry := &lruEntry{
		key:       key,
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
	elem := c.order.PushFront(entry)
	c.items[key] = elem
}

// Delete removes a key from the cache.
func (c *LRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeLocked(elem)
	}
}

// Len returns the number of items in the cache.
func (c *LRUCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func (c *LRUCache) evictLocked() {
	back := c.order.Back()
	if back != nil {
		c.removeLocked(back)
	}
}

func (c *LRUCache) removeLocked(elem *list.Element) {
	entry := elem.Value.(*lruEntry)
	delete(c.items, entry.key)
	c.order.Remove(elem)
}
