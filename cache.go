// Copyright (c) 2014 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Adapted from:
// https://code.google.com/p/vitess/source/browse/go/cache/lru_cache.go

// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The implementation borrows heavily from SmallLRUCache (originally by Nathan
// Schrenk). The object maintains a doubly-linked list of elements in the
// When an element is accessed it is promoted to the head of the list, and when
// space is needed the element at the tail of the list (the least recently used
// element) is evicted.

package occult

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type cache struct {
	mu sync.Mutex

	// list & table of *entry objects
	list  *list.List
	table map[uint64]*list.Element

	// How many elements we can store in the cache before evicting.
	capacity uint64
}

type item struct {
	Key   uint64
	Value Value
}

type entry struct {
	key           uint64
	value         Value
	time_accessed time.Time
}

func newCache(capacity uint64) *cache {
	return &cache{
		list:     list.New(),
		table:    make(map[uint64]*list.Element),
		capacity: capacity,
	}
}

func (c *cache) get(key uint64) (v Value, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element := c.table[key]
	if element == nil {
		return nil, false
	}
	c.moveToFront(element)
	return element.Value.(*entry).value, true
}

func (c *cache) getSlice(start uint64, size int) (sl *Slice) {
	sl = NewSlice(start, 0, size)
	for k, _ := range sl.Data {
		v, ok := c.get(start + uint64(k))
		if !ok {
			break
		}
		sl.Data = append(sl.Data, v)
	}
	return
}

func (c *cache) set(key uint64, value Value) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element := c.table[key]; element != nil {
		c.updateInplace(element, value)
	} else {
		c.addNew(key, value)
	}
}

func (c *cache) setSlice(start uint64, sl *Slice) {

	for k, v := range sl.Data {
		key := start + uint64(k)
		c.set(key, v)
	}
}

func (c *cache) setIfAbsent(key uint64, value Value) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element := c.table[key]; element != nil {
		c.moveToFront(element)
	} else {
		c.addNew(key, value)
	}
}

func (c *cache) delete(key uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	element := c.table[key]
	if element == nil {
		return false
	}

	c.list.Remove(element)
	delete(c.table, key)
	return true
}

func (c *cache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.list.Init()
	c.table = make(map[uint64]*list.Element)
}

func (c *cache) setCapacity(capacity uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.capacity = capacity
	c.checkCapacity()
}

func (c *cache) stats() (length, capacity uint64, oldest time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if lastElem := c.list.Back(); lastElem != nil {
		oldest = lastElem.Value.(*entry).time_accessed
	}
	return uint64(c.list.Len()), c.capacity, oldest
}

func (c *cache) statsJSON() string {
	if c == nil {
		return "{}"
	}
	l, cap, o := c.stats()
	return fmt.Sprintf("{\"Length\": %v, \"Capacity\": %v, \"OldestAccess\": \"%v\"}", l, cap, o)
}

func (c *cache) keys() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	keys := make([]uint64, 0, c.list.Len())
	for e := c.list.Front(); e != nil; e = e.Next() {
		keys = append(keys, e.Value.(*entry).key)
	}
	return keys
}

func (c *cache) Items() []item {
	c.mu.Lock()
	defer c.mu.Unlock()

	items := make([]item, 0, c.list.Len())
	for e := c.list.Front(); e != nil; e = e.Next() {
		v := e.Value.(*entry)
		items = append(items, item{Key: v.key, Value: v.value})
	}
	return items
}

func (c *cache) updateInplace(element *list.Element, value Value) {
	element.Value.(*entry).value = value
	c.moveToFront(element)
	c.checkCapacity()
}

func (c *cache) moveToFront(element *list.Element) {
	c.list.MoveToFront(element)
	element.Value.(*entry).time_accessed = time.Now()
}

func (c *cache) addNew(key uint64, value Value) {
	newEntry := &entry{key, value, time.Now()}
	element := c.list.PushFront(newEntry)
	c.table[key] = element
	c.checkCapacity()
}

func (c *cache) checkCapacity() {
	for uint64(c.list.Len()) > c.capacity {
		delElem := c.list.Back()
		delValue := delElem.Value.(*entry)
		c.list.Remove(delElem)
		delete(c.table, delValue.key)
	}
}
