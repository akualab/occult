// Copyright (c) 2014 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Adapted from:
// https://code.google.com/p/vitess/source/browse/go/cache/lru_cache_test.go

// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package occult

import (
	"testing"
)

type cacheValue struct {
	x int
}

func TestInitialState(t *testing.T) {
	cache := newCache(5)
	l, c, _ := cache.stats()
	if l != 0 {
		t.Errorf("length = %v, want 0", l)
	}
	if c != 5 {
		t.Errorf("capacity = %v, want 5", c)
	}
}

func TestSetInsertsValue(t *testing.T) {
	cache := newCache(100)
	data := &cacheValue{3}
	key := uint64(100)
	cache.set(key, data)

	v, ok := cache.get(key)
	if !ok || v.(*cacheValue) != data {
		t.Errorf("Cache has incorrect value: %v != %v", data, v)
	}
}

func TestGetValueWithMultipleTypes(t *testing.T) {
	cache := newCache(100)
	data := &cacheValue{3}
	key := uint64(100)
	cache.set(key, data)

	v, ok := cache.get(uint64(100))
	if !ok || v.(*cacheValue) != data {
		t.Errorf("Cache has incorrect value for \"key\": %v != %v", data, v)
	}
}

func TestSetWithOldKeyUpdatesValue(t *testing.T) {
	cache := newCache(100)
	emptyValue := &cacheValue{3}
	key := uint64(101)
	cache.set(key, emptyValue)
	someValue := &cacheValue{20}
	cache.set(key, someValue)

	v, ok := cache.get(key)
	if !ok || v.(*cacheValue) != someValue {
		t.Errorf("Cache has incorrect value: %v != %v", someValue, v)
	}
}

func TestGetNonExistent(t *testing.T) {
	cache := newCache(100)

	if _, ok := cache.get(uint64(333)); ok {
		t.Error("Cache returned a crap value after no inserts.")
	}
}

func TestDelete(t *testing.T) {
	cache := newCache(100)
	value := &cacheValue{1}
	key := uint64(101)

	if cache.delete(key) {
		t.Error("Item unexpectedly already in cache.")
	}

	cache.set(key, value)

	if !cache.delete(key) {
		t.Error("Expected item to be in cache.")
	}

	if l, _, _ := cache.stats(); l != 0 {
		t.Errorf("length = %v, expected 0", l)
	}

	if _, ok := cache.get(key); ok {
		t.Error("Cache returned a value after deletion.")
	}
}

func TestClear(t *testing.T) {
	cache := newCache(100)
	value := &cacheValue{1}
	key := uint64(100)

	cache.set(key, value)
	cache.clear()

	if l, _, _ := cache.stats(); l != 0 {
		t.Errorf("length = %v, expected 0 after clear()", l)
	}
}

func TestCapacityIsObeyed(t *testing.T) {
	size := uint64(3)
	cache := newCache(size)
	value := &cacheValue{1}

	// Insert up to the cache's capacity.
	cache.set(uint64(101), value)
	cache.set(uint64(102), value)
	cache.set(uint64(103), value)
	if l, _, _ := cache.stats(); l != size {
		t.Errorf("cache length = %v, expected %v", l, size)
	}
	// Insert one more; something should be evicted to make room.
	cache.set(uint64(104), value)
	if l, _, _ := cache.stats(); l != size {
		t.Errorf("post-evict cache length = %v, expected %v", l, size)
	}
}

func TestLRUIsEvicted(t *testing.T) {
	size := uint64(3)
	cache := newCache(size)
	value := &cacheValue{1}

	cache.set(uint64(101), value)
	cache.set(uint64(102), value)
	cache.set(uint64(103), value)
	// lru: [103, 102, 101]

	// Look up the elements. This will rearrange the LRU ordering.
	cache.get(uint64(103))
	cache.get(uint64(102))
	cache.get(uint64(101))
	// lru: [101, 102, 103]

	cache.set(uint64(100), &cacheValue{1})
	// lru: [100, 101, 102]

	// The least recently used one should have been evicted.
	if _, ok := cache.get(uint64(103)); ok {
		t.Error("Least recently used element was not evicted.")
	}
}
