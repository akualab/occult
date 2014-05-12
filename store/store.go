// Copyright (c) 2014 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// A key-value store based on leveldb.
// Uses uint64 for keys.
package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"

	"github.com/jmhodges/levigo"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

type Store struct {
	db *levigo.DB
	ro *levigo.ReadOptions
	wo *levigo.WriteOptions
}

// Open or create a datastore using with the given filename.
func NewStore(fn string) (*Store, error) {

	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)

	db, err := levigo.Open(fn, opts)
	if err != nil {
		return nil, err
	}

	return &Store{
		db: db,
		ro: levigo.NewReadOptions(),
		wo: levigo.NewWriteOptions(),
	}, nil
}

func (store *Store) Close() {
	store.wo.Close()
	store.ro.Close()
	store.db.Close()
}

func (store *Store) GetInt32(key uint64) (int32, error) {

	kk, e := toBytes(key)
	if e != nil {
		return 0, e
	}
	v, err := store.db.Get(store.ro, kk)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, ErrKeyNotFound
	}
	return bytesToInt32(v)
}

func (store *Store) PutInt32(key uint64, value int32) error {

	var kk, vv []byte
	var err error

	kk, err = toBytes(key)
	if err != nil {
		return err
	}
	vv, err = toBytes(value)
	if err != nil {
		return err
	}
	err = store.db.Put(store.wo, kk, vv)
	if err != nil {
		return err
	}
	return nil
}

func (store *Store) Put(key uint64, value interface{}) error {

	kk, err := toBytes(key)
	if err != nil {
		return err
	}

	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err = enc.Encode(value)
	if err != nil {
		return err
	}
	err = store.db.Put(store.wo, kk, b.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (store *Store) Get(key uint64) (value interface{}, err error) {

	kk, err := toBytes(key)
	if err != nil {
		return
	}
	v, err := store.db.Get(store.ro, kk)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return 0, ErrKeyNotFound
	}
	r := bytes.NewReader(v)
	dec := gob.NewDecoder(r)
	err = dec.Decode(&value)
	if err != nil {
		return
	}
	return
}

func toBytes(v interface{}) ([]byte, error) {

	b := new(bytes.Buffer)
	err := binary.Write(b, binary.LittleEndian, v)
	return b.Bytes(), err
}

func bytesToInt32(b []byte) (int32, error) {

	var v int32
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.LittleEndian, &v)
	return v, err
}
