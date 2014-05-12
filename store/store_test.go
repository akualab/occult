// Copyright (c) 2014 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package store

import (
	"encoding/gob"
	"testing"
)

func TestStoreInt(t *testing.T) {

	db, err := NewStore("/tmp/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.PutInt32(uint64(222), 55)
	if err != nil {
		t.Fatal(err)
	}

	var v int32
	v, err = db.GetInt32(uint64(222))
	if err != nil {
		t.Fatal(err)
	}
	if v != 55 {
		t.Fatalf("expected 55, got %d", v)
	}

	// key not in db
	v, err = db.GetInt32(uint64(776688))
	if err == nil {
		t.Fatalf("expected err not nil, got nil")
	}
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrNoKey got %+v", v)
	}
}

type T struct {
	A, B int
}

func TestStoreStruct(t *testing.T) {

	gob.Register(T{})
	var x interface{} = T{66, 77}
	db, err := NewStore("/tmp/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Put(uint64(111), &x)
	if err != nil {
		t.Fatal(err)
	}
	var y interface{}
	y, err = db.Get(uint64(111))
	if err != nil {
		t.Fatal(err)
	}
	v := y.(T)
	if v.A != 66 && v.B != 77 {
		t.Fatalf("expected 66/77, got %v", v)
	}
}
