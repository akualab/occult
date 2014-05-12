// Copyright (c) 2014 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package occult

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

type Options struct {
	intSlice []int
	winSize  int
	quant    int
	step     int
}

func randomFunc(idx uint64, ctx *Context) (Value, error) {
	opt := ctx.Options.(*Options)
	if idx >= uint64(len(opt.intSlice)) {
		return nil, ErrEndOfArray
	}
	return opt.intSlice[idx], nil
}

func windowFunc(idx uint64, ctx *Context) (Value, error) {
	opt := ctx.Options.(*Options)
	win := uint64(opt.winSize)
	step := uint64(opt.step)
	in := ctx.inputs[0]
	out := make([]int, win, win)
	k := 0
	for i := idx * step; i < idx*step+win; i++ {
		v, err := in(i)
		if err != nil {
			return nil, err
		}
		out[k] = v.(int)
		k++
	}
	return out, nil
}

func sortFunc(idx uint64, ctx *Context) (Value, error) {
	in := ctx.inputs[0]
	v, err := in(idx)
	if err != nil {
		return nil, err
	}
	s := v.([]int)
	out := make([]int, len(s), len(s))
	copy(out, s)
	sort.Ints(out)
	return out, nil
}

func quantileFunc(idx uint64, ctx *Context) (Value, error) {
	opt := ctx.Options.(*Options)
	q := opt.quant
	in := ctx.inputs[0]
	v, err := in(idx)
	if err != nil {
		return nil, err
	}
	s := v.([]int)
	bin := len(s) / q
	out := make([]int, q-1, q-1)
	for k := 0; k < q-1; k++ {
		out[k] = s[bin*(k+1)]
	}

	return out, nil
}

func TestQuantiles(t *testing.T) {

	n := 10000
	opt := &Options{
		intSlice: getRandomInts(n),
		winSize:  100,
		quant:    4,
		step:     30,
	}

	app := NewApp("test")
	randomInts := app.AddSource(randomFunc, opt, nil)
	window := app.Add(windowFunc, opt, randomInts)
	sorted := app.Add(sortFunc, opt, window)
	quantile := app.Add(quantileFunc, opt, sorted)

	var i uint64
	for {
		v, e := quantile(i)
		if e == ErrEndOfArray {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		t.Logf("quantile[%3d]: %v", i, v)
		i++
	}

	// test Map
	values, err := sorted.Map(100, 103)
	FatalIf(t, err)
	t.Logf("slice values: %#v", values)
}

func TestChannels(t *testing.T) {

	opt := &Options{
		intSlice: getRandomInts(1000),
		winSize:  100,
		quant:    4,
	}
	app := NewApp("test")
	randomInts := app.AddSource(randomFunc, opt, nil)
	ch := randomInts.MapAllN(0, 5)
	for {
		v, ok := <-ch
		if !ok {
			return
		}
		t.Logf("final got: %#v", v)
	}
}

func getRandomInts(n int) []int {

	rand.Seed(42)
	s := make([]int, n, n)
	for k, _ := range s {
		s[k] = rand.Intn(1000)
	}
	return s
}

func isOrdered(s []int) bool {
	for i := 0; i < len(s)-1; i++ {
		if s[i] > s[i+1] {
			return false
		}
	}
	return true
}

/* Test Helpers */
func expect(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Errorf("Expected %v (type %v) - Got %v (type %v)", b, reflect.TypeOf(b), a, reflect.TypeOf(a))
	}
}

func refute(t *testing.T, a interface{}, b interface{}) {
	if a == b {
		t.Errorf("Did not expect %v (type %v) - Got %v (type %v)", b, reflect.TypeOf(b), a, reflect.TypeOf(a))
	}
}

func FatalIf(t *testing.T, err error) {

	if err != nil {
		t.Fatal(err)
	}
}
