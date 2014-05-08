package main

import (
	"log"

	"github.com/akualab/coap"
	"github.com/akualab/coap/store"
)

type Options struct {
	db        *store.Store
	chunkSize int
}

func movieFunc(idx uint64, ctx *coap.Context, in ...coap.Processor) (coap.Value, error) {
	opt := ctx.Options.(*Options)
	db := opt.db
	n := uint64(opt.chunkSize)
	s := make([]Obs, 0, n)
	var base uint64 = idx * n
	var i uint64
	for ; i < n; i++ {
		v, err := db.Get(base + i)
		if err == store.ErrKeyNotFound {
			return s, coap.ErrEndOfArray
		}
		s = append(s, v.(Obs))
	}
	return s, nil
}

func createApp(dbName string, chunkSize int) {

	var db *store.Store
	var err error

	db, err = store.NewStore(dbName)
	fatalIf(err)
	defer db.Close()

	opt := &Options{
		db:        db,
		chunkSize: chunkSize,
	}

	app := coap.NewApp(dbName)
	dataChunk := app.AddSource(movieFunc, opt, nil)

	var i uint64
	for {
		v, e := dataChunk(i)
		if e == coap.ErrEndOfArray {
			break
		}
		if e != nil {
			log.Fatal(e)
		}
		log.Printf("chunk[%3d]: %v", i, v)
		i++
	}
}
