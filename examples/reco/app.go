package main

import (
	"fmt"
	"log"

	"github.com/akualab/coap"
	"github.com/akualab/coap/store"
)

type Options struct {
	db        *store.Store
	chunkSize int
}

func movieFunc(idx uint64, ctx *coap.Context) (coap.Value, error) {
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

func ratingDistFunc(idx uint64, ctx *coap.Context) (coap.Value, error) {
	in, err := ctx.Inputs()[0](idx)
	if err != nil && err != coap.ErrEndOfArray {
		return nil, err // something is wrong
	}
	if in == nil {
		return nil, coap.ErrEndOfArray
	}
	s := in.([]Obs)
	dist := make([]int, 5, 5)
	for _, v := range s {
		r := v.Rating
		if r < 1 || r > 5 {
			return nil, fmt.Errorf("rating out of range: %d", r)
		}
		dist[r-1] += 1
	}
	return dist, err // err may be ErrEndOfArray
}

func aggRatingsFunc(idx uint64, ctx *coap.Context) (coap.Value, error) {

	log.Printf("XXXXXXXXXXX AGG START idx:%d ctx:%#v", idx, ctx)
	dist := make([]int, 5, 5)
	var j uint64
	for j = 0; ; j++ {
		log.Printf("XXXXXXXXXXX AGG LOOP j:%d", j)
		in, err := ctx.Inputs()[0](j)
		if err != nil && err != coap.ErrEndOfArray {
			log.Printf("********** %v %v", nil, err)
			return nil, err // something is wrong
		}
		if err == coap.ErrEndOfArray {
			log.Printf("XXXXXXXXXXX %v %v", dist, err)
			return dist, err
		}
		s := in.([]int)
		for k, v := range s {
			dist[k] += v
		}
	}
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
	log.Printf("DEBUG: datachunk:  %#v", dataChunk)
	ratingDist := app.Add(ratingDistFunc, opt, dataChunk)
	log.Printf("DEBUG: ratingDist:  %#v", ratingDist)
	aggRatings := app.Add(aggRatingsFunc, opt, ratingDist)
	log.Printf("DEBUG: aggratings:  %#v", aggRatings)

	v, e := aggRatings(0)
	log.Printf(">>>>>>>> %#v %#v", v, e)

	// var i uint64
	// for {
	// 	v, e := aggRatings(i)
	// 	log.Printf("ZZZZZZZZZZ %v %v", v, e)
	// 	if e != nil && e != coap.ErrEndOfArray {
	// 		log.Fatal(e)
	// 	}

	// 	if v != nil {
	// 		log.Printf("chunk[%4d]: %v", i, v)
	// 	}
	// 	if e == coap.ErrEndOfArray {
	// 		log.Printf("end of array found at index %d", i)
	// 		break
	// 	}

	// 	i++
	// }
}
