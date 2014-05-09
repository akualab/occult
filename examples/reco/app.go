package main

import (
	"fmt"
	"log"
	"time"

	"github.com/akualab/coap"
	"github.com/akualab/coap/store"
)

type Options struct {
	db         *store.Store
	chunkSize  int
	numWorkers int
}

// gets data from DB
// returns chunks of observations in a slice.
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

// returns histogram of ratings for chunk
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

// returns histogram of ratings for all the data
func aggRatingsFunc(idx uint64, ctx *coap.Context) (coap.Value, error) {

	var err error
	var in interface{}
	if idx > 0 {
		return nil, coap.ErrEndOfArray
	}
	dist := make([]int, 5, 5)
	var j uint64
	for j = 0; ; j++ {
		in, err = ctx.Inputs()[0](j)
		if err != nil && err != coap.ErrEndOfArray {
			return nil, err // something is wrong
		}
		if err == coap.ErrEndOfArray {
			return dist, nil
		}
		s := in.([]int)
		for k, v := range s {
			dist[k] += v
		}
	}
}

// returns histogram of ratings for all the data computed with concurrency
func concurrentRatingsFunc(idx uint64, ctx *coap.Context) (coap.Value, error) {
	opt := ctx.Options.(*Options)
	if idx > 0 {
		return nil, coap.ErrEndOfArray
	}
	dist := make([]int, 5, 5)
	ch := ctx.Inputs()[0].ChanAll(0, opt.numWorkers)
	for {
		v, ok := <-ch
		if !ok {
			return dist, nil
		}
		s := v.([]int)
		for k, v := range s {
			dist[k] += v
		}
	}
}

// Computes various global statistics on the data set.
func statsFunc(idx uint64, ctx *coap.Context) (coap.Value, error) {

	in, err := ctx.Inputs()[0](idx)
	if err != nil && err != coap.ErrEndOfArray {
		return nil, err // something is wrong
	}
	if in == nil {
		return nil, coap.ErrEndOfArray
	}
	s := in.([]Obs)
	stats := NewStats()
	for _, v := range s {
		r := v.Rating
		if r < 1 || r > 5 {
			return nil, fmt.Errorf("rating out of range: %d", r)
		}
		stats.Update(v.User, v.Item, v.Rating)
	}
	return stats, err // err may be ErrEndOfArray
}

// aggregate stats
func aggStatsFunc(idx uint64, ctx *coap.Context) (coap.Value, error) {
	opt := ctx.Options.(*Options)
	if idx > 0 {
		return nil, coap.ErrEndOfArray
	}
	stats := NewStats()
	ch := ctx.Inputs()[0].ChanAll(0, opt.numWorkers)
	for {
		v, ok := <-ch
		if !ok {
			return stats, nil
		}
		q := v.(*Stats)
		stats.Add(q)
	}
}

// the app
func createApp(dbName string, chunkSize int) {

	var db *store.Store
	var err error

	db, err = store.NewStore(dbName)
	fatalIf(err)
	defer db.Close()

	opt := &Options{
		db:         db,
		chunkSize:  chunkSize,
		numWorkers: 2,
	}

	app := coap.NewApp(dbName)
	dataChunk := app.AddSource(movieFunc, opt, nil)
	//ratingDist := app.Add(ratingDistFunc, opt, dataChunk)
	//aggRatings := app.Add(aggRatingsFunc, opt, ratingDist)
	//aggRatings := app.Add(concurrentRatingsFunc, opt, ratingDist)

	stats := app.Add(statsFunc, opt, dataChunk)
	aggStats := app.Add(aggStatsFunc, opt, stats)

	// start := time.Now()
	// v, e := aggRatings(0)
	// if e != nil {
	// 	log.Fatal(e)
	// }
	// end := time.Now()
	// d := end.Sub(start)

	//log.Printf("Ratings distribution {1..5}: %v", v)
	//log.Printf("duration: %v", d)

	start := time.Now()
	x, ex := aggStats(0)
	if ex != nil {
		log.Fatal(ex)
	}
	s := x.(*Stats)
	end := time.Now()
	d := end.Sub(start)
	log.Printf("num_ratings:%d, num_users:%d, num_items:%d",
		s.SumAll(),
		len(s.NumRatingsByUser),
		len(s.NumRatingsByItem))
	log.Printf("compute aggStats, duration: %v", d)

	// log.Printf("Stats: %v", x)
	// s := x.(*Stats)
	// for k, v := range s.NumRatingsByItem {
	// 	log.Printf("I:%5d, r:%v", k, v)
	// }
	// for k, v := range s.NumRatingsByUser {
	// 	log.Printf("U:%5d, r:%v", k, v)
	// }

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
