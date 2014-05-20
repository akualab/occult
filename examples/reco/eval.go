// Copyright (c) 2014 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Evaluate various collaborative filtering algorithms using a test set.

import (
	"math"

	"github.com/akualab/occult"
	"github.com/akualab/occult/store"
	"github.com/golang/glog"
)

type SqErr struct {
	n                uint64
	globalMean       float64
	weightedUserMean float64
	weightedItemMean float64
	mf               float64
}

type EvalOptions struct {
	cf         *CF
	db         *store.Store
	globalMean float64
	sqErr      *SqErr
	alpha      float64
}

func EvalCF(dbTest string, config *occult.Config, cf *CF) {
	db, err := store.NewStore(dbTest)
	fatalIf(err)
	defer db.Close()

	opt := &EvalOptions{
		db:         db,
		cf:         cf,
		globalMean: cf.GlobalMean(),
		sqErr:      &SqErr{},
	}

	app := occult.NewApp(config)
	evalProc := app.AddSource(evalFunc, opt, nil)

	var i uint64
	for {
		v, e := evalProc(i)
		if e != nil && e != occult.ErrEndOfArray {
			glog.Fatal(e)
		}
		if v != nil {
			glog.V(5).Infof("chunk[%4d]: %v", i, v)
		}
		if e == occult.ErrEndOfArray {
			glog.V(3).Infof("end of array found at index %d", i)
			break
		}
		i++
	}

	n := float64(opt.sqErr.n)
	glog.Infof("N:%.0f, alpha:%.2f", n, cf.alpha)
	glog.Infof("%20s: %.4f", "Global Mean", math.Sqrt(opt.sqErr.globalMean/n))
	glog.Infof("%20s: %.4f", "Adj. User Mean", math.Sqrt(opt.sqErr.weightedUserMean/n))
	glog.Infof("%20s: %.4f", "Item Mean", math.Sqrt(opt.sqErr.weightedItemMean/n))
	glog.Infof("%20s: %.4f", "Simple MF", math.Sqrt(opt.sqErr.mf/n))
}

func evalFunc(idx uint64, ctx *occult.Context) (occult.Value, error) {
	opt := ctx.Options.(*EvalOptions)
	db := opt.db
	v, err := db.Get(idx)
	if err == store.ErrKeyNotFound {
		return nil, occult.ErrEndOfArray
	}
	obs := v.(Obs)
	glog.V(7).Infof("U:%d, I:%d, R:%d, Mean%.2f", obs.User, obs.Item, obs.Rating, opt.globalMean)
	opt.sqErr.n += 1

	// Global Mean
	diff := float64(obs.Rating) - opt.globalMean
	opt.sqErr.globalMean += diff * diff

	// Weighted User Mean
	diff = float64(obs.Rating) - opt.cf.WeightedUserMean(obs.User)
	opt.sqErr.weightedUserMean += diff * diff

	// Weighted Item Mean
	diff = float64(obs.Rating) - opt.cf.WeightedItemMean(obs.Item)
	opt.sqErr.weightedItemMean += diff * diff

	// Matrix Factorization
	rhat, e := opt.cf.MFPredict(obs.User, obs.Item)
	if e != nil {
		// backoff to another prediction
		rhat = opt.cf.WeightedItemMean(obs.Item)
		glog.V(2).Infof("backing off prediction, %s", e)
	}
	diff = float64(obs.Rating) - rhat
	opt.sqErr.mf += diff * diff

	return nil, nil
}
