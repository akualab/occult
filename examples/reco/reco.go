// Copyright (c) 2014 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Implement various collaborative filtering algorithms [1] and evaluate using the
// movie lense data set [2]
// [1] http://www.stanford.edu/~lmackey/papers/cf_slides-pml09.pdf
// [2] http://grouplens.org/datasets/movielens/
// See README.md for details.
package main

import "log"

const (
	// movie lense data set http://grouplens.org/datasets/movielens/
	DataURL = "http://www.grouplens.org/system/files/ml-100k.zip"

	OutDir    = "out"
	TrainFile = "u1.base"
	TestFile  = "u1.test"
	ChunkSize = 200
)

func main() {

	// donloads movielens data
	fn := downloadData()

	// writes train and test data as small data files with ChunkLength lines.
	dbTrain, dbTest := writeData(fn)
	log.Printf("train: %s, test: %s", dbTrain, dbTest)
	cf := TrainCF(dbTrain, ChunkSize)
	EvalCF(dbTest, cf)
}
