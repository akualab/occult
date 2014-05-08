// Copyright (c) 2014 AKUALAB INC., All rights reserved.

// Example of a recommender system using matrix factorization.
package main

import "log"

const (
	// movie lense data set http://grouplens.org/datasets/movielens/
	DataURL = "http://www.grouplens.org/system/files/ml-100k.zip"

	OutDir      = "out"
	TrainFile   = "u1.base"
	TestFile    = "u1.test"
	ChunkLength = 100
)

func main() {

	// donloads movielens data
	fn := downloadData()

	// writes train and test data as small data files with ChunkLength lines.
	dbTrain, dbTest := writeData(fn)
	log.Printf("train: %s, test: %s", dbTrain, dbTest)
}
