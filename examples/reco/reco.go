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

import (
	"flag"
	"log"

	"github.com/akualab/occult"
)

const (
	// movie lense data set http://grouplens.org/datasets/movielens/
	DataURL = "http://www.grouplens.org/system/files/ml-100k.zip"

	OutDir    = "out"
	TrainFile = "u1.base"
	TestFile  = "u1.test"
	ChunkSize = 200
)

var isServer bool
var nodeID int

func init() {
	flag.IntVar(&nodeID, "node", 0, "the node id for this process")
	flag.BoolVar(&isServer, "server", false, "runs in server mode")
}
func main() {

	flag.Parse()

	// donloads movielens data
	fn := downloadData()

	// writes train and test data as small data files with ChunkLength lines.
	dbTrain, dbTest := writeData(fn, nodeID)
	log.Printf("train: %s, test: %s", dbTrain, dbTest)

	config, err := occult.ReadConfig("config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Config:\n%s", config)

	// TODO: Need a way hide this from app.
	config.Cluster.NodeID = nodeID
	config.App.SetServer(isServer)

	// Run trainer on multiple nodes.
	cf := TrainCF(dbTrain, config, ChunkSize)

	// Run the evaluation on a single node.
	EvalCF(dbTest, occult.OneNodeConfig(), cf)
}
