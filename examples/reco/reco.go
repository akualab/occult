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
	"os"

	"github.com/akualab/occult"
	"github.com/golang/glog"
)

const (
	// movie lense data set http://grouplens.org/datasets/movielens/
	DataURL = "http://www.grouplens.org/system/files/ml-100k.zip"

	OutDir     = "out"
	TrainFile  = "u1.base"
	TestFile   = "u1.test"
	ChunkSize  = 50
	SingleNode = "reco.yaml"
)

var isServer bool
var nodeID int
var configFile string

func init() {
	flag.IntVar(&nodeID, "node", 0, "the node id for this process")
	flag.BoolVar(&isServer, "server", false, "runs in server mode")
	flag.StringVar(&configFile, "config", SingleNode, "config file")
}

func main() {

	logDir := os.TempDir()
	flag.Parse()
	defer glog.Flush()

	// Check if flag log_dir is set and create dir just in case.
	// (Otherwise glog will ignore it.)
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "log_dir" {
			logDir = f.Value.String()
			err := os.MkdirAll(logDir, 0777)
			if err != nil {
				glog.Fatal(err)
			}
		}
	})
	glog.Infof("log_dir: %s", logDir)

	// donloads movielens data
	fn := downloadData()

	// writes train and test data as small data files with ChunkLength lines.
	dbTrain, dbTest := writeData(fn, nodeID)
	glog.Infof("train: %s, test: %s", dbTrain, dbTest)

	config, err := occult.ReadConfig(configFile)
	if err != nil {
		glog.Fatal(err)
	}
	glog.Infof("Config:\n%s", config)

	// TODO: Need a way hide this from app.
	if nodeID > 0 {
		config.Cluster.NodeID = nodeID
	}
	config.App.SetServer(isServer)

	// Run trainer on multiple nodes.
	cf := TrainCF(dbTrain, config, ChunkSize)

	// Run the evaluation on a single node.
	EvalCF(dbTest, occult.OneNodeConfig(), cf)
}
