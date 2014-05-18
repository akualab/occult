// Copyright (c) 2014 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"archive/zip"
	"bufio"
	"encoding/gob"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/akualab/occult/store"
	"github.com/golang/glog"
)

type Obs struct {
	User, Item, Rating int
}

// Write data to store
func writeData(fn string, nodeID int) (dbTrain, dbTest string) {

	gob.Register(Obs{})

	// Open a zip archive for reading.
	r, err := zip.OpenReader(fn)
	if err != nil {
		glog.Fatalf("can't open zip file %s - error: %s", fn, err)
	}
	defer r.Close()

	// Iterate through the files in the archive,
	for _, f := range r.File {
		fn := path.Base(f.Name)
		if fn == TrainFile {
			glog.Infof("Found %s\n", f.Name)
			dbTrain = writeStore(f, nodeID)
		}
		if fn == TestFile {
			glog.Infof("Found %s\n", f.Name)
			dbTest = writeStore(f, nodeID)
		}
	}
	return
}

func writeStore(f *zip.File, nodeID int) (dbName string) {

	rc, e := f.Open()
	if e != nil {
		glog.Fatal(e)
	}
	defer rc.Close()
	scanner := bufio.NewScanner(rc)
	// Create a custom split function by wrapping the existing ScanLines function.
	split := func(data []byte, atEOF bool) (advance int, line []byte, err error) {
		advance, line, err = bufio.ScanLines(data, atEOF)
		if err == nil && line != nil {
			// can validate here and return error.
		}
		return
	}
	// Set the split function for the scanning operation.
	scanner.Split(split)

	// create store
	name := path.Base(f.Name) + "-" + strconv.Itoa(nodeID)
	dbName = path.Join(OutDir, name)

	// Return if db exists.
	if _, err := os.Stat(dbName); err == nil {
		glog.Infof("db %s already exist, skipping...", dbName)
		return dbName
	}

	glog.Infof("creating store %s", dbName)
	db, err := store.NewStore(dbName)
	fatalIf(err)
	defer db.Close()

	var key uint64
	for scanner.Scan() {
		newObs := Obs{}
		fields := strings.Fields(scanner.Text())
		newObs.User, e = strconv.Atoi(fields[0])
		fatalIf(e)
		newObs.Item, e = strconv.Atoi(fields[1])
		fatalIf(e)
		newObs.Rating, e = strconv.Atoi(fields[2])
		fatalIf(e)
		var io interface{} = newObs
		fatalIf(db.Put(key, &io))
		key++
	}
	if err = scanner.Err(); err != nil {
		glog.Fatalf("Invalid input: %s", err)
	}
	glog.Infof("wrote %d records", key)
	return dbName
}

// If data not available, download.
func downloadData() string {

	fn, err := downloadFromUrl(DataURL, OutDir)
	if err != nil {
		glog.Fatal(err)
	}
	return fn
}

func downloadFromUrl(url, dir string) (string, error) {

	// Create dir.
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		return "", err
	}

	tokens := strings.Split(url, "/")
	fileName := path.Join(dir, tokens[len(tokens)-1])

	// Return if file exists.
	if _, err := os.Stat(fileName); err == nil {
		glog.Infof("found data file: %s", fileName)
		return fileName, nil
	}

	// Otherwise create file and download.
	glog.Infoln("Downloading", url, "to", fileName)
	output, err := os.Create(fileName)
	if err != nil {
		return "", nil
	}
	defer output.Close()

	response, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	n, err := io.Copy(output, response.Body)
	if err != nil {
		return "", err
	}

	glog.Infoln(n, "bytes downloaded.")
	return fileName, nil
}

func fatalIf(err error) {
	if err != nil {
		glog.Fatal(err)
	}
}
