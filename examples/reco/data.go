package main

import (
	"archive/zip"
	"bufio"
	"encoding/gob"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/akualab/coap/store"
)

type Obs struct {
	User, Item, Rating int
}

// Write data to store
func writeData(fn string) (dbTrain, dbTest string) {

	gob.Register(Obs{})

	// Open a zip archive for reading.
	r, err := zip.OpenReader(fn)
	if err != nil {
		log.Fatalf("can't open zip file %s - error: %s", fn, err)
	}
	defer r.Close()

	// Iterate through the files in the archive,
	for _, f := range r.File {
		fn := path.Base(f.Name)
		if fn == TrainFile {
			log.Printf("Found %s\n", f.Name)
			dbTrain = writeStore(f)
		}
		if fn == TestFile {
			log.Printf("Found %s\n", f.Name)
			dbTest = writeStore(f)
		}
	}
	return
}

func writeStore(f *zip.File) (dbName string) {

	rc, e := f.Open()
	if e != nil {
		log.Fatal(e)
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
	name := path.Base(f.Name)
	dbName = path.Join(OutDir, name)
	log.Printf("creating store %s", dbName)
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
		log.Fatalf("Invalid input: %s", err)
	}
	log.Printf("wrote %d records", key)
	return dbName
}

// Write chunks of data in small files.
func writeChunks(fn string) {

	// Create data dir.
	dir := path.Join(OutDir, "data")
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("create data dir %s\n", dir)
	// Open a zip archive for reading.
	r, err := zip.OpenReader(fn)
	if err != nil {
		log.Fatalf("can't open zip file %s - error: %s", fn, err)
	}
	defer r.Close()

	// Iterate through the files in the archive,
	for _, f := range r.File {
		fn := path.Base(f.Name)
		if fn == TrainFile || fn == TestFile {
			log.Printf("Found %s\n", f.Name)
			dd := path.Join(dir, path.Dir(f.Name))
			err := os.MkdirAll(dd, 0777)
			if err != nil {
				log.Fatal(err)
			}
			writeFiles(f, dir, ChunkLength)
		}
	}
}

// Split the big file into smaller files.
func writeFiles(f *zip.File, dir string, ChunkLength int) {

	rc, e := f.Open()
	if e != nil {
		log.Fatal(e)
	}
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
	k := 0
	count := 0
	var cf *os.File
	var err error
	for scanner.Scan() {
		if k%ChunkLength == 0 {
			if cf != nil {
				cf.Close()
			}
			fileName := path.Join(dir, f.Name+"."+strconv.Itoa(k))
			cf, err = os.Create(fileName)
			if err != nil {
				log.Fatal(err)
			}
			count++
		}
		cf.WriteString(scanner.Text() + "\n")
		k++
	}
	cf.Close()
	if err = scanner.Err(); err != nil {
		log.Fatalf("Invalid input: %s", err)
	}
	rc.Close()
	log.Printf("Wrote %d files.", count)
}

// If data not available, download.
func downloadData() string {

	fn, err := downloadFromUrl(DataURL, OutDir)
	if err != nil {
		log.Fatal(err)
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
		log.Printf("found data file: %s", fileName)
		return fileName, nil
	}

	// Otherwise create file and download.
	log.Println("Downloading", url, "to", fileName)
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

	log.Println(n, "bytes downloaded.")
	return fileName, nil
}

type obs struct {
	user, item, rating int
}

func observation(fn string) []obs {

	var e error
	f, err := os.Open(fn)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	obsSlice := make([]obs, 0, ChunkLength)
	scanner := bufio.NewScanner(f)
	k := 0
	for scanner.Scan() {
		newObs := obs{}
		fields := strings.Fields(scanner.Text())
		newObs.user, e = strconv.Atoi(fields[0])
		fatalIf(e)
		newObs.item, e = strconv.Atoi(fields[1])
		fatalIf(e)
		newObs.rating, e = strconv.Atoi(fields[2])
		fatalIf(e)
		obsSlice = append(obsSlice, newObs)
		k++
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	log.Printf("read file %s", fn)
	return obsSlice
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
