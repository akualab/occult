package asap

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// Define a trivial processor as a data source. The undelying data is a local slice of ints.
type intSource struct {
	s []int
}

func NewIntSource(n int) *intSource {
	return &intSource{(getRandomInts(n))}
}

// Returns a Slice. Implements required VSlice method.
func (is *intSource) Get(start, end uint64) (*Slice, error) {
	return NewSlice(start, end, intSource{is.s[start:end]}), nil
}

// Sorter takes a slice of ints and sorts them.
type Sorter struct {
	*BaseProcessor
}

func sortProc(start, end uint64, in ...Processor) (*Slice, error) {

	// Copy the data so we can sort in place.
	l := end - start
	s := make([]int, l, l)
	copy(s, in[0].(*intSource).s[start:end])

	// Sort the data.
	sort.Ints(s)
	return NewSlice(start, end, s), nil
}

// Decile takes a sorted slice of ints and returns a slice with the deciles (nine values).
type Decile struct {
	*BaseProcessor
}

func decileProc(start, end uint64, in ...Processor) (*Slice, error) {

	// Allocate a slice for the results
	l := end - start
	bin := int(l / 10)
	s := make([]int, 9, 9)
	sl, err := in[0].Get(start, end)
	if err != nil {
		return nil, err
	}
	input := sl.Data
	for k := 0; k < 9; k++ {
		s[k] = input.([]int)[bin*(k+1)]
	}
	return NewSlice(0, 1, s), nil
}

// Window returns a slice of ints.
type Window struct {
	step, length int
	*BaseProcessor
}

func NewWindow(step, length int) *Window {
	return &Window{step: step, length: length}
}

func windowProc(start, end uint64, in ...Processor) (*Slice, error) {

	// sl, err := in[0].Get(start, end)
	// if err != nil {
	// 	return nil, err
	// }
	// input := sl.Data
	//	inLen := len(input)

	// calculate number of outpur slices.

	return NewSlice(start, end, nil), nil
}

func TestASAPSort(t *testing.T) {

	// Create an int source.
	is := NewIntSource(100)
	t.Logf("intSource: %v", is)
	// Create a sorter.
	sorter := Sorter{&BaseProcessor{Inputs: []Processor{is}, Process: sortProc}}
	// Create a decile processor.
	decile := Decile{&BaseProcessor{Inputs: []Processor{sorter}, Process: decileProc}}
	// Finally, create app. (Initializes all the processors.)
	app := NewApp("sort test", is, sorter, decile)
	t.Logf("created app: %s", app.Name)
	t.Logf("intSource ID: %d", app.ID(is))
	t.Logf("sorter ID: %d", app.ID(sorter))
	t.Logf("decile ID: %d", app.ID(decile))

	// Sort the slice.
	sortedSlice, err := sorter.Get(0, uint64(len(is.s)))
	FatalIf(t, err)
	t.Logf("sorted: %v", sortedSlice)
	if !isOrdered([]int(sortedSlice.Data.([]int))) {
		t.Fatal("not in order")
	}

	// Sort a subset of the slice.
	sortedSlice, err = sorter.Get(2, 8)
	FatalIf(t, err)
	t.Logf("sorted: %v", sortedSlice)
	if !isOrdered([]int(sortedSlice.Data.([]int))) {
		t.Fatal("not in order")
	}

	// Compute deciles.
	decileSlice, e2 := decile.Get(0, uint64(len(is.s)))
	FatalIf(t, e2)
	t.Logf("deciles: %v", decileSlice)
}

func TestDeciles(t *testing.T) {

	// Create an int source.
	is := NewIntSource(100)
	// Create a sorter.
	sorter := Sorter{&BaseProcessor{Inputs: []Processor{is}, Process: sortProc}}
	// Create a decile processor.
	decile := Decile{&BaseProcessor{Inputs: []Processor{sorter}, Process: decileProc}}
	// Finally, create app. (Initializes all the processors.)
	app := NewApp("sort test", is, sorter, decile)
	t.Logf("created app: %s", app.Name)
	t.Logf("intSource ID: %d", app.ID(is))
	t.Logf("sorter ID: %d", app.ID(sorter))
	t.Logf("decile ID: %d", app.ID(decile))

	// Compute deciles.
	decileSlice, err := decile.Get(0, uint64(len(is.s)))
	FatalIf(t, err)
	t.Logf("deciles: %v", decileSlice)
}

func getRandomInts(n int) []int {

	rand.Seed(42)
	s := make([]int, n, n)
	for k, _ := range s {
		s[k] = rand.Intn(1000)
	}
	return s
}

func isOrdered(s []int) bool {
	for i := 0; i < len(s)-1; i++ {
		if s[i] > s[i+1] {
			return false
		}
	}
	return true
}

/* Test Helpers */
func expect(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Errorf("Expected %v (type %v) - Got %v (type %v)", b, reflect.TypeOf(b), a, reflect.TypeOf(a))
	}
}

func refute(t *testing.T, a interface{}, b interface{}) {
	if a == b {
		t.Errorf("Did not expect %v (type %v) - Got %v (type %v)", b, reflect.TypeOf(b), a, reflect.TypeOf(a))
	}
}

func FatalIf(t *testing.T, err error) {

	if err != nil {
		t.Fatal(err)
	}
}
