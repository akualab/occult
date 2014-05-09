package main

type Ratings []int

func NewRatings() Ratings {
	return make(Ratings, 5, 5)
}

func (r Ratings) Add(s Ratings) {
	for k, _ := range r {
		r[k] += s[k]
	}
}

type Stats struct {
	// counts occurrences of each rating {1..5}
	NumRatings Ratings
	// num ratings by user
	NumRatingsByUser map[int]Ratings
	// num ratings by item
	NumRatingsByItem map[int]Ratings
}

func NewStats() *Stats {
	return &Stats{
		NumRatings:       NewRatings(),
		NumRatingsByUser: make(map[int]Ratings),
		NumRatingsByItem: make(map[int]Ratings),
	}
}

func (s *Stats) SumAll() int {

	sum := 0
	for _, v := range s.NumRatings {
		sum += v
	}
	return sum
}

func (s *Stats) SumForUser(u int) int {

	sum := 0
	for _, v := range s.NumRatingsByUser[u] {
		sum += v
	}
	return sum
}

func (s *Stats) SumForItem(i int) int {

	sum := 0
	for _, v := range s.NumRatingsByItem[i] {
		sum += v
	}
	return sum
}

func (s *Stats) Update(u, i, r int) {
	// update histogram counts
	s.NumRatings[r-1] += 1

	// update for item
	ri, ok := s.NumRatingsByItem[i]
	if !ok {
		ri = NewRatings()
		s.NumRatingsByItem[i] = ri
	}
	ri[r-1] += 1

	// update for user
	ru, ok := s.NumRatingsByUser[u]
	if !ok {
		ru = NewRatings()
		s.NumRatingsByUser[u] = ru
	}
	ru[r-1] += 1
}

func (s *Stats) Add(q *Stats) {

	s.NumRatings.Add(q.NumRatings)

	for i, v := range q.NumRatingsByItem {
		ri, ok := s.NumRatingsByItem[i]
		if !ok {
			ri = NewRatings()
			s.NumRatingsByItem[i] = ri
		}
		ri.Add(v)
	}
	for u, v := range q.NumRatingsByUser {
		ru, ok := s.NumRatingsByUser[u]
		if !ok {
			ru = NewRatings()
			s.NumRatingsByUser[u] = ru
		}
		ru.Add(v)
	}
}
