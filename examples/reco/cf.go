// Copyright (c) 2014 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"math/rand"
)

// A data type for 5-star rating systems.
type Ratings []int

func NewRatings() Ratings {
	return make(Ratings, 5, 5)
}

func (r Ratings) Add(s Ratings) {
	for k, _ := range r {
		r[k] += s[k]
	}
}

func (r Ratings) Count() int {

	cnt := 0
	for _, v := range r {
		cnt += v
	}
	return cnt
}

func (r Ratings) Sum() int {

	sum := 0
	for _, v := range r {
		sum += v
	}
	return sum

}

func (r Ratings) Mean() float64 {

	sum := 0
	cnt := 0
	for k, v := range r {
		sum += (k + 1) * v
		cnt += v
	}
	return float64(sum) / float64(cnt)
}

// A data type for Collaborative Filtering.
type CF struct {
	NumFactors, NumUsers, NumItems int
	LRate, Reg                     float64
	MeanNorm                       bool
	// counts occurrences of each rating {1..5}
	NumRatings Ratings
	// num ratings by user
	NumRatingsByUser map[int]Ratings
	// num ratings by item
	NumRatingsByItem map[int]Ratings
	// user factors
	UserFactors map[int]Factor
	// item factors
	ItemFactors map[int]Factor
	// random number generator
	r *rand.Rand
	// to compute weighted means (and bias)
	alpha float64
}

func NewCF(alpha float64) *CF {
	return &CF{
		NumRatings:       NewRatings(),
		NumRatingsByUser: make(map[int]Ratings),
		NumRatingsByItem: make(map[int]Ratings),
		UserFactors:      make(map[int]Factor),
		ItemFactors:      make(map[int]Factor),
		r:                rand.New(rand.NewSource(6555)),
		alpha:            alpha,
	}
}

// Initialize MF data structires before start training
// and after data set statistics have been collected.
func (cf *CF) InitMF(numFactors int, lrate, reg float64, meanNorm bool) {
	cf.NumFactors = numFactors
	cf.NumUsers = len(cf.NumRatingsByUser)
	cf.NumItems = len(cf.NumRatingsByItem)
	cf.LRate = lrate
	cf.Reg = reg
	cf.MeanNorm = meanNorm
}

func (s *CF) GlobalMean() float64 {

	return s.NumRatings.Mean()
}

func (s *CF) WeightedUserMean(u int) float64 {

	uMean := s.NumRatingsByUser[u].Mean()
	uCount := float64(s.NumRatingsByUser[u].Count())
	gMean := s.NumRatings.Mean()
	if uCount == 0 {
		return gMean
	}
	return (s.alpha*gMean + uCount*uMean) / (s.alpha + uCount)
}

func (s *CF) WeightedItemMean(i int) float64 {

	iMean := s.NumRatingsByItem[i].Mean()
	iCount := float64(s.NumRatingsByItem[i].Count())
	gMean := s.NumRatings.Mean()
	if iCount == 0 {
		return gMean
	}
	return (s.alpha*gMean + iCount*iMean) / (s.alpha + iCount)
}

func (s *CF) UserMean(u int) float64 {
	return s.NumRatingsByUser[u].Mean()
}

func (s *CF) ItemMean(i int) float64 {
	return s.NumRatingsByItem[i].Mean()
}

func (cf *CF) Bias(u, i int) float64 {

	return 3.0*cf.GlobalMean() - cf.WeightedUserMean(u) - cf.WeightedItemMean(i)
}

func (s *CF) SumAll() int {

	sum := 0
	for _, v := range s.NumRatings {
		sum += v
	}
	return sum
}

func (s *CF) SumForUser(u int) int {

	sum := 0
	for _, v := range s.NumRatingsByUser[u] {
		sum += v
	}
	return sum
}

func (s *CF) SumForItem(i int) int {

	sum := 0
	for _, v := range s.NumRatingsByItem[i] {
		sum += v
	}
	return sum
}

func (s *CF) Update(u, i, r int) {
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

// Merges result into s, destroys q.
func (s *CF) Reduce(q *CF) {

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

type Factor []float64

func NewFactor(f int) Factor {
	return make(Factor, f, f)
}

func (p Factor) Multiply(c float64) Factor {

	for k, _ := range p {
		p[k] = p[k] * c
	}
	return p
}

// Adds in place, overwrites p
func (p Factor) Add(q Factor) Factor {

	for k, _ := range p {
		p[k] += q[k]
	}
	return p
}

func (p Factor) DotProd(q Factor) float64 {

	var sum float64
	for k, _ := range p {
		sum += p[k] * q[k]
	}
	return sum
}

// Gradient descent update function. (in place)
// e[u,i] = r[u,i] - q[i]^T * p[u]
// q[i] <- q[i] + lrate * (e[u,i] * p[u] - reg * q[i])
// p[u] <- p[u] + lrate * (e[u,i] * q[i] - reg * p[u])
func (cf *CF) GDUpdate(user, item, rating int) {

	var p, q Factor
	var ok bool
	p, ok = cf.UserFactors[user]
	if !ok {
		cf.UserFactors[user] = randomFactor(cf.NumFactors, cf.r)
		p = cf.UserFactors[user]
	}
	q, ok = cf.ItemFactors[item]
	if !ok {
		cf.ItemFactors[item] = randomFactor(cf.NumFactors, cf.r)
		q = cf.ItemFactors[item]
	}

	// error
	var bias float64
	if cf.MeanNorm {
		bias = cf.Bias(user, item)
	}
	e := float64(rating) - bias - p.DotProd(q)

	// prepare
	c1 := 1 - cf.LRate*cf.Reg
	c2 := cf.LRate * e
	pp := NewFactor(cf.NumFactors)
	qq := NewFactor(cf.NumFactors)
	copy(pp, p)
	copy(qq, q)

	// TODO: if necessary cf could provide a slice pool to reuse pp,qq. (need to profile).

	// update q and p
	qq.Multiply(c1).Add(p.Multiply(c2))
	pp.Multiply(c1).Add(q.Multiply(c2))
	copy(p, pp)
	copy(q, qq)
}

func (cf *CF) MFPredict(user, item int) (float64, error) {

	var p, q Factor
	var ok bool
	p, ok = cf.UserFactors[user]
	if !ok {
		return 0, fmt.Errorf("no MF prediction for unseen user:, %d", user)
	}
	q, ok = cf.ItemFactors[item]
	if !ok {
		return 0, fmt.Errorf("no MF prediction for unseen item: %d", item)
	}

	var bias float64
	if cf.MeanNorm {
		bias = cf.Bias(user, item)
	}

	return p.DotProd(q) + bias, nil
}

func randomFactor(n int, r *rand.Rand) Factor {

	f := NewFactor(n)
	for i, _ := range f {
		f[i] = r.NormFloat64()
	}
	return f
}
