package occult

import (
	"sync/atomic"
	"time"
)

type stats struct {
	numRequests  uint64
	numCacheHits uint64
	start        time.Time
}

func newStats() *stats {
	s := new(stats)
	s.start = time.Now()
	return s
}

func (s *stats) addRequest() {
	atomic.AddUint64(&s.numRequests, 1)
}

func (s *stats) addCacheHit() {
	atomic.AddUint64(&s.numCacheHits, 1)
}

func startStats() {

}
