# Example Collaborative filtering algorithms

* Algorithms: http://www.stanford.edu/~lmackey/papers/cf_slides-pml09.pdf
* Data: http://grouplens.org/datasets/movielens/

# Recommendation Algorithm Example

## Compare concurrent computation.

The Processor method ChanAll() executes teh process using n workers.

Task: ratings distribution

```
Host: MacBook Air

Concurrent   Num Procs     Num Workers    Time
No           2             NA             1.85 s
Yes          2             2              1.20 s (35% speedup)
```
