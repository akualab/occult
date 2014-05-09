# Recommendation Algorithm Example

## Compare concurrent computation.

The Processor method ChanAll() executes teh process using n workers.

Task: ratings distribution

```
Concurrent   Num Procs     Num Workers    Time
No           2             NA             1.85 s
Yes          2             2              1.20 s (35% speedup)
```
