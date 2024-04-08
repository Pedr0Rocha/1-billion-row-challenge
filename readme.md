# 1 Billion Row Challenge

## First attempt

Made it work. No optimizations, profiling, concurrency or anything.

- 220.42s user 7.13s system 102% cpu 3:41.08 total

Added tests and profiling to prepare for second attempt.

[Release](https://github.com/Pedr0Rocha/1-billion-row-challenge/releases/tag/v1.0)

[Code](https://github.com/Pedr0Rocha/1-billion-row-challenge/tree/v1.0)

## Second attempt

After adding pprof to the program, it is clear that scanning each line using
`bufio` `Scanner` is not ideal. It is spending ~80% of the time on the
`read` `syscall`.
