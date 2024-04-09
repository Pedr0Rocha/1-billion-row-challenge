# 1 Billion Row Challenge

## Attempt #1

First working version. No optimizations, concurrency or anything fancy.

Added tests and profiling to prepare for second attempt.

### Results

- 220.42s

### Code state

[Release](https://github.com/Pedr0Rocha/1-billion-row-challenge/releases/tag/v1.0)

[Code](https://github.com/Pedr0Rocha/1-billion-row-challenge/tree/v1.0)

## Attempt #2

After adding pprof to the program, it is clear that scanning **each line** using
`bufio` `Scanner` is not ideal. Pprof shows that we are spending ~80% of the time on the `read` `syscall`.

To solve this, we can try ingesting more rows per read by increasing the buffer size and
drastically reduce the number of `read` calls. Assuming that we have an avg. of 16 bytes per row, setting
the buffer size to `16 * 1024 * 1024` should be enough to read ~1M rows at once.

However, this improvement introduced a new issue. Incomplete rows for each chunk read, refered as "leftover"
from now on.

Since we are not reading each line anymore, we don't know where our buffer will end up
at when reading the main file. So the following situation happens very often:

**Input:** `42 bytes`

```bash
Juruá;95.7
Popunhas;-99.7
Imperatriz;50.4
```

**Buffer size:** `32 bytes`

```bash
Juruá;95.7
Popunhas;-99.7
Imper
```

**Leftovers**

```bash
Imper
```

To solve this issue, we can either keep reading the file until we find a new line or EOF, or
backtrack the current buffer to the last new line. This attempt implements the later. Continuing the example
above:

The last `\n` found is here: `Popunhas;-99.7`

```bash
Juruá;95.7
Popunhas;-99.7 <--HERE---
Imper
```

So the processed chunk will be:

```bash
Juruá;95.7
Popunhas;-99.7
```

In the next read, we have `atriz;50.4`. We will append it to the leftover from the last iteration and get:

Leftover + next read = new current buffer

`Imper` + `atriz;50.4` = `Imperatriz;50.4`

_note: using this strategy, the buffer chunk size can never be less than a complete row. Otherwise
it gets stuck trying to backtrack to the last new line._

### Results

Previous best was 220.42s

- 173.05s

### Code state

[Release](https://github.com/Pedr0Rocha/1-billion-row-challenge/releases/tag/v2.0)

[Code](https://github.com/Pedr0Rocha/1-billion-row-challenge/tree/v2.0)

## Attempt #3

The `read` `syscall` is now gone from the flamegraph, revealing the new performance killers,
`ParseFloat` from our measurement and `WriteByte` from our parser.

To improve it, we are going to use integers instead of float and only convert it to float
as the last operation to print it out. As for the `WriteByte`, we can write our own parser
for the resulting buffer and try to optimize it.

General improvements were also implemented as they would show up in pprof, such as unnecessary
assignments of `stationData` and bytes to string convertion.

Turns out that at this level, `string(myBytes)` costs a lot. But after some research, we can
implement a more efficient way of doing it.

```go
func bytesToString(b []byte) string {
	// gets the pointer to the underlying array of the slice
	pointerToArray := unsafe.SliceData(b)
	// returns the string of length len(b) of the bytes in the pointer
	return unsafe.String(pointerToArray, len(b))
}
```

After all those improvements, we have a clear direction on where to improve next.

- We stopped converting to float, but we still convert to int, which is taking some time.

```bash
3.57s  measurementInt, _ := strconv.Atoi(bytesToString(measurement))
```

- Parsing the `resultBuffer` is too expensive, specially when trying to skip the `'.'` to make
  our conversion to int easy.

```bash
21.03s  newBuffer, stationName, measurement := parseBufferSingle(resultBuffer)
```

- This was a very unpleasant surprise, the map look up is extremly slow.

```bash
38.53s  data, exist := stations[station]
```

And of course, we are still waiting for the resultBuffer to be parsed to continue reading from
the file. This can be improved by parsing pieces of the buffer at the same time using go routines
and merge the result somewhere.
But I'll try to improve the linear solution as much as possible before paralelizing the workload as
it will become more complex.

### Results

- 77.08s

_Previous best time was 173.05s._

### Code state

[Release](https://github.com/Pedr0Rocha/1-billion-row-challenge/releases/tag/v3.0)

[Code](https://github.com/Pedr0Rocha/1-billion-row-challenge/tree/v3.0)
