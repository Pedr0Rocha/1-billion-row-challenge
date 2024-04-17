# 1 Billion Row Challenge

The One Billion Row Challenge -- A fun exploration of how quickly 1B rows from a text file can be
aggregated with ~~Java~~ Go

[Link to the original challenge](https://github.com/gunnarmorling/1brc)

## Baseline measurement

Generate the measurements from the original challenge and make sure you have a 1 billion row file:

`wc -l measurements.txt` should output `1000000000 measurements.txt`

`time ./calculate_average_baseline.sh`

> 176.07s user 4.37s system 99% cpu 3:02.18 total

Mine is 1 minute slower than the machine they were running the tests on. I'm running the tests on a
Macbook M2 Pro, 16GB RAM.

## Attempts

Avg time from 5 executions, dropping the highest and lowest value. I'm using `time ./bin/main` to measure
it and getting the total time (last value displayed).

| Attempt                                                            | Time   |
| ------------------------------------------------------------------ | ------ |
| [#1](#attempt-1---got-it-working)                                  | 165.3s |
| [#2](#attempt-2---first-improvements-and-fixes)                    | 128.0s |
| [#3](#attempt-3---parser-to-extract-data-from-the-bytes)           | 49.0s  |
| [#4](#attempt-4---from-helper-functions-to-a-more-manual-approach) | 35.52s |
| [#5](#attempt-5---workers-for-the-rescue)                          | 5.98s  |
| [#6](#attempt-6---getting-creative-when-parsing-measurement)       | 5.12s  |
| [#7](#attempt-7---custom-hash-function)                            | 4.82s  |

I'm pretty happy with the results. This challenge is super interesting because you can iterate over your
own solution trying to find a better way. I had a blast and learned a lot.

## Attempt #1 - Got it working

First working version. No optimizations, concurrency or anything fancy.

Added tests and profiling to prepare for the first improvements, lots of work ahead.

### Results

#### 165.3s

| Run | Time                                             | Result   |
| --- | ------------------------------------------------ | -------- |
| #1  | 160.42s user 5.52s system 101% cpu 2:43.43 total | drop min |
| #2  | 162.67s user 5.64s system 101% cpu 2:45.80 total | 2:45.80  |
| #3  | 164.42s user 5.72s system 101% cpu 2:47.66 total | 2:47.66  |
| #4  | 163.55s user 5.71s system 101% cpu 2:46.45 total | 2:46.45  |
| #5  | 178.00s user 5.64s system 101% cpu 3:00.98 total | drop max |
|     |                                                  | 165.3s   |

### Code state

[Release](https://github.com/Pedr0Rocha/1-billion-row-challenge/releases/tag/v1.0)

[Code](https://github.com/Pedr0Rocha/1-billion-row-challenge/tree/v1.0)

## Attempt #2 - First improvements and fixes

After adding pprof to the program, it is clear that scanning **each line** using the `bufio` `scanner.Text()`
is far from ideal. Pprof shows that we are spending ~80% of the time on the `read` `syscall`.

To solve this, we can try ingesting more rows per read by increasing the buffer size and
drastically reduce the number of `read` calls. Assuming that we have an avg. of 16 bytes per row, setting
the buffer size to `16 * 1024 * 1024` should be enough to read ~1M rows at once. This is now a param that needs
some tweaking later on.

However, this improvement introduced a new issue. Incomplete rows for each chunk read or "leftover".

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
backtrack the current buffer to the last new line. This attempt implements the second strategy.
Continuing the example above:

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

#### 128.0s

| Run | Time                                            | Result   |
| --- | ----------------------------------------------- | -------- |
| #1  | 125.39s user 1.04s system 98% cpu 2:08.82 total | 2:08.82  |
| #2  | 125.27s user 1.13s system 98% cpu 2:08.49 total | 2:08.49  |
| #3  | 125.19s user 1.03s system 98% cpu 2:08.23 total | 2:08.23  |
| #4  | 123.17s user 1.12s system 98% cpu 2:06.38 total | drop min |
| #5  | 126.22s user 1.13s system 98% cpu 2:09.33 total | drop max |
|     |                                                 | 128.0s   |

_Previous best was 165.3s_

### Code state

[Release](https://github.com/Pedr0Rocha/1-billion-row-challenge/releases/tag/v2.0)

[Code](https://github.com/Pedr0Rocha/1-billion-row-challenge/tree/v2.0)

## Attempt #3 - Parser to extract data from the bytes

The `read` `syscall` is now gone from the flamegraph, revealing the new performance killers.
`ParseFloat` from our measurement parsing and `WriteByte` from our string builder that extracts the data.

To improve it, we are going to use integers instead of float and only convert it to float
as the last operation to print it out. As for the `WriteByte`, we can write our own parser
for the resulting buffer and try to optimize it.

General improvements were also implemented as they would show up in pprof, such as unnecessary
assignments of `stationData` and bytes to string convertion.

At this level, `string(myBytes)` costs a lot and makes a big difference. After some research, we can
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

- This was a very unpleasant surprise, the map lookup is extremely slow.

```bash
38.53s  data, exist := stations[station]
```

And of course, we are still waiting for the resultBuffer to be parsed to continue reading from
the file. This can be improved by parsing pieces of the buffer at the same time using go routines
and merge the result somewhere.
But I'll try to improve the linear solution as much as possible before paralelizing the workload as
it will become more complex.

### Results

#### 49.0s

| Run | Time                                          | Result   |
| --- | --------------------------------------------- | -------- |
| #1  | 46.26s user 0.61s system 96% cpu 48.750 total | drop min |
| #2  | 47.18s user 0.58s system 96% cpu 49.373 total | 49.373   |
| #3  | 46.65s user 0.70s system 96% cpu 48.911 total | 48.911   |
| #4  | 47.17s user 0.83s system 96% cpu 49.752 total | drop max |
| #5  | 46.41s user 0.65s system 96% cpu 48.758 total | 48.758   |
|     |                                               | 49.0s    |

_Previous best time was 128.0s._

### Code state

[Release](https://github.com/Pedr0Rocha/1-billion-row-challenge/releases/tag/v3.0)

[Code](https://github.com/Pedr0Rocha/1-billion-row-challenge/tree/v3.0)

## Attempt #4 - From helper functions to a more manual approach

From those three problems from attempt #3, I could only tackle the parsing of the result buffer.
It was very expensive to use `bytes.Index` to find the next `;`. Iterating over the buffer
and cutting it manually reduced the time spent in this function from 21.03s to ~13.00s.

Another small improvement was to stop appending the chars to build the measurement and only
use indexes to extract it from the buffer while cutting the `.`. This saved us ~3.00s

There is still some work to do when parsing the row. We are copying and passing a lot of
bytes around when we could try to return the indexes representing the interval where the
main buffer should be cut to extract the correct data.

I did some research on how to solve the map lookup issue, with no success. We have some alternatives
to explore. Implementing our own hash to build the map key, using an int as key and check if it would
improve something or use a Swissmap. But I will avoid to use external libs for this challenge, so
unless I can implement a simple version of the Swissmap, I will skip it.

Now our biggest problems are map lookup, and, of course, all the waiting. Since I'm still not
sure how to move forward with the map key, it's time to spin up some go routines and kill the time
wasted on waiting.

### Results

#### 35.52s

| Run | Time                                          | Result   |
| --- | --------------------------------------------- | -------- |
| #1  | 32.79s user 0.57s system 94% cpu 35.227 total | drop min |
| #2  | 33.17s user 0.55s system 95% cpu 35.333 total | 35.333   |
| #3  | 33.21s user 0.61s system 95% cpu 35.360 total | 35.360   |
| #4  | 33.74s user 0.59s system 95% cpu 35.868 total | 35.868   |
| #5  | 33.70s user 0.97s system 96% cpu 36.092 total | drop max |
|     |                                               | 35.52s   |

_Previous best time was 49.0s._

### Code state

[Release](https://github.com/Pedr0Rocha/1-billion-row-challenge/releases/tag/v4.0)

[Code](https://github.com/Pedr0Rocha/1-billion-row-challenge/tree/v4.0)

## Attempt #5 - Workers for the rescue

Finally, it's concurrency time!

The concurrency pipeline has several elements that are connected to each other:

`chunk buffer channel` -> receives all the data read from the main file in chunks

`results channel` -> receives the maps produced by our workers

`file reader + chunk generator go routine` -> responsible for reading from the file by chunks,
handling the leftovers and pushing the result buffer to the `chunk buffer channel` to be processed.
It also manages the closing of the `chunk channel` and `results channel`.

`parse chunk workers` -> the workers read from the `chunk buffer channel` and process each chunk
to extract all the stations contained inside of the chunk. It reads until the `chunk buffer channel`
gets closed, which is right after the `file reader` finds EOF and pushes the last chunk.

To produce the final result, we also need a reader for the `results channel` that will aggregate
all the data produced by the workers. Since we will be writing the aggregate data in alphabetical order and
in the same slice, some serious locking would have to be put in place to avoid race conditions, making this piece
not very concurrency friendly.

I failed to draw a markdown table to explain this properly, so here's a diagram:

![Diagram](diagram.png)
_concurrency diagram_

### Results

#### 5.98s

| Run | Time                                          | Result   |
| --- | --------------------------------------------- | -------- |
| #1  | 42.10s user 2.47s system 724% cpu 6.154 total | 6.154    |
| #2  | 42.97s user 2.51s system 729% cpu 6.232 total | drop max |
| #3  | 42.58s user 2.50s system 771% cpu 5.843 total | drop min |
| #4  | 42.65s user 2.53s system 769% cpu 5.874 total | 5.874    |
| #5  | 42.64s user 2.50s system 760% cpu 5.934 total | 5.934    |
|     |                                               | 5.98s    |

_Previous best time was 35.52s._

### Code state

[Release](https://github.com/Pedr0Rocha/1-billion-row-challenge/releases/tag/v5.0)

[Code](https://github.com/Pedr0Rocha/1-billion-row-challenge/tree/v5.0)

## Attempt #6 - Getting creative when parsing measurement

Great! We managed to get to sub 10 seconds.

But of course, we still have some work to do. This is our "Most Wanted" poster now:

![Flame](flamegraph-attempt-6.png)
_most wanted poster_

I tried many ways of parsing the chunks, but could only save around 50ms in the end.
However, converting the measurement manually, byte per byte, paid off and we now achieved sub 5 seconds for
the first time! This is how it works:

Parsing: `Name;-98.7\n`, cursor: `[]`

| Input       | Output | Negative | Desc                                                                                                          |
| ----------- | ------ | -------- | ------------------------------------------------------------------------------------------------------------- |
| `-98.7\n`   | ``     | false    | Initial input                                                                                                 |
| `[-]98.7\n` | ``     | true     | Check for negative sign                                                                                       |
| `-[9]8.7\n` | `9`    | true     | It will always be the first digit. Parse using this [creative solution](https://stackoverflow.com/a/21322694) |
| `-9[8].7\n` | `98`   | true     | Check for last digit before decimal or dot. Multiply current output by 10 and sum                             |
| `-98[.]7\n` | `98`   | true     | Skip '.'                                                                                                      |
| `-98.[7]\n` | `987`  | true     | Parse the decimal digit                                                                                       |
| `-98.7[\n]` | `987`  | true     | Skip new line                                                                                                 |
| `-98.7\n`   | `-987` | true     | Apply negative sign                                                                                           |

But if we really want to see some significant progress, we need to tackle the map problem. We need to find
a way to improve the `runtime.mapaccess2_faststr` call.

### Results

#### 05.12s

| Run | Time                                          | Result   |
| --- | --------------------------------------------- | -------- |
| #1  | 34.21s user 2.42s system 683% cpu 5.362 total | drop max |
| #2  | 34.40s user 2.29s system 730% cpu 5.020 total | 5.020    |
| #3  | 34.40s user 2.41s system 693% cpu 5.310 total | 5.310    |
| #4  | 34.26s user 2.30s system 726% cpu 5.034 total | 5.034    |
| #5  | 34.24s user 2.43s system 736% cpu 4.980 total | drop min |
|     |                                               | 5.12s    |

_Previous best time was 5.98s._

### Code state

[Release](https://github.com/Pedr0Rocha/1-billion-row-challenge/releases/tag/v6.0)

[Code](https://github.com/Pedr0Rocha/1-billion-row-challenge/tree/v6.0)

## Attempt #7 - Custom hash function

It was not easy to find an improvement to the map `runtime.mapaccess2_faststr` call. I wanted to test if the
key being an int instead of a string we would get any improvements. The problem is convert our `[]byte` station
name into a unique uint64 while avoiding collisions.

I found [this](https://github.com/segmentio/fasthash/blob/master/fnv1/hash.go#L63) algorithm that can do
the job. To be honest, I thought we would get a much better improvement out of this. There must be a better
way still.

### Results

#### 04.82s

| Run | Time                                          | Result   |
| --- | --------------------------------------------- | -------- |
| #1  | 33.93s user 2.48s system 769% cpu 4.729 total | 4.729    |
| #2  | 34.45s user 2.41s system 786% cpu 4.686 total | drop min |
| #3  | 33.81s user 2.51s system 744% cpu 4.881 total | 4.881    |
| #4  | 33.95s user 2.42s system 749% cpu 4.855 total | 4.855    |
| #5  | 34.26s user 2.46s system 751% cpu 4.885 total | drop max |
|     |                                               | 4.82s    |

_Previous best time was 5.12s._

### Code state

[Release](https://github.com/Pedr0Rocha/1-billion-row-challenge/releases/tag/v7.0)

[Code](https://github.com/Pedr0Rocha/1-billion-row-challenge/tree/v7.0)
