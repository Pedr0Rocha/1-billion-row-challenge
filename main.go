package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"unsafe"
)

const (
	MAX_STATIONS = 10_000
	// on an average of 16 bytes per row, we now read ~1M rows at once
	CHUNK_SIZE = 16 * 1024 * 1024
)

type bufferRange [2]int
type StationMap map[uint64]*stationData

type stationData struct {
	name  []byte
	min   int
	max   int
	sum   int
	count int
}

func (s stationData) mean() float64 {
	return math.Ceil(float64(s.sum) / float64(s.count))
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	file, err := os.Open("measurements.txt")
	if err != nil {
		panic("Cannot read file")
	}
	defer file.Close()

	result := processFile(file, CHUNK_SIZE)
	fmt.Print(result)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

func processFile(file *os.File, chunkSize int) string {
	var wg sync.WaitGroup
	nWorkers := runtime.NumCPU() - 1

	resultsChannel := make(chan StationMap)
	chunkBufferChannel := make(chan []byte)

	// read chunks of bytes and send it over in the chunkBufferChannel
	go func() {
		readBuffer := make([]byte, chunkSize)
		var leftoverBuffer []byte
		for {
			readBytes, err := file.Read(readBuffer)
			if err == io.EOF {
				break
			}
			if readBytes == 0 {
				log.Fatal("could not read file")
			}

			readBuffer = readBuffer[:readBytes]

			// find last new line to set a stopping point where we have complete rows
			lastLineIndex := bytes.LastIndex(readBuffer, []byte("\n"))

			// walk until the last new line +1 to consume it
			// here we need to merge the last leftover with the current buffer
			resultBuffer := make([]byte, len(leftoverBuffer)+readBytes)
			resultBuffer = append(leftoverBuffer, readBuffer[:lastLineIndex+1]...)

			// prepare the leftovers to be used in the next iteration
			// which is everything that we left out when finding the last new line
			leftoverBuffer = make([]byte, len(readBuffer[lastLineIndex+1:]))
			copy(leftoverBuffer, readBuffer[lastLineIndex+1:])

			chunkBufferChannel <- resultBuffer
		}
		close(chunkBufferChannel)
		// waiting for all `parseChunk` calls to finish, meaning we have a result channel ready
		wg.Wait()
		close(resultsChannel)
	}()

	for range nWorkers {
		wg.Add(1)
		go func() {
			for chunk := range chunkBufferChannel {
				parseChunk(chunk, resultsChannel)
			}
			wg.Done()
		}()
	}

	// process all the station maps coming from the parseChunk workers
	stations := make(StationMap, MAX_STATIONS)
	for result := range resultsChannel {
		for st, data := range result {
			existingData, exist := stations[st]
			if !exist {
				stationData := stationData{
					name:  data.name,
					min:   data.min,
					max:   data.max,
					sum:   data.sum,
					count: data.count,
				}
				stations[st] = &stationData
				continue
			}
			existingData.min = min(existingData.min, data.min)
			existingData.max = max(existingData.max, data.max)
			existingData.count += data.count
			existingData.sum += data.sum
		}
	}

	results := make([]string, len(stations))
	resultsData := make(map[string]stationData, len(stations))
	iter := 0
	for s := range stations {
		name := bytesToString(stations[s].name)
		results[iter] = name
		resultsData[name] = *stations[s]
		iter++
	}

	sort.Strings(results)

	resultStr := "{"
	for _, v := range results {
		resultStr += fmt.Sprintf(
			"%s=%.1f/%.1f/%.1f, ",
			v,
			float64(resultsData[v].min)/10,
			float64(resultsData[v].mean())/10,
			float64(resultsData[v].max)/10,
		)
	}
	// remove last ', '
	resultStr = resultStr[:len(resultStr)-2]
	resultStr += "}\n"

	return resultStr
}

func parseChunk(resultBuffer []byte, resultsChan chan<- StationMap) {
	stations := make(StationMap, MAX_STATIONS)
	cursor := 0
	for cursor < len(resultBuffer)-1 {
		stationRange := bufferRange{cursor, 0}
		for {
			// found ';' -> name interval ends here and measurement starts next
			if resultBuffer[cursor] == ';' {
				stationRange[1] = cursor
				break
			}
			cursor++
		}
		// skip ';'
		cursor++

		// we are here \/
		// StationName;-99.9\n
		// StationName;99.9\n

		isNegative := false
		if resultBuffer[cursor] == '-' {
			isNegative = true
			cursor++
		}

		// first digit
		measure := int(resultBuffer[cursor] - '0')
		cursor++

		// at this point we can have either a number or a '.'
		if resultBuffer[cursor] != '.' {
			// https://stackoverflow.com/a/21322694
			measure = measure*10 + int(resultBuffer[cursor]-'0')
			cursor++
		}

		// walk because here we guarantee to have a '.'
		cursor++
		measure = measure*10 + int(resultBuffer[cursor]-'0')

		if isNegative {
			measure = -measure
		}

		station := resultBuffer[stationRange[0]:stationRange[1]]
		// skip new line
		cursor += 2

		hashedStation := customHash(station)

		data, exist := stations[hashedStation]
		if !exist {
			stationData := stationData{
				name:  station,
				min:   measure,
				max:   measure,
				sum:   measure,
				count: 1,
			}

			stations[hashedStation] = &stationData
			continue
		}

		data.min = min(data.min, measure)
		data.max = max(data.max, measure)
		data.count++
		data.sum += measure
	}
	resultsChan <- stations
}

// Replaces `stationName := string(stationNameInBytes)`
func bytesToString(b []byte) string {
	// gets the pointer to the underlying array of the slice
	pointerToArray := unsafe.SliceData(b)
	// returns the string of length len(b) of the bytes in the pointer
	return unsafe.String(pointerToArray, len(b))
}

func customHash(name []byte) uint64 {
	return addBytes64(Init64, name)
}

const (
	// FNV-1
	offset64 = uint64(14695981039346656037)
	prime64  = uint64(1099511628211)
	// Init64 is what 64 bits hash values should be initialized with.
	Init64 = offset64
)

// fasthash bytes to uint64 implementation
// https://github.com/segmentio/fasthash/blob/master/fnv1/hash.go#L63
func addBytes64(h uint64, b []byte) uint64 {
	for len(b) >= 8 {
		h = (h * prime64) ^ uint64(b[0])
		h = (h * prime64) ^ uint64(b[1])
		h = (h * prime64) ^ uint64(b[2])
		h = (h * prime64) ^ uint64(b[3])
		h = (h * prime64) ^ uint64(b[4])
		h = (h * prime64) ^ uint64(b[5])
		h = (h * prime64) ^ uint64(b[6])
		h = (h * prime64) ^ uint64(b[7])
		b = b[8:]
	}
	if len(b) >= 4 {
		h = (h * prime64) ^ uint64(b[0])
		h = (h * prime64) ^ uint64(b[1])
		h = (h * prime64) ^ uint64(b[2])
		h = (h * prime64) ^ uint64(b[3])
		b = b[4:]
	}
	if len(b) >= 2 {
		h = (h * prime64) ^ uint64(b[0])
		h = (h * prime64) ^ uint64(b[1])
		b = b[2:]
	}
	if len(b) > 0 {
		h = (h * prime64) ^ uint64(b[0])
	}
	return h
}
