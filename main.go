package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"unsafe"
)

const (
	MAX_STATIONS = 10_000
	// on an average of 16 bytes per row, we now read ~1M rows at once
	// @TODO: experiment buffer sizes
	CHUNK_SIZE = 16 * 1024 * 1024
)

type StationMap map[string]*stationData

type stationData struct {
	min   int
	max   int
	sum   int
	count int
}

func (s stationData) mean() float64 {
	return float64(s.sum) / float64(s.count)
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

	// @TODO: experiment with buffered channels
	resultsChannel := make(chan StationMap)
	chunkBufferChannel := make(chan []byte)

	// read chunks of bytes and send it over in the chunkBufferChannel
	go func() {
		readBuffer := make([]byte, chunkSize)
		var leftoverBuffer []byte
		for {
			readBytes, err := file.Read(readBuffer)
			if err == io.EOF {
				// read the whole file
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
			// will read until the chunk channel closes
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
					min:   data.min,
					max:   data.max,
					sum:   data.sum,
					count: data.count,
				}

				stations[st] = &stationData
				continue
			}

			if existingData.min > data.min {
				existingData.min = data.min
			}
			if existingData.max < data.max {
				existingData.max = data.max
			}

			existingData.count += data.count
			existingData.sum += data.sum
		}
	}

	results := make([]string, len(stations))
	iter := 0
	for s := range stations {
		results[iter] = s
		iter++
	}

	sort.Strings(results)

	resultStr := "{"
	for _, v := range results {
		resultStr += fmt.Sprintf(
			"%s=%.1f/%.1f/%.1f, ",
			v,
			float64(stations[v].min)/10,
			float64(stations[v].mean())/10,
			float64(stations[v].max)/10,
		)
	}
	// remove last ', '
	resultStr = resultStr[:len(resultStr)-2]
	resultStr += "}\n"

	return resultStr
}

func parseChunk(resultBuffer []byte, resultsChan chan<- StationMap) {
	stations := make(StationMap, MAX_STATIONS)
	for len(resultBuffer) != 0 {
		newBuffer, stationName, measurement := parseRow(resultBuffer)
		resultBuffer = newBuffer

		measurementInt, _ := strconv.Atoi(bytesToString(measurement))
		station := bytesToString(stationName)

		data, exist := stations[station]
		if !exist {
			stationData := stationData{
				min:   measurementInt,
				max:   measurementInt,
				sum:   measurementInt,
				count: 1,
			}

			stations[station] = &stationData
			continue
		}

		if data.min > measurementInt {
			data.min = measurementInt
		}
		if data.max < measurementInt {
			data.max = measurementInt
		}

		data.count++
		data.sum += measurementInt
	}
	resultsChan <- stations
}

// parses the buffer to extract station name and measurement from a single row
// also returns the remainder bytes of the buffer
// can most likely be modified to return byte interval positions, name[0:20], measurement[21:25]
func parseRow(resultBuffer []byte) ([]byte, []byte, []byte) {
	cursor := -1
	var stationName []byte
	for i, char := range resultBuffer {
		if char != ';' {
			continue
		} else {
			stationName = resultBuffer[:i]
			// skip ';'
			cursor = i + 1
			break
		}
	}

	resultBuffer = resultBuffer[cursor:]

	endIndex := -1
	var measurement []byte
	for i, char := range resultBuffer {
		if char != '.' {
			continue
		} else {
			measurement = resultBuffer[:i]
			measurement = append(measurement, resultBuffer[i+1])
			// skip '.X\n'
			endIndex = i + 3
			break
		}
	}

	resultBuffer = resultBuffer[endIndex:]

	return resultBuffer, stationName, measurement
}

// Replaces `stationName := string(stationNameInBytes)`
func bytesToString(b []byte) string {
	// gets the pointer to the underlying array of the slice
	pointerToArray := unsafe.SliceData(b)
	// returns the string of length len(b) of the bytes in the pointer
	return unsafe.String(pointerToArray, len(b))
}
