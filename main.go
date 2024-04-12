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

type bufferRange [2]int

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

func parseChunk2(resultBuffer []byte, resultsChan chan<- StationMap) {
	stations := make(StationMap, MAX_STATIONS)
	i := 0
	lastIndex := 0
	for {
		if lastIndex >= len(resultBuffer)-1 {
			break
		}
		stationRange := bufferRange{}
		measureRange := bufferRange{}

		i = lastIndex
		stationRange[0] = i
		for {
			// found ';' -> name interval ends here and measurement starts next
			if resultBuffer[i] == ';' {
				stationRange[1] = i
				measureRange[0] = i + 1
				break
			}
			i++
		}
		for {
			// found '.' -> swap decimal with dot to return interval: 33'.3' -> 33'3.'
			// this way we can return the measurement interval without the '.'
			if resultBuffer[i] == '.' {
				resultBuffer[i] = resultBuffer[i+1]
				measureRange[1] = i + 1
				break
			}
			i++
		}

		// readBytes, stationRange, measureRange := getRowIntervals2(resultBuffer)

		measurementInt, _ := strconv.Atoi(bytesToString(resultBuffer[measureRange[0]:measureRange[1]]))
		station := bytesToString(resultBuffer[stationRange[0]:stationRange[1]])

		// fmt.Println("adding", station)
		// fmt.Println("adding", measurementInt)

		// account for separators and new line
		// readBytes := i + 3
		lastIndex = i + 3
		// resultBuffer = resultBuffer[i+3:]

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

func parseChunk(resultBuffer []byte, resultsChan chan<- StationMap) {
	stations := make(StationMap, MAX_STATIONS)
	for len(resultBuffer) != 0 {
		readBytes, stationName, measurement := parseRow(resultBuffer)
		resultBuffer = resultBuffer[readBytes:]

		station := bytesToString(stationName)
		measurementInt, _ := strconv.Atoi(bytesToString(measurement))

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

func getRowIntervals2(resultBuffer []byte) (int, bufferRange, bufferRange) {
	stationRange := bufferRange{}
	measurementRange := bufferRange{}

	i := 0
	for {
		// found ';' -> name interval ends here and measurement starts next
		if resultBuffer[i] == ';' {
			stationRange[1] = i
			measurementRange[0] = i + 1
			break
		}

		i++
	}

	for {

		// found '.' -> swap decimal with dot to return interval: 33'.3' -> 33'3.'
		// this way we can return the measurement interval without the '.'
		if resultBuffer[i] == '.' {
			resultBuffer[i] = resultBuffer[i+1]
			measurementRange[1] = i + 1
			break
		}

		i++
	}

	// account for separators and new line
	readBytes := i + 3
	return readBytes, stationRange, measurementRange
}

// parses the buffer to extract station name and measurement from a single row
// also returns the remainder bytes of the buffer
func parseRow(resultBuffer []byte) (int, []byte, []byte) {
	cursor := 0
	var stationName []byte
	for i := 0; i < len(resultBuffer); i++ {
		if resultBuffer[i] == ';' {
			stationName = resultBuffer[:i]
			// skip ';'
			cursor = i + 1
			break
		}
	}

	var measurement []byte
	for i := cursor; i < len(resultBuffer); i++ {
		if resultBuffer[i] == '.' {
			resultBuffer[i] = resultBuffer[i+1]
			measurement = resultBuffer[cursor : i+1]
			// skip '.', '\n'
			cursor = i + 3
			break
		}
	}
	return cursor, stationName, measurement
}

// Replaces `stationName := string(stationNameInBytes)`
func bytesToString(b []byte) string {
	// gets the pointer to the underlying array of the slice
	pointerToArray := unsafe.SliceData(b)
	// returns the string of length len(b) of the bytes in the pointer
	return unsafe.String(pointerToArray, len(b))
}
