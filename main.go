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
)

const (
	MAX_STATIONS = 10_000
	// on an average of 16 bytes per row, we now read ~1M rows at once
	CHUNK_SIZE = 16 * 1024 * 1024
)

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
	readBuffer := make([]byte, chunkSize)
	var leftoverBuffer []byte

	stations := make(map[string]*stationData, MAX_STATIONS)
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

		for len(resultBuffer) != 0 {
			newBuffer, stationName, measurement := parseBufferSingle(resultBuffer)
			resultBuffer = newBuffer

			measurementInt, _ := strconv.Atoi(string(measurement))
			station := string(stationName)

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

func parseBufferSingle(resultBuffer []byte) ([]byte, []byte, []byte) {
	splitIndex := bytes.Index(resultBuffer, []byte{';'})
	stationName := resultBuffer[:splitIndex]

	resultBuffer = resultBuffer[splitIndex+1:]

	var measurement []byte
	endIndex := -1
	for i, char := range resultBuffer {
		if char != '.' {
			measurement = append(measurement, char)
		} else {
			measurement = append(measurement, resultBuffer[i+1])
			// jump '.', '/', 'n'
			endIndex = i + 3
			break
		}
	}

	resultBuffer = resultBuffer[endIndex:]

	return resultBuffer, stationName, measurement
}
