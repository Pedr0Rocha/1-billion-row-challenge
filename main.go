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
	"strings"
)

const (
	MAX_STATIONS = 10_000
	// on an average of 16 bytes per row, we now read ~1M rows at once
	CHUNK_SIZE = 16 * 1024 * 1024
)

type stationData struct {
	min   float64
	max   float64
	sum   float64
	count int64
}

func (s stationData) mean() float64 {
	return s.sum / float64(s.count)
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

		var stationName, measurement string
		var stringBuilder strings.Builder

		for _, char := range resultBuffer {
			switch char {
			case '\n':
				measurement = stringBuilder.String()
				stringBuilder.Reset()
				break

			case ';':
				stationName = stringBuilder.String()
				stringBuilder.Reset()
				break

			default:
				stringBuilder.WriteByte(char)
			}

			// parsed a complete name and measurement, store and reset
			if len(stationName) != 0 && len(measurement) != 0 {
				measurementFloat, _ := strconv.ParseFloat(measurement, 64)
				station := stationName

				stationName = ""
				measurement = ""

				data, exist := stations[station]
				if !exist {
					stationData := stationData{
						min:   measurementFloat,
						max:   measurementFloat,
						sum:   measurementFloat,
						count: 1,
					}

					stations[station] = &stationData
					continue
				}

				if data.min > measurementFloat {
					stations[station].min = measurementFloat
				}
				if data.max < measurementFloat {
					stations[station].max = measurementFloat
				}

				stations[station].count++
				stations[station].sum += measurementFloat
			}
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
		resultStr += fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", v, stations[v].min, stations[v].mean(), stations[v].max)
	}
	// remove last ', '
	resultStr = resultStr[:len(resultStr)-2]
	resultStr += "}\n"

	return resultStr
}
