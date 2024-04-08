package main

import (
	"bufio"
	"flag"
	"fmt"
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

	file, err := os.Open("50m-measurements.txt")
	if err != nil {
		panic("Cannot read file")
	}
	defer file.Close()

	result := processFile(file)
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

func processFile(file *os.File) string {
	scanner := bufio.NewScanner(file)

	stations := make(map[string]*stationData, MAX_STATIONS)
	for scanner.Scan() {
		line := scanner.Text()

		parts := strings.Split(line, ";")
		station := parts[0]
		measurementString := parts[1]
		measurementFloat, err := strconv.ParseFloat(measurementString, 64)
		if err != nil {
			panic("Could not convert string value to float")
		}

		_, exist := stations[station]
		if !exist {
			stationData := stationData{
				min:   measurementFloat,
				max:   measurementFloat,
				count: 1,
				sum:   measurementFloat,
			}

			stations[station] = &stationData
			continue
		}

		if stations[station].min > measurementFloat {
			stations[station].min = measurementFloat
		}
		if stations[station].max < measurementFloat {
			stations[station].max = measurementFloat
		}

		stations[station].count++
		stations[station].sum += measurementFloat
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
