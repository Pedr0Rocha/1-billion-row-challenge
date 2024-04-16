package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAll(t *testing.T) {
	paths, err := filepath.Glob("test_data/*.txt")
	if err != nil {
		t.Error(err)
		return
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			file, err := os.Open(path)
			if err != nil {
				t.Error(err)
				return
			}

			outputPath := strings.ReplaceAll(path, ".txt", ".out")
			expected, _ := os.ReadFile(outputPath)
			expectedContents := string(expected)

			testChunkSize := 1024 * 512

			result := processFile(file, testChunkSize)
			if result != expectedContents {
				t.Errorf("wrong result. \nexpected:%s, \ngot:%s", expectedContents, result)
			}
		})
	}
}
