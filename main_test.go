package main

import (
	"os"
	"testing"
)

func TestMain(t *testing.T) {
	t.Run("should return correct output for small dataset", func(t *testing.T) {
		input, _ := os.Open("test_data/input.txt")
		defer input.Close()
		expected, _ := os.ReadFile("test_data/expected.txt")
		expectedContents := string(expected)

		testChunkSize := 32

		result := processFile(input, testChunkSize)

		if result != expectedContents {
			t.Errorf("wrong result. \nexpected:%s, \ngot:%s", expectedContents, result)
		}
	})

	t.Run("should return correct output for 1M entries dataset", func(t *testing.T) {
		input, _ := os.Open("test_data/input-1m.txt")
		defer input.Close()
		expected, _ := os.ReadFile("test_data/expected-1m.txt")
		expectedContents := string(expected)

		testChunkSize := 1024 * 512

		result := processFile(input, testChunkSize)

		if result != expectedContents {
			t.Errorf("wrong result. \nexpected:%s, \ngot:%s", expectedContents, result)
		}
	})
}
