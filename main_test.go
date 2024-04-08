package main

import (
	"os"
	"testing"
)

const (
	// small enough to have leftovers
	TEST_CHUNK_SIZE = 64
)

func TestMain(t *testing.T) {
	t.Run("should return correct output", func(t *testing.T) {
		input, _ := os.Open("test_data/input.txt")
		defer input.Close()
		expected, _ := os.ReadFile("test_data/expected.txt")
		expectedContents := string(expected)

		result := processFile(input, TEST_CHUNK_SIZE)

		if result != expectedContents {
			t.Errorf("wrong result. \nexpected:%s, \ngot:%s", expectedContents, result)
		}
	})
}
