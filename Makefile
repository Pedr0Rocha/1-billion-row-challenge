all: test build

build:
	go build -o bin/main main.go

test:
	go test -v ./...

clean:
	rm -f bin/main

run: build
	time ./bin/main

fmt:
	go fmt ./...

prof: build
	./bin/main -cpuprofile cpu.prof -memprofile mem.prof

.PHONY: all build test clean run fmt
