.PHONY: build client server

build:
	go build ./...

client:
	go build ./...
	go run ./client

server:
	go build ./...
	go run ./server -debug

test:
	go build ./...
	go test ./raft/... -v
