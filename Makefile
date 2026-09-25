.PHONY: build client server

build:
	go build ./...

client:
	go run ./client

server:
	go run ./server -debug

test:
	make
	go test ./raft/... -v
