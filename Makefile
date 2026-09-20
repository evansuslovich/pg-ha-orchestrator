.PHONY: build client server

build:
	go build ./...

client:
	go run ./client

server:
	go run ./server

server-debug:
	go run ./server -debug
