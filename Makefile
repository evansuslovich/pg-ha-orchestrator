.PHONY: build client server

build:
	go build ./...

client:
	go run ./client

server:
	go run ./server -debug
