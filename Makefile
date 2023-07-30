build:
	go build -o bin/pallas cmd/main.go

run: build
	./bin/pallas

test:
	go test ./...