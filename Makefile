.PHONY: gen build build-server build-worker start-server start-worker

# gen: generate protobuf stubs
gen:
	@mkdir -p gen
	buf generate proto

build-server:
	go build -o ./bin/fairsched-server ./cmd/fairsched-server

build-worker:
	go build -o ./bin/worker ./cmd/worker

## build: compile all binaries
build: build-server build-worker

start-server:
	go run ./cmd/fairsched-server

start-worker:
	go run ./cmd/worker