.PHONY: gen build

# gen: generate protobuf stubs
gen:
	@mkdir -p gen
	buf generate proto

## build: compile all packages
build:
	go build ./...
