# fairsched

A VFT-based multi-tenant fair task scheduler.

---

## Development Setup

### Prerequisites

- **Go 1.22+** — [https://go.dev/dl](https://go.dev/dl)
- **buf** — proto compiler and linter, [official installation guide](https://buf.build/docs/cli/installation/)
- **protoc-gen-go** and **protoc-gen-go-grpc** — Go code generation plugins (Make sure `$(go env GOPATH)/bin` is on your `$PATH`)
  ```bash
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  ```

---

## Commands

| Command | Description |
|---------|-------------|
| `make gen` | Generate Go stubs from `proto/worker/v1/worker.proto` into `gen/` |
| `make build` | Compile all packages |

---

## Architecture

Two separate listeners:

| Listener | Protocol | Port | Consumers |
|----------|----------|------|-----------|
| Admin + Submit API | HTTP/JSON | `:8080` | Producers, ops tooling |
| Worker Poll API | gRPC | `:9090` | Worker binaries only |
