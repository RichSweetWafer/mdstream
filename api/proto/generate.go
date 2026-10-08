// Package proto holds the protobuf definitions of the mdstream API.
//
// Generated Go code is written to api/gen and is not committed. Run `make proto`
// (or `go generate ./api/proto/...`); it requires protoc, protoc-gen-go and
// protoc-gen-go-grpc on PATH (`make tools` installs the two plugins).
package proto

//go:generate protoc --proto_path=. --go_out=../.. --go_opt=module=mdstream --go-grpc_out=../.. --go-grpc_opt=module=mdstream mdstream/v1/mdstream.proto
