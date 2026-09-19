.PHONY: all build test vet demo run generate clean

all: vet test build

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

demo:
	go run ./cmd/lsdemo

run:
	go run ./cmd/lifesupportd -db lifesupport.db -addr :50051

# 重新生成 protobuf 代码 (需要 protoc + protoc-gen-go + protoc-gen-go-grpc)
generate:
	mkdir -p gen
	protoc -I proto \
	  --go_out=gen --go_opt=module=lifesupport/gen \
	  --go-grpc_out=gen --go-grpc_opt=module=lifesupport/gen \
	  lifesupport/v1/lifesupport.proto

clean:
	rm -f lifesupport.db lifesupport.db-wal lifesupport.db-shm
