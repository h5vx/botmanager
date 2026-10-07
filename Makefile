.PHONY: build test lint vet proto clean run

build:
	go build -o bin/botmanager ./cmd/botmanager

test:
	go test ./...

vet:
	go vet ./...

# staticcheck — часть требования CLAUDE.md ("go vet, staticcheck"); не входит
# в стандартную библиотеку, поэтому целится в GOPATH/bin, а не в PATH.
lint: vet
	$$(go env GOPATH)/bin/staticcheck ./...

# Кодогенерация из proto/botmanager.proto.
#
# Требует protoc и плагины Go в PATH:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#
# Установка самого protoc — через системный пакетный менеджер
# (apt install protobuf-compiler / brew install protobuf / pacman -S protobuf).
proto:
	mkdir -p proto/gen
	protoc \
		--go_out=proto/gen --go_opt=paths=source_relative \
		--go-grpc_out=proto/gen --go-grpc_opt=paths=source_relative \
		--proto_path=proto \
		proto/botmanager.proto

run: build
	./bin/botmanager -config config/config.yaml

clean:
	rm -rf bin
