.PHONY: build test lint vet proto clean run certs

build:
	go build -o bin/botmanager ./cmd/botmanager
	go build -o bin/botmanager-certs ./cmd/botmanager-certs

# Оба модуля: сервис и api/ (контракт + клиентская библиотека).
test:
	go test ./...
	cd api && go test ./...

vet:
	go vet ./...
	cd api && go vet ./...

# staticcheck не входит в стандартную поставку Go:
#   go install honnef.co/go/tools/cmd/staticcheck@latest
lint: vet
	$$(go env GOPATH)/bin/staticcheck ./...
	cd api && $$(go env GOPATH)/bin/staticcheck ./...

# Кодогенерация из api/proto/botmanager.proto в api/botmanagerpb через buf
# (системный protoc не нужен). Плагины и buf — см. api/buf.gen.yaml.
proto:
	cd api && buf generate proto

# Локальный запуск одного узла без TLS (config/dev.yaml, данные в ./data).
run: build
	./bin/botmanager -config config/dev.yaml

# CA, сертификат узла node-1 для localhost и ключ токенов в ./certs.
certs:
	go run ./cmd/botmanager-certs init -out certs
	go run ./cmd/botmanager-certs issue -out certs -name node-1 -hosts 127.0.0.1,localhost

clean:
	rm -rf bin
