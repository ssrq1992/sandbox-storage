GO ?= go
.PHONY: build test race vet generate check-generated linux integration
build:
	$(GO) build -o bin/ ./cmd/...
test:
	$(GO) test ./...
race:
	$(GO) test -race ./...
vet:
	$(GO) vet ./...
generate:
	$(GO) run ./tools/generate
check-generated:
	python3 scripts/check_generated.py
linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build -o bin/linux/ ./cmd/...
integration:
	bash scripts/integration.sh
