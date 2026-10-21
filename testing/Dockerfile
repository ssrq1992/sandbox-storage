FROM golang:1.26.3-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends util-linux e2fsprogs openssl python3 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN go mod download && go build -o /usr/local/bin/ssf-test-driver ./cmd/ssf-test-driver
ENTRYPOINT ["ssf-test-driver"]
