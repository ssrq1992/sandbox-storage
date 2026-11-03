FROM golang:1.26.3-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY sdk/go/control ./sdk/go/control
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/sandboxschedadapter ./cmd/sandboxschedadapter

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates e2fsprogs util-linux && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/sandboxschedadapter /usr/local/bin/sandboxschedadapter
ENTRYPOINT ["sandboxschedadapter"]
