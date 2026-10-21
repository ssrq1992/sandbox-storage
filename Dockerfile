FROM golang:1.26.3-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ssf-node ./cmd/ssf-node && \
    CGO_ENABLED=0 go build -o /out/ssf-guard ./cmd/ssf-guard && \
    CGO_ENABLED=0 go build -o /out/ssf-csi-gateway ./cmd/ssf-csi-gateway && \
    CGO_ENABLED=0 go build -o /out/ssfctl ./cmd/ssfctl
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
USER 65532:65532
ENTRYPOINT ["ssf-node"]
