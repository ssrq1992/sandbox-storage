FROM golang:1.26.0-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY sdk/go/control ./sdk/go/control
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /agentenvadapter ./adapter/agentenvadapter/cmd/agentenvadapter
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates e2fsprogs util-linux && rm -rf /var/lib/apt/lists/*
COPY --from=build /agentenvadapter /usr/local/bin/agentenvadapter
ENTRYPOINT ["agentenvadapter"]
