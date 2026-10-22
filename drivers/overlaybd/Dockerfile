# syntax=docker/dockerfile:1.7
# Joint build context: sandboxstorage, containing AgentENV and sandbox-storage.
# Images match the existing native build baseline; no toolchain upgrade here.
FROM golang:1.26.0-bookworm AS go-builder
WORKDIR /build/sandbox-storage
COPY sandbox-storage/go.mod sandbox-storage/go.sum ./
COPY sandbox-storage/sdk/go/control ./sdk/go/control
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked go mod download
COPY sandbox-storage .
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked \
    CGO_ENABLED=0 go build -trimpath -o /out/overlaybd-csi ./cmd/overlaybd-csi && \
    CGO_ENABLED=0 go build -trimpath -o /out/overlaybd-config ./cmd/overlaybd-config && \
    CGO_ENABLED=0 go build -trimpath -o /out/overlaybd-register ./cmd/overlaybd-register && \
    CGO_ENABLED=0 go build -trimpath -o /out/overlaybd-evidence ./cmd/overlaybd-evidence

FROM rust:1.99.1-bookworm AS native-builder
ENV CARGO_TARGET_DIR=/build/target
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang libclang-dev protobuf-compiler libprotobuf-dev cmake pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY AgentENV /build/AgentENV
COPY sandbox-storage/adapter/agentenvadapter/runtime /build/sandbox-storage/adapter/agentenvadapter/runtime
COPY sandbox-storage/drivers/overlaybd/native /build/sandbox-storage/drivers/overlaybd/native
COPY sandbox-storage/api/overlaybd /build/sandbox-storage/api/overlaybd
WORKDIR /build/AgentENV
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --locked --release -p uvm-ublk-daemon --bin uvm-ublk-daemon
WORKDIR /build/sandbox-storage/drivers/overlaybd/native
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --locked --release --features device-host --bin ssf-overlaybd-native

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates e2fsprogs util-linux libstdc++6 libssl3 zlib1g libzstd1 liblz4-1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=go-builder /out/overlaybd-csi /usr/local/bin/overlaybd-csi
COPY --from=go-builder /out/overlaybd-config /usr/local/bin/overlaybd-config
COPY --from=go-builder /out/overlaybd-register /usr/local/bin/overlaybd-register
COPY --from=go-builder /out/overlaybd-evidence /usr/local/bin/overlaybd-evidence
COPY --from=native-builder /build/target/release/ssf-overlaybd-native /usr/local/bin/ssf-overlaybd-native
COPY --from=native-builder /build/target/release/uvm-ublk-daemon /usr/local/bin/uvm-ublk-daemon
# Explicit processes/units own lifetimes. This image must not use a shell trap
# that kills the managed daemon before VMM stop and final publication complete.
ENTRYPOINT ["/usr/local/bin/overlaybd-csi"]
