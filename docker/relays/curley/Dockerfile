# syntax=docker/dockerfile:1.7

ARG RUST_IMAGE="rust:1.94.0-bookworm@sha256:365468470075493dc4583f47387001854321c5a8583ea9604b297e67f01c5a4f"
ARG RUNTIME_IMAGE="debian:bookworm-slim@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df"

FROM ${RUST_IMAGE} AS builder

ARG MOQ_REF
RUN test -n "${MOQ_REF}"
RUN git clone --filter=blob:none https://github.com/moq-dev/moq.git /src && \
    git -C /src checkout --detach "${MOQ_REF}"
WORKDIR /src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -p moq-relay && \
    cp target/release/moq-relay /usr/local/bin/moq-relay

FROM ${RUNTIME_IMAGE}
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/moq-relay /usr/local/bin/moq-relay
EXPOSE 443/udp
