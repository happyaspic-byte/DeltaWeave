# syntax=docker/dockerfile:1.7

FROM rust:1.91.0-bookworm AS builder

WORKDIR /src
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --locked --release -p deltaweave

FROM debian:bookworm-slim AS runtime

ARG DELTAWEAVE_VERSION=0.1.0

LABEL org.opencontainers.image.title="DeltaWeave" \
      org.opencontainers.image.description="Authenticated content-defined P2P file transfer" \
      org.opencontainers.image.source="https://github.com/happyaspic-byte/DeltaWeave" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${DELTAWEAVE_VERSION}"

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 deltaweave \
    && useradd --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin deltaweave \
    && install -d -m 0700 -o 65532 -g 65532 /data

COPY --from=builder /src/target/release/deltaweave /usr/local/bin/deltaweave

USER 65532:65532
WORKDIR /data
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/deltaweave"]
CMD ["self-test"]
