# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM rust:1.91.0-bookworm AS builder

ARG TARGETARCH

WORKDIR /src
COPY . .

ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++ \
    AR_aarch64_unknown_linux_gnu=aarch64-linux-gnu-ar

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    set -eux; \
    case "$TARGETARCH" in \
      amd64) target="x86_64-unknown-linux-gnu" ;; \
      arm64) \
        apt-get update; \
        apt-get install --no-install-recommends -y \
          gcc-aarch64-linux-gnu \
          g++-aarch64-linux-gnu \
          libc6-dev-arm64-cross; \
        rm -rf /var/lib/apt/lists/*; \
        target="aarch64-unknown-linux-gnu" \
        ;; \
      *) echo "unsupported target architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    rustup target add "$target"; \
    cargo build --locked --release --target "$target" -p deltaweave; \
    install -D -m 0755 "target/$target/release/deltaweave" /out/deltaweave

FROM debian:bookworm-slim AS runtime

ARG DELTAWEAVE_VERSION=0.1.2

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

COPY --from=builder /out/deltaweave /usr/local/bin/deltaweave

USER 65532:65532
WORKDIR /data
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/deltaweave"]
CMD ["self-test"]
