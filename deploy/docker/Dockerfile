# syntax=docker/dockerfile:1.7
# The data-avalanche image (roadmap §4 M11a): the release binary on distroless, as the
# non-root user. Built from the repository root:
#   docker build -f deploy/docker/Dockerfile -t data-avalanche .

FROM rust:1.98.1-trixie AS build
WORKDIR /src
COPY . .
# The target directory is a cache across builds. Debug info is stripped, the symbols kept: a
# panic's backtrace still names its functions.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release --locked -p data-avalanche --bin data-avalanche \
    && cp target/release/data-avalanche /data-avalanche \
    && strip --strip-debug /data-avalanche

FROM gcr.io/distroless/cc-debian13:nonroot
ARG VERSION=dev
LABEL org.opencontainers.image.title="data-avalanche" \
      org.opencontainers.image.description="Change data capture from PostgreSQL, MySQL and MariaDB into Delta Lake" \
      org.opencontainers.image.source="https://github.com/alex-karpenko/data-avalanche" \
      org.opencontainers.image.version="${VERSION}"
COPY --from=build /data-avalanche /usr/local/bin/data-avalanche
EXPOSE 8080/tcp 7946/udp
ENTRYPOINT ["/usr/local/bin/data-avalanche"]
CMD ["run", "--config", "/etc/data-avalanche/config.yaml"]
