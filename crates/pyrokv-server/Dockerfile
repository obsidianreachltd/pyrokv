FROM rust:bullseye AS builder

WORKDIR /usr/src/pyrokv-server

COPY . .

RUN cargo build --release

FROM debian:bullseye-slim

WORKDIR /usr/src/pyrokv-server

COPY --from=builder /usr/src/pyrokv-server/target/release/pyrokv-server .

EXPOSE 8001

ENTRYPOINT ["/bin/bash", "-c", "./pyrokv-server --port ${PYROKV_PORT:-8001}"]