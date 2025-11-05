FROM rust:bullseye AS builder

WORKDIR /usr/src/pyrokv-server

COPY . .

RUN cargo build --release

FROM debian:bullseye-slim

WORKDIR /usr/src/pyrokv-server

COPY --from=builder /usr/src/pyrokv-server/target/release/pyrokv-server .

RUN mkdir -p /var/lib/pyrokv/data

CMD ["./pyrokv-server"]