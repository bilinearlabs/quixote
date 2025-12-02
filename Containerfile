# Build stage ---------------

    FROM docker.io/rust:1.91.1-trixie AS builder

    WORKDIR /app
    COPY . .
    RUN cargo build --release

    # Runtime stage -------------

    FROM docker.io/debian:trixie-slim AS runtime
    WORKDIR /app
    COPY --from=builder /app/target/release/etherduck etherduck
    ENTRYPOINT [ "./etherduck" ]
