# Build stage — plain cargo build, no external toolchain requirements.
FROM rust:1.83-slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p server -p client -p bench

FROM debian:bookworm-slim
# iproute2 provides `tc` for netem fault injection inside the container.
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 && \
    rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/kvd /usr/local/bin/
COPY --from=build /src/target/release/kvctl /usr/local/bin/
COPY --from=build /src/target/release/loadgen /usr/local/bin/
ENTRYPOINT ["kvd"]
