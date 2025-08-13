FROM rust:1.75 as build
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim
COPY --from=build /app/target/release/lsmt /usr/local/bin/lsmt
CMD ["lsmt"]
