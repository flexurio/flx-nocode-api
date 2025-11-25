FROM rust:1.91-slim

RUN apt update
RUN apt install pkg-config libssl-dev -y
RUN apt install -y iputils-ping
RUN apt install cmake -y

# Set working directory
WORKDIR /app

# Copy the actual source code
COPY . /app

# Build the application
RUN cargo build --release

EXPOSE 8080
# Set the startup command
CMD ["./target/release/flexurio-api-nocode-v2"]
