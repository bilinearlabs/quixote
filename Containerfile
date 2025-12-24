# Build stage ---------------

FROM docker.io/rust:1.91.1-trixie AS builder

# Setup Miniconda to package the Python frontend.
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"

# Install wget to fetch Miniconda
RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Miniconda on x86 or ARM platforms
RUN arch=$(uname -m) && \
    if [ "$arch" = "x86_64" ]; then \
    MINICONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"; \
    elif [ "$arch" = "aarch64" ]; then \
    MINICONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-aarch64.sh"; \
    else \
    echo "Unsupported architecture: $arch"; \
    exit 1; \
    fi && \
    wget $MINICONDA_URL -O miniconda.sh && \
    mkdir -p /root/.conda && \
    bash miniconda.sh -b -p /root/miniconda3 && \
    rm -f miniconda.sh

RUN conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main
RUN conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r
RUN conda create --prefix=/app/quixote_frontend_env python=3.11 streamlit pyarrow -y

RUN apt-get update && apt-get install -y wget unzip
WORKDIR /app
COPY . .
RUN wget https://github.com/duckdb/duckdb/releases/download/v1.4.2/libduckdb-linux-amd64.zip \
    -O libduckdb.zip \
    && unzip -o -q libduckdb.zip -d libduckdb
RUN DUCKDB_LIB_DIR=$PWD/libduckdb \
    DUCKDB_INCLUDE_DIR=$PWD/libduckdb \
    LD_LIBRARY_PATH=$PWD/libduckdb \
    cargo build --release

# Runtime stage -------------

FROM docker.io/debian:trixie-slim AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/quixote quixote
COPY --from=builder /app/libduckdb /duckdb
COPY --from=builder /app/frontend/generic_dashboard.py frontend/generic_dashboard.py
COPY --from=builder /app/quixote_frontend_env quixote_frontend_env
ENV DUCKDB_LIB_DIR=/duckdb
ENV LD_LIBRARY_PATH=/duckdb
ENTRYPOINT [ "./quixote" ]
