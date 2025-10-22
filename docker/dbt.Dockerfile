# dbt runner container
FROM python:3.12-slim as builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install dbt and dependencies
RUN pip install --upgrade pip setuptools wheel && \
    pip install dbt-core>=1.7.0 dbt-databricks>=1.7.0

# Production stage
FROM python:3.12-slim as production

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    DBT_PROFILES_DIR=/app/dbt

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create app user
RUN groupadd -r dbtuser && useradd -r -g dbtuser dbtuser

# Set working directory
WORKDIR /app

# Copy dbt project
COPY dbt/ ./dbt/
COPY .env.example ./.env.example

# Create necessary directories
RUN mkdir -p logs target && \
    chown -R dbtuser:dbtuser /app

# Switch to non-root user
USER dbtuser

# Set working directory to dbt project
WORKDIR /app/dbt

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD dbt --version

# Default command
CMD ["dbt", "run"]

# Labels for metadata
LABEL maintainer="Data Engineering Team" \
      version="1.0" \
      description="dbt runner for Hiscox ETL Pipeline" \
      org.opencontainers.image.source="https://github.com/your-org/etl-hiscox"
