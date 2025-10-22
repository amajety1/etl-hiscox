# Multi-stage build for transformation pipeline
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
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Create and activate virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install Python dependencies
COPY requirements-minimal.txt /tmp/requirements-minimal.txt
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r /tmp/requirements-minimal.txt && \
    pip install pyspark==3.5.0 delta-spark==3.0.0

# Production stage
FROM python:3.12-slim as production

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3

# Install runtime dependencies including Java
RUN apt-get update && apt-get install -y \
    curl \
    openjdk-17-jdk \
    procps \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Download and install Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz | \
    tar -xz -C /opt && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    chown -R root:root /opt/spark

# Create app user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Copy application code
COPY scripts/ ./scripts/
COPY dbt/ ./dbt/
COPY .env.example ./.env.example

# Create necessary directories
RUN mkdir -p logs data spark-warehouse && \
    chown -R appuser:appuser /app && \
    chown -R appuser:appuser /opt/spark/work-dir 2>/dev/null || true

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import pyspark; print('Spark available'); import sys; sys.exit(0)"

# Default command
CMD ["python", "scripts/transformation.py"]

# Labels for metadata
LABEL maintainer="Data Engineering Team" \
      version="1.0" \
      description="Hiscox ETL Transformation Pipeline with Spark" \
      org.opencontainers.image.source="https://github.com/your-org/etl-hiscox"
