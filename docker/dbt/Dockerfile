# Use official dbt image as base
FROM ghcr.io/dbt-labs/dbt-databricks:1.6.0

# Set environment variables
ENV DBT_PROFILES_DIR=/app/dbt \
    DBT_PROJECT_DIR=/app/dbt \
    PYTHONPATH=/app

# Set work directory
WORKDIR /app

# Install additional Python packages
RUN pip install --upgrade pip \
    && pip install \
        azure-storage-blob==12.17.0 \
        azure-keyvault-secrets==4.7.0 \
        azure-identity==1.14.0 \
        great-expectations==0.17.23 \
        pydantic==1.10.12

# Copy dbt project files
COPY dbt/ ./dbt/

# Copy utility scripts
COPY scripts/utils/ ./scripts/utils/

# Create directories
RUN mkdir -p /app/logs /app/tmp

# Create non-root user
RUN groupadd -r dbtuser && useradd -r -g dbtuser dbtuser \
    && chown -R dbtuser:dbtuser /app
USER dbtuser

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD dbt debug --profiles-dir $DBT_PROFILES_DIR --project-dir $DBT_PROJECT_DIR || exit 1

# Default command
CMD ["dbt", "run", "--profiles-dir", "/app/dbt", "--project-dir", "/app/dbt"]
