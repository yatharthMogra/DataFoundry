# ── Build stage ──────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Runtime stage ────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY src/ src/
COPY config/ config/

# Create output directories
RUN mkdir -p output/data output/dlq

# Expose webhook + metrics ports
EXPOSE 8080 9090

# Run the ingestion service
ENTRYPOINT ["python", "-m", "src"]
