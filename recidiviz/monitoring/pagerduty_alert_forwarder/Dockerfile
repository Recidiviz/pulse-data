# Multi-stage build for alert forwarder service
FROM python:3.11-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Create virtualenv
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy and install requirements
COPY recidiviz/monitoring/pagerduty_alert_forwarder/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Final stage - minimal runtime image
FROM python:3.11-slim

# Copy virtualenv from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create app directory
WORKDIR /app

# Copy application code
COPY recidiviz/monitoring/pagerduty_alert_forwarder/ /app/recidiviz/monitoring/pagerduty_alert_forwarder/
COPY recidiviz/__init__.py /app/recidiviz/
COPY recidiviz/monitoring/__init__.py /app/recidiviz/monitoring/

# Create directory for config
RUN mkdir -p /app/config

# Copy default configuration
COPY recidiviz/monitoring/pagerduty_alert_forwarder/config.yaml /app/config/config.yaml

# Set Python path
ENV PYTHONPATH=/app

# Run as non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8080/health', timeout=3)"

# Run the server
CMD ["python", "-m", "recidiviz.monitoring.pagerduty_alert_forwarder.server"]
