# syntax=docker/dockerfile:1.6

# --- Base image (slim) ---
FROM --platform=$BUILDPLATFORM python:3.11-slim AS base

# Metadata
LABEL maintainer="Johan Eelde Koivisto <Johan@eelde-koivisto.se>"
LABEL description="Flask-SocketIO gateway (multi-arch ready)"

# Miljöinställningar
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# --- System dependencies ---
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# --- Copy & install Python dependencies ---
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# --- Copy application ---
COPY app.py .

# --- Non-root user ---
RUN useradd -u 1000 -m appuser
USER appuser

# --- Default ENV ---
ENV REDIS_HOST=redis \
    REDIS_PORT=6379 \
    LATEST_HASH_KEY=latest:ohlc \
    USE_STREAM=0 \
    STREAM_NAME=ticks.v1 \
    STREAM_MAXLEN=100000

EXPOSE 5000

# --- Entrypoint ---
CMD ["python", "app.py"]
