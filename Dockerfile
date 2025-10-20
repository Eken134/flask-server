# Dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# OS-build-deps (krävs för eventlet/greenlet m.fl.) + certs
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential gcc python3-dev libffi-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Dependencies först (bättre cache)
COPY requirements.txt .
RUN python -m pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

# App-kod
COPY app.py .

# Kör som non-root
RUN useradd -u 1000 -m appuser
USER appuser

# Default ENVs (kan överskridas i K8s)
ENV REDIS_HOST=redis \
    REDIS_PORT=6379 \
    LATEST_HASH_KEY=latest:ohlc \
    USE_STREAM=0 \
    STREAM_NAME=ticks.v1 \
    STREAM_MAXLEN=100000

EXPOSE 5000
CMD ["python", "app.py"]
