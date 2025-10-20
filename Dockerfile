FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# build deps (rarely used if we get wheels, but keep as fallback)
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential gcc python3-dev libffi-dev libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# requirements first (cache-friendly)
COPY requirements.txt .

# upgrade build tooling
RUN python -m pip install --upgrade pip setuptools wheel

# try to ensure greenlet comes from wheel (faster & avoids compile under QEMU)
RUN pip install --no-cache-dir --only-binary=:all: -v greenlet==3.0.3 \
 || pip install --no-cache-dir -v greenlet==3.0.3

# install the rest (verbose so Actions log shows the failing pkg if any)
RUN pip install --no-cache-dir -v -r requirements.txt

# app code
COPY app.py .

# run as non-root
RUN useradd -u 1000 -m appuser
USER appuser

ENV REDIS_HOST=redis \
    REDIS_PORT=6379 \
    LATEST_HASH_KEY=latest:ohlc \
    USE_STREAM=0 \
    STREAM_NAME=ticks.v1 \
    STREAM_MAXLEN=100000

EXPOSE 5000
CMD ["python", "app.py"]
