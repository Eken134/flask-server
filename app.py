import os, json, time, logging
from logging.handlers import RotatingFileHandler
from flask import Flask
from flask_socketio import SocketIO
from redis import Redis, RedisError

# === Konfiguration ===
REDIS_HOST   = os.getenv("REDIS_HOST", "redis")
REDIS_PORT   = int(os.getenv("REDIS_PORT", "6379"))
LATEST_HASH = os.getenv("LATEST_HASH_KEY", "latest:ohlc")

USE_STREAM    = os.getenv("USE_STREAM", "0") == "1"
STREAM_NAME   = os.getenv("STREAM_NAME", "ticks.v1")
STREAM_MAXLEN = int(os.getenv("STREAM_MAXLEN", "100000"))

# --- Logging ---
LOG_LEVEL          = os.getenv("LOG_LEVEL", "INFO").upper()        # DEBUG/INFO/WARNING/ERROR
LOG_SAMPLE_EVERY   = int(os.getenv("LOG_SAMPLE_EVERY", "200"))     # logga vart n:e WS-event
LOG_PREVIEW_CHARS  = int(os.getenv("LOG_PREVIEW_CHARS", "300"))    # trunkera stora strängar
LOG_TO_FILE        = os.getenv("LOG_TO_FILE", "0") == "1"          # valfritt

logger = logging.getLogger("ws-gateway")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
logger.addHandler(_sh)

if LOG_TO_FILE:
    _fh = RotatingFileHandler("gateway.log", maxBytes=10_000_000, backupCount=3)
    _fh.setFormatter(_fmt)
    logger.addHandler(_fh)

app = Flask(__name__)
# Stäng av SocketIO:s egna verbositet
socketio = SocketIO(app, cors_allowed_origins="*", logger=False, engineio_logger=False)

# === Redis-klient ===
r = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# --- enkel sampler för WS-event ---
_event_counter = 0
def should_log_verbose() -> bool:
    global _event_counter
    _event_counter += 1
    return (_event_counter % LOG_SAMPLE_EVERY) == 0

@app.get("/health")
def health():
    try:
        r.ping()
        return "ok", 200
    except Exception as e:
        logger.warning("Redis down: %s", e)
        return f"redis down: {e}", 503

@app.get("/")
def index():
    return "WebSocket-gateway up", 200

def extract_list(data: dict):
    data_type = data.get("id")
    payload   = data.get("data")
    if not data_type or payload is None:
        raise ValueError("saknar id eller data")

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            raise ValueError("data är sträng men inte giltig JSON")

    items = None
    if isinstance(payload, dict):
        if isinstance(payload.get("ohlc"), list):
            items = payload["ohlc"]
        elif isinstance(payload.get("data"), list):
            items = payload["data"]
        elif isinstance(payload.get("dataPoints"), list):
            tmp = []
            for p in payload["dataPoints"]:
                ts = p.get("t") or p.get("timestamp")
                c  = p.get("c") or p.get("close") or p.get("value")
                if ts and c is not None:
                    tmp.append({
                        "timestamp": int(ts),
                        "open":  float(p.get("o") or p.get("open")  or c),
                        "high":  float(p.get("h") or p.get("high")  or c),
                        "low":   float(p.get("l") or p.get("low")   or c),
                        "close": float(c),
                        "volume": p.get("volume"),
                    })
            items = tmp
    elif isinstance(payload, list):
        items = payload

    if not isinstance(items, list):
        raise ValueError(f"okänt payloadformat för id={data_type} (typ={type(payload).__name__})")

    return data_type, items

def latest_item(items: list):
    items = [x for x in items if isinstance(x, dict) and "timestamp" in x and "close" in x]
    return max(items, key=lambda x: x["timestamp"]) if items else None

def normalize_bar(security_id: str, src_id: str, obj: dict):
    return {
        "security_id": security_id,
        "source_id":   src_id,
        "timestamp":   int(obj.get("timestamp")),
        "open":  float(obj.get("open",  obj.get("close"))),
        "high":  float(obj.get("high",  obj.get("close"))),
        "low":   float(obj.get("low",   obj.get("close"))),
        "close": float(obj.get("close")),
        "volume": obj.get("volume"),
    }

@socketio.on("custom_event")
def handle_custom_event(data):
    verbose = should_log_verbose()

    try:
        if isinstance(data, str):
            data = json.loads(data)
    except json.JSONDecodeError as e:
        logger.warning("Bad JSON in incoming event: %s", e)
        socketio.emit("response_event", {"status": "Error", "message": "Bad JSON"})
        return

    security_id = str(data.get("security_id") or "")
    if not security_id:
        if verbose:
            logger.info("Validation fail: security_id saknas")
        socketio.emit("response_event", {"status": "Error", "message": "security_id saknas"})
        return

    if verbose and logger.isEnabledFor(logging.DEBUG):
        raw = data.get("data")
        if isinstance(raw, str):
            preview = raw[:LOG_PREVIEW_CHARS].replace("\n", " ")
            if len(raw) > LOG_PREVIEW_CHARS:
                preview += " ..."
            logger.debug("payload type=str, bytes=%s, preview=%s", len(raw), preview)
        else:
            logger.debug("payload type=%s", type(raw).__name__)

    try:
        src_id, items = extract_list(data)
        n = len(items) if isinstance(items, list) else 0
        first_ts = items[0].get("timestamp") if n else None
        last_ts  = items[-1].get("timestamp") if n else None
        if verbose:
            logger.info("IN %s items=%s first=%s last=%s sec=%s",
                        src_id, n, first_ts, last_ts, security_id)

        last = latest_item(items)
        if not last:
            if verbose:
                logger.info("EXTRACT fail: ingen giltig datapunkt")
            socketio.emit("response_event", {"status": "Error", "message": "ingen giltig datapunkt"})
            return

        latest_doc = normalize_bar(security_id, src_id, last)
        payload_json = json.dumps(latest_doc, ensure_ascii=False)

        redis_ok = True
        try:
            r.hset(LATEST_HASH, security_id, payload_json)
        except RedisError as e:
            redis_ok = False
            if verbose:
                logger.warning("Redis HSET fail: %s", e)

        zadd_written = 0
        try:
            zkey = f"z:ohlc:{security_id}"
            pipe = r.pipeline()
            for b in items:
                if not isinstance(b, dict) or "timestamp" not in b or "close" not in b:
                    continue
                nb = normalize_bar(security_id, src_id, b)
                pipe.zadd(zkey, {json.dumps(nb, ensure_ascii=False): nb["timestamp"]}, nx=True)
            pipe.sadd("idx:ohlc", security_id)
            res = pipe.execute()
            zadd_written = sum(1 for x in res[:-1] if isinstance(x, (int, float)) and x > 0)
        except RedisError as e:
            redis_ok = False
            if verbose:
                logger.warning("Redis ZADD fail: %s", e)

        if USE_STREAM:
            try:
                r.xadd(STREAM_NAME, {"payload": payload_json}, maxlen=STREAM_MAXLEN)
            except RedisError as e:
                redis_ok = False
                if verbose:
                    logger.warning("Redis XADD fail: %s", e)

        ack = {
            "status": "Success" if redis_ok else "Partial",
            "message": "Senaste datapunkt uppdaterad" if redis_ok else "Mottog data, men Redis-skrivning misslyckades",
            "security_id": security_id,
            "bars_received": n,
            "first_ts": first_ts,
            "last_ts": latest_doc["timestamp"],
            "zadd_written": zadd_written,
            "redis_ok": redis_ok,
            "ts_server": int(time.time() * 1000),
        }
        # Låt acket alltid gå — men logga bara ibland
        if verbose:
            logger.info("OUT ack=%s", {k: ack[k] for k in ("status","security_id","bars_received","zadd_written","redis_ok")})
        socketio.emit("response_event", ack)
        return ack

    except Exception as e:
        logger.exception("Unhandled exception in custom_event: %s", e)
        socketio.emit("response_event", {"status": "Error", "message": str(e)})

if __name__ == "__main__":
    logger.info("Starting Flask WS on 0.0.0.0:5000 (REDIS=%s:%s, USE_STREAM=%s, LOG_LEVEL=%s, SAMPLE_EVERY=%s)",
                REDIS_HOST, REDIS_PORT, USE_STREAM, LOG_LEVEL, LOG_SAMPLE_EVERY)
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
