import os, json
from flask import Flask
from flask_socketio import SocketIO
from redis import Redis, RedisError

# === Konfiguration ===
REDIS_HOST   = os.getenv("REDIS_HOST", "redis")
REDIS_PORT   = int(os.getenv("REDIS_PORT", "6379"))
LATEST_HASH  = os.getenv("LATEST_HASH_KEY", "latest:ohlc")
USE_STREAM   = os.getenv("USE_STREAM", "0") == "1"
STREAM_NAME  = os.getenv("STREAM_NAME", "ticks.v1")
STREAM_MAXLEN= int(os.getenv("STREAM_MAXLEN", "100000"))

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# === Redis-klient (kan vara ned) ===
r = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

@app.get("/health")
def health():
    try:
        r.ping()
        return "ok", 200
    except Exception as e:
        return f"redis down: {e}", 503

@app.get("/")
def index():
    return "WebSocket-gateway up", 200

# === Hjälpfunktioner ===
def extract_list(data: dict):
    data_type = data.get("id")         # "today" eller "ta50"
    payload   = data.get("data")
    if not data_type or payload is None:
        raise ValueError("saknar id eller data")

    # Om extensionen skickar en sträng → parsa
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            raise ValueError("data är sträng men inte giltig JSON")

    # Normalisera till en lista av OHLC-diktar
    items = None
    if isinstance(payload, dict):
        if isinstance(payload.get("ohlc"), list):
            items = payload["ohlc"]
        elif isinstance(payload.get("data"), list):
            items = payload["data"]
    elif isinstance(payload, list):
        items = payload

    if not isinstance(items, list):
        raise ValueError(f"okänt payloadformat för id={data_type} (typ={type(payload).__name__})")

    return data_type, items


def latest_item(items: list):
    items = [x for x in items if isinstance(x, dict) and "timestamp" in x]
    return max(items, key=lambda x: x["timestamp"]) if items else None


def normalize_latest(security_id: str, src_id: str, obj: dict):
    return {
        "security_id": security_id,
        "source_id": src_id,
        "timestamp": obj.get("timestamp"),
        "open":  obj.get("open"),
        "high":  obj.get("high"),
        "low":   obj.get("low"),
        "close": obj.get("close"),
        "volume": obj.get("volume"),
    }


# === WebSocket-handler ===
@socketio.on("custom_event")
def handle_custom_event(data):
    print("\n=== [EVENT IN] custom_event ===")
    try:
        if isinstance(data, str):
            data = json.loads(data)
        dt = type(data.get("data")).__name__
        print(f"[DEBUG] payload type: {dt}")
    except json.JSONDecodeError as e:
        print(f"[PARSE] FAIL: Bad JSON ({e})")
        socketio.emit("response_event", {"status": "Error", "message": "Bad JSON"})
        return

    security_id = data.get("security_id")
    print(f"[INFO] security_id={security_id}")
    if not security_id:
        print("[VALIDATION] FAIL: security_id saknas")
        socketio.emit("response_event", {"status": "Error", "message": "security_id saknas"})
        return

    try:
        src_id, items = extract_list(data)
        last = latest_item(items)
        if not last:
            print("[EXTRACT] FAIL: ingen giltig datapunkt")
            socketio.emit("response_event", {"status": "Error", "message": "ingen giltig datapunkt"})
            return

        latest_doc = normalize_latest(security_id, src_id, last)
        payload_json = json.dumps(latest_doc)

        # 1) skriv senaste till hash
        try:
            r.hset(LATEST_HASH, security_id, payload_json)
            print(f"[REDIS] OK hash HSET {LATEST_HASH}[{security_id}]")
        except RedisError as e:
            print(f"[REDIS] FAIL hash: {e}")

        # 2) valfritt stream
        if USE_STREAM:
            try:
                r.xadd(STREAM_NAME, {"payload": payload_json}, maxlen=STREAM_MAXLEN)
                print(f"[REDIS] OK stream XADD {STREAM_NAME}")
            except RedisError as e:
                print(f"[REDIS] FAIL stream: {e}")

        socketio.emit("response_event", {
            "status": "Success",
            "message": "Senaste datapunkt uppdaterad",
            "security_id": security_id,
            "timestamp": latest_doc["timestamp"]
        })
        print("[DONE] response_event sent (Success)")

    except Exception as e:
        print(f"[EXCEPTION] {e}")
        socketio.emit("response_event", {"status": "Error", "message": str(e)})


# === Main ===
if __name__ == "__main__":
    print(f"Starting Flask WS on 0.0.0.0:5000 (REDIS={REDIS_HOST}:{REDIS_PORT}, USE_STREAM={USE_STREAM})")
    socketio.run(app, host="0.0.0.0", port=5000)
