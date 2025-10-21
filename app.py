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
        # Tillåt både str och dict
        if isinstance(data, str):
            data = json.loads(data)

        security_id = str(data.get("security_id") or "").strip()
        if not security_id:
            print("[VALIDATION] FAIL: security_id saknas")
            socketio.emit("response_event", {"status": "Error", "message": "security_id saknas"})
            return

        # Hämta rå-payload
        payload = data.get("data")
        if payload is None:
            socketio.emit("response_event", {"status": "Error", "message": "saknar 'data'"})
            return

        # Om extensionen skickar sträng → parsa
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                socketio.emit("response_event", {"status": "Error", "message": "data ej giltig JSON"})
                return

        # Plocka ut lista med datapunkter oavsett format (ohlc | data | dataPoints)
        points = []
        if isinstance(payload, dict):
            if isinstance(payload.get("ohlc"), list):
                points = payload["ohlc"]
            elif isinstance(payload.get("data"), list):
                points = payload["data"]
            elif isinstance(payload.get("dataPoints"), list):
                # mappa dataPoints → OHLC-ish
                for p in payload["dataPoints"]:
                    if not isinstance(p, dict):
                        continue
                    ts = p.get("t") or p.get("timestamp")
                    if ts is None:
                        continue
                    c = p.get("c") or p.get("close") or p.get("value")
                    o = p.get("o") or p.get("open")  or c
                    h = p.get("h") or p.get("high")  or c
                    l = p.get("l") or p.get("low")   or c
                    points.append({
                        "timestamp": int(ts),
                        "open": float(o) if o is not None else None,
                        "high": float(h) if h is not None else None,
                        "low":  float(l) if l is not None else None,
                        "close":float(c) if c is not None else None,
                        "volume": p.get("volume"),
                    })
        elif isinstance(payload, list):
            points = payload
        else:
            socketio.emit("response_event", {"status": "Error", "message": "okänt payloadformat"})
            return

        # Filtrera bort skräp och säkerställ timestamp
        clean = []
        for b in points:
            if not isinstance(b, dict):
                continue
            ts = b.get("timestamp")
            if ts is None:
                continue
            try:
                ts = int(ts)
            except Exception:
                continue
            clean.append((ts, b))

        if not clean:
            socketio.emit("response_event", {"status": "Error", "message": "inga datapunkter med timestamp"})
            return

        # Redis-nycklar
        hkey = f"h:ohlc:{security_id}"
        zkey = f"z:ohlc:{security_id}"

        # Batcha skrivningarna
        pipe = r.pipeline()
        inserted_new = 0
        for ts, obj in clean:
            # värde = exakt JSON för punkten
            raw_point = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
            # Hash: fält=timestamp → value = json (ersätter om redan finns)
            pipe.hset(hkey, str(ts), raw_point)
            # ZSET-index: member=timestamp (str), score=timestamp (NX för att undvika dubbletter)
            pipe.zadd(zkey, {str(ts): ts}, nx=True)
        # Lägg in instrument i indexset
        pipe.sadd("idx:ohlc", security_id)

        res = pipe.execute()

        # Räkna hur många nya timestamps som lades till i ZSET (zadd returnerar 1/0)
        # Notera: res = [hset, zadd, hset, zadd, ..., sadd]
        for i in range(1, len(clean)*2, 2):
            if isinstance(res[i], (int, float)) and res[i] > 0:
                inserted_new += 1

        first_ts = min(ts for ts, _ in clean)
        last_ts  = max(ts for ts, _ in clean)

        # Ack
        ack = {
            "status": "Success",
            "message": "Inskrev datapunkter",
            "security_id": security_id,
            "received_points": len(clean),
            "inserted_new": inserted_new,  # nya timestamps (ej dubbletter)
            "first_ts": first_ts,
            "last_ts": last_ts,
        }
        socketio.emit("response_event", ack)
        print(f"[REDIS] HSET→{hkey} ZADD→{zkey} points={len(clean)} new={inserted_new}")
        return ack

    except RedisError as e:
        print(f"[REDIS] FAIL: {e}")
        socketio.emit("response_event", {"status": "Error", "message": "Redis fail"})
    except Exception as e:
        print(f"[EXCEPTION] {e}")
        socketio.emit("response_event", {"status": "Error", "message": str(e)})




# === Main ===
if __name__ == "__main__":
    print(f"Starting Flask WS on 0.0.0.0:5000 (REDIS={REDIS_HOST}:{REDIS_PORT}, USE_STREAM={USE_STREAM})")
    socketio.run(app, host="0.0.0.0", port=5000)
