#!/usr/bin/env python3
# dhan_commodity_bot.py (patched: safe header handling + startup diagnostics + poller)
# Sends LTP snapshot every POLL_INTERVAL seconds. WebSocket optional.

import os, time, json, threading, traceback, urllib.parse
import requests

try:
    import websocket
    WS_AVAILABLE = True
except Exception:
    WS_AVAILABLE = False

# ---------------- CONFIG (env) ----------------
CLIENT_ID = (os.getenv("DHAN_CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("DHAN_ACCESS_TOKEN") or "").strip()
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

WS_URL_RAW = (os.getenv("WS_URL") or "wss://api-feed.dhan.co").strip()
WS_AUTH_MODE = (os.getenv("WS_AUTH_MODE") or "auto").strip().lower()  # auto / header / query
SYMBOLS = (os.getenv("SYMBOLS") or "GOLD,SILVER,CRUDEOIL").strip()
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL") or 60)
DISABLE_WS = (os.getenv("DISABLE_WS") or "").strip().lower() in ("1","true","yes")
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY") or 5)

DHAN_LTP_ENDPOINT = (os.getenv("DHAN_LTP_ENDPOINT") or "https://api.dhan.co/v2/marketfeed/ltp").strip()
INSTRUMENT_CSV_URL = (os.getenv("INSTRUMENT_CSV_URL") or "https://images.dhan.co/api-data/api-scrip-master-detailed.csv").strip()

# ---------------- Sample mapping: SYMBOL -> SECURITY_ID ----------------
COMMODITY_IDS = {
    "GOLD": "5001",
    "SILVER": "5002",
    "CRUDEOIL": "5003",
    "NATGAS": "5004",
}

# ---------------- Telegram helper ----------------
TELEGRAM_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"
def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[telegram skipped] token/chat not configured. Message would be:\n", text)
        return False
    try:
        r = requests.post(
            TELEGRAM_SEND_URL.format(token=TELEGRAM_BOT_TOKEN),
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
        r.raise_for_status()
        print("[telegram] sent")
        return True
    except Exception as e:
        print("[telegram error]", e, getattr(e, "response", None))
        return False

# Startup telemetry
print("BOOT: starting dhan_commodity_bot")
print("ENV: CLIENT_ID set?", bool(CLIENT_ID), " ACCESS_TOKEN set?", bool(ACCESS_TOKEN))
print("ENV: TELEGRAM configured?", bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID))
try:
    send_telegram(f"🔄 Dhan commodity bot starting. POLL_INTERVAL={POLL_INTERVAL}s, SYMBOLS={SYMBOLS}")
except Exception:
    pass

# ---------------- Utilities ----------------
def get_security_id(symbol: str):
    return COMMODITY_IDS.get(symbol.upper())

def build_security_payload(symbols):
    seg_map = {}
    for s in symbols:
        sid = get_security_id(s)
        if not sid:
            print("[poller-warn] security id not found for symbol:", s)
            continue
        seg = "NSE_EQ"
        try:
            seg_map.setdefault(seg, []).append(int(sid))
        except Exception:
            seg_map.setdefault(seg, []).append(sid)
    return seg_map

def format_poll_message(json_resp):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    if not json_resp:
        return f"⏱ {now} — Poller: no response from Dhan"
    data = json_resp.get("data") if isinstance(json_resp, dict) else json_resp
    if not data:
        data = json_resp
    lines = []
    if isinstance(data, dict):
        for seg, bucket in data.items():
            if not isinstance(bucket, dict):
                continue
            for secid, info in bucket.items():
                if not isinstance(info, dict):
                    continue
                ltp = info.get('last_price') or info.get('ltp') or info.get('lastPrice') or info.get('price')
                if ltp is None:
                    continue
                readable = None
                for k, v in COMMODITY_IDS.items():
                    if str(v) == str(secid):
                        readable = k
                        break
                lines.append(f"<b>{readable or secid}</b>  LTP: <code>{ltp}</code>  ({seg})")
    if not lines:
        return f"⏱ {now} — Poller: response received but no LTP fields found (see logs)"
    return f"⏱ {now} — Poller snapshot:\n" + "\n".join(lines)

def call_dhan_ltp(payload, retries=2):
    headers = {
        "access-token": ACCESS_TOKEN or "",
        "client-id": CLIENT_ID or "",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    for attempt in range(1, retries+1):
        try:
            r = requests.post(DHAN_LTP_ENDPOINT, json=payload, headers=headers, timeout=12)
            print(f"[poller] HTTP {r.status_code} (attempt {attempt})")
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception as e:
                    print("[poller] JSON parse failed:", e)
                    return None
            else:
                snippet = (r.text[:600] + "...") if r.text and len(r.text) > 600 else (r.text or "")
                # If telegram configured, send diag; else print
                if not send_telegram(f"[poller diagnostic] HTTP {r.status_code} attempt {attempt}\nBody: {snippet}"):
                    print("[poller diagnostic] HTTP", r.status_code, "Body:", snippet)
        except Exception as e:
            print("[poller] request exception:", e)
        time.sleep(1)
    return None

class Poller(threading.Thread):
    def __init__(self, symbols, interval=POLL_INTERVAL):
        super().__init__(daemon=True)
        self.symbols = [s.strip() for s in symbols.split(',') if s.strip()]
        self.interval = interval

    def run(self):
        # try CSV fallback once if mapping incomplete
        payload_map = build_security_payload(self.symbols)
        if not payload_map:
            try:
                r = requests.get(INSTRUMENT_CSV_URL, timeout=15); r.raise_for_status()
                from csv import DictReader
                reader = DictReader(r.text.splitlines())
                csv_map = {}
                for row in reader:
                    name = (row.get('SM_SYMBOL_NAME') or row.get('TRADING_SYMBOL') or row.get('SYMBOL') or '').strip().upper()
                    sid = row.get('SECURITY_ID') or row.get('SM_INSTRUMENT_ID') or row.get('EXCH_TOKEN')
                    if name and sid: csv_map[name] = sid
                for s in self.symbols:
                    found = csv_map.get(s.strip().upper())
                    if found:
                        COMMODITY_IDS[s.strip().upper()] = found
                        print("[poller] patched mapping", s, "->", found)
            except Exception as e:
                print("CSV fallback failed:", e)

        while True:
            try:
                payload = build_security_payload(self.symbols)
                print("[poller] payload:", payload)
                resp = call_dhan_ltp(payload)
                msg = format_poll_message(resp)
                # always try to send snapshot
                if not send_telegram(msg):
                    print(msg)
            except Exception as e:
                print("Poller exception:", e)
            time.sleep(self.interval)

# ---------------- WebSocket client ----------------
def build_effective_ws_url_and_headers():
    """
    Return (ws_url, headers_list or None, use_headers_bool)
    Only include header entries if value non-empty.
    """
    base = "wss://api-feed.dhan.co"
    # If user provided the official host or query, prefer it
    if WS_URL_RAW.startswith("wss://api-feed.dhan.co"):
        if '?' in WS_URL_RAW:
            return WS_URL_RAW, None, False
        # if raw host and WS_AUTH_MODE == query, build query
        if WS_AUTH_MODE == "query" and ACCESS_TOKEN and CLIENT_ID:
            q = {"version":"2","token":ACCESS_TOKEN,"clientId":CLIENT_ID,"authType":"2"}
            return base + "?" + urllib.parse.urlencode(q), None, False
        # else header mode if tokens present
        headers = []
        if ACCESS_TOKEN: headers.append(f"access-token: {ACCESS_TOKEN}")
        if CLIENT_ID: headers.append(f"client-id: {CLIENT_ID}")
        return WS_URL_RAW, headers if headers else None, bool(headers)
    # If placeholder or unknown host: build recommended
    if "dhan.websocket.endpoint" in WS_URL_RAW or "placeholder" in WS_URL_RAW or WS_URL_RAW.endswith("/marketfeed"):
        if WS_AUTH_MODE == "header":
            headers = []
            if ACCESS_TOKEN: headers.append(f"access-token: {ACCESS_TOKEN}")
            if CLIENT_ID: headers.append(f"client-id: {CLIENT_ID}")
            return base, headers if headers else None, bool(headers)
        # default try query if we have both token+client
        if ACCESS_TOKEN and CLIENT_ID:
            q = {"version":"2","token":ACCESS_TOKEN,"clientId":CLIENT_ID,"authType":"2"}
            return base + "?" + urllib.parse.urlencode(q), None, False
        else:
            # neither complete -> return base and headers if any non-empty, else None
            headers = []
            if ACCESS_TOKEN: headers.append(f"access-token: {ACCESS_TOKEN}")
            if CLIENT_ID: headers.append(f"client-id: {CLIENT_ID}")
            return base, headers if headers else None, bool(headers)
    # fallback: if https provided convert to wss
    if WS_URL_RAW.startswith("https://"):
        wsurl = "wss://" + WS_URL_RAW[len("https://"):]
        headers = []
        if ACCESS_TOKEN: headers.append(f"access-token: {ACCESS_TOKEN}")
        if CLIENT_ID: headers.append(f"client-id: {CLIENT_ID}")
        return wsurl, headers if headers else None, bool(headers)
    headers = []
    if ACCESS_TOKEN: headers.append(f"access-token: {ACCESS_TOKEN}")
    if CLIENT_ID: headers.append(f"client-id: {CLIENT_ID}")
    return WS_URL_RAW, headers if headers else None, bool(headers)

def build_subscribe_payload(symbols):
    out=[]
    for s in symbols:
        sid = get_security_id(s)
        if not sid:
            print("[ws-warn] missing id for", s); continue
        out.append({"segment":"NSE_EQ","instrument_id": str(sid)})
    return {"action":"subscribe","instruments": out}

class DhanWS:
    def __init__(self, url, headers=None, use_headers=False, symbols=SYMBOLS):
        self.url = url
        self.headers = headers
        self.use_headers = use_headers
        self.symbols = [s.strip() for s in symbols.split(',') if s.strip()]
        self.ws = None
        self.last = {}

    def on_open(self, ws):
        print("WS opened - sending optional JSON auth + subscribe")
        try:
            # safe: also send JSON auth even if header-auth is used
            ws.send(json.dumps({"action":"auth","client_id": CLIENT_ID, "access_token": ACCESS_TOKEN}))
        except Exception:
            pass
        try:
            ws.send(json.dumps(build_subscribe_payload(self.symbols)))
            print("Subscribe sent")
        except Exception as e:
            print("Subscribe send failed:", e)

    def on_message(self, ws, message):
        try:
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            data = json.loads(message)
        except Exception as e:
            print("WS msg parse fail:", e, str(message)[:200])
            return
        ltp=None; label=None
        if isinstance(data, dict):
            payload = data.get('data') or data
            if isinstance(payload, dict):
                for seg, bucket in payload.items():
                    if isinstance(bucket, dict):
                        for instid, info in bucket.items():
                            if isinstance(info, dict):
                                ltp = info.get('last_price') or info.get('ltp') or info.get('lastPrice')
                                if ltp is not None:
                                    label = instid; break
                        if ltp is not None: break
            if ltp is None:
                ltp = data.get('ltp') or data.get('last_price') or data.get('price')
                label = data.get('symbol') or data.get('instrument') or label
        if ltp is not None:
            readable = label
            for k,v in COMMODITY_IDS.items():
                if str(v) == str(label):
                    readable = k; break
            key = f"{readable}:{label}"
            prev = self.last.get(key)
            self.last[key] = ltp
            text = f"⏱ {time.strftime('%Y-%m-%d %H:%M:%S')} — <b>{readable}</b> LTP: <code>{ltp}</code>"
            if prev != ltp:
                send_telegram(text); print("Sent ws alert:", text)

    def on_error(self, ws, err):
        print("WS error:", err)
        errstr = str(err)
        if "Name or service not known" in errstr or "getaddrinfo" in errstr:
            send_telegram("[ws diagnostic] DNS or host resolution failed for WS URL: " + str(self.url))
        if "Client-id is missing" in errstr or "Client-id is missing or contains only whitespace" in errstr:
            send_telegram("[ws diagnostic] WS handshake failed: client-id missing or blank. Check DHAN_CLIENT_ID env var.")
        # let outer loop reconnect

    def on_close(self, ws, code, reason):
        print("WS closed:", code, reason)

    def run_forever(self):
        while True:
            try:
                if self.use_headers and self.headers:
                    print("Connecting WS (header-auth) to:", self.url, " headers:", bool(self.headers))
                    self.ws = websocket.WebSocketApp(self.url,
                                                     header=self.headers,
                                                     on_open=self.on_open,
                                                     on_message=self.on_message,
                                                     on_error=self.on_error,
                                                     on_close=self.on_close)
                else:
                    print("Connecting WS (url-auth/plain) to:", self.url)
                    self.ws = websocket.WebSocketApp(self.url,
                                                     on_open=self.on_open,
                                                     on_message=self.on_message,
                                                     on_error=self.on_error,
                                                     on_close=self.on_close)
                self.ws.run_forever(ping_interval=25, ping_timeout=10)
            except Exception as e:
                print("Exception in WS loop:", e)
                send_telegram("[ws diagnostic] Exception while connecting to WS: " + str(e)[:400])
            print(f"Reconnecting to WS in {RECONNECT_DELAY}s...")
            time.sleep(RECONNECT_DELAY)

# ---------------- Launcher ----------------
if __name__ == "__main__":
    print("Starting commodity bot. Symbols:", SYMBOLS)
    # Always run poller thread (fallback)
    poller = Poller(SYMBOLS, interval=POLL_INTERVAL)
    poller.start()

    if DISABLE_WS:
        print("DISABLE_WS is set -> running poller-only")
        send_telegram("⚠️ WS disabled - running poller-only.")
        try:
            while True:
                time.sleep(600)
        except KeyboardInterrupt:
            print("Stopping poller-only.")
            raise SystemExit(0)

    # Build ws url and headers intelligently
    effective_url, headers, use_headers = build_effective_ws_url_and_headers()

    # If CLIENT_ID missing or ACCESS_TOKEN missing -> notify and run poller-only
    if not CLIENT_ID or not ACCESS_TOKEN:
        msg = ("[credential warning] DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN missing/empty. "
               "WS/REST will fail until valid credentials are set. Running poller attempts (will show errors).")
        print(msg)
        send_telegram(msg)
        # run poller-only (poller already started); keep main alive
        try:
            while True:
                time.sleep(600)
        except KeyboardInterrupt:
            raise SystemExit(0)

    # Start WS client
    wsclient = DhanWS(effective_url, headers=headers, use_headers=use_headers, symbols=SYMBOLS)
    try:
        wsclient.run_forever()
    except KeyboardInterrupt:
        print("Exiting")
    except Exception as e:
        print("Fatal WS error:", e)
        send_telegram("[ws diagnostic] Fatal WS error: " + str(e)[:400])
#!/usr/bin/env python3
# dhan_commodity_bot.py (patched: safe header handling + startup diagnostics + poller)
# Sends LTP snapshot every POLL_INTERVAL seconds. WebSocket optional.

import os, time, json, threading, traceback, urllib.parse
import requests

try:
    import websocket
    WS_AVAILABLE = True
except Exception:
    WS_AVAILABLE = False

# ---------------- CONFIG (env) ----------------
CLIENT_ID = (os.getenv("DHAN_CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("DHAN_ACCESS_TOKEN") or "").strip()
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

WS_URL_RAW = (os.getenv("WS_URL") or "wss://api-feed.dhan.co").strip()
WS_AUTH_MODE = (os.getenv("WS_AUTH_MODE") or "auto").strip().lower()  # auto / header / query
SYMBOLS = (os.getenv("SYMBOLS") or "GOLD,SILVER,CRUDEOIL").strip()
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL") or 60)
DISABLE_WS = (os.getenv("DISABLE_WS") or "").strip().lower() in ("1","true","yes")
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY") or 5)

DHAN_LTP_ENDPOINT = (os.getenv("DHAN_LTP_ENDPOINT") or "https://api.dhan.co/v2/marketfeed/ltp").strip()
INSTRUMENT_CSV_URL = (os.getenv("INSTRUMENT_CSV_URL") or "https://images.dhan.co/api-data/api-scrip-master-detailed.csv").strip()

# ---------------- Sample mapping: SYMBOL -> SECURITY_ID ----------------
COMMODITY_IDS = {
    "GOLD": "5001",
    "SILVER": "5002",
    "CRUDEOIL": "5003",
    "NATGAS": "5004",
}

# ---------------- Telegram helper ----------------
TELEGRAM_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"
def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[telegram skipped] token/chat not configured. Message would be:\n", text)
        return False
    try:
        r = requests.post(
            TELEGRAM_SEND_URL.format(token=TELEGRAM_BOT_TOKEN),
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
        r.raise_for_status()
        print("[telegram] sent")
        return True
    except Exception as e:
        print("[telegram error]", e, getattr(e, "response", None))
        return False

# Startup telemetry
print("BOOT: starting dhan_commodity_bot")
print("ENV: CLIENT_ID set?", bool(CLIENT_ID), " ACCESS_TOKEN set?", bool(ACCESS_TOKEN))
print("ENV: TELEGRAM configured?", bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID))
try:
    send_telegram(f"🔄 Dhan commodity bot starting. POLL_INTERVAL={POLL_INTERVAL}s, SYMBOLS={SYMBOLS}")
except Exception:
    pass

# ---------------- Utilities ----------------
def get_security_id(symbol: str):
    return COMMODITY_IDS.get(symbol.upper())

def build_security_payload(symbols):
    seg_map = {}
    for s in symbols:
        sid = get_security_id(s)
        if not sid:
            print("[poller-warn] security id not found for symbol:", s)
            continue
        seg = "NSE_EQ"
        try:
            seg_map.setdefault(seg, []).append(int(sid))
        except Exception:
            seg_map.setdefault(seg, []).append(sid)
    return seg_map

def format_poll_message(json_resp):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    if not json_resp:
        return f"⏱ {now} — Poller: no response from Dhan"
    data = json_resp.get("data") if isinstance(json_resp, dict) else json_resp
    if not data:
        data = json_resp
    lines = []
    if isinstance(data, dict):
        for seg, bucket in data.items():
            if not isinstance(bucket, dict):
                continue
            for secid, info in bucket.items():
                if not isinstance(info, dict):
                    continue
                ltp = info.get('last_price') or info.get('ltp') or info.get('lastPrice') or info.get('price')
                if ltp is None:
                    continue
                readable = None
                for k, v in COMMODITY_IDS.items():
                    if str(v) == str(secid):
                        readable = k
                        break
                lines.append(f"<b>{readable or secid}</b>  LTP: <code>{ltp}</code>  ({seg})")
    if not lines:
        return f"⏱ {now} — Poller: response received but no LTP fields found (see logs)"
    return f"⏱ {now} — Poller snapshot:\n" + "\n".join(lines)

def call_dhan_ltp(payload, retries=2):
    headers = {
        "access-token": ACCESS_TOKEN or "",
        "client-id": CLIENT_ID or "",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    for attempt in range(1, retries+1):
        try:
            r = requests.post(DHAN_LTP_ENDPOINT, json=payload, headers=headers, timeout=12)
            print(f"[poller] HTTP {r.status_code} (attempt {attempt})")
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception as e:
                    print("[poller] JSON parse failed:", e)
                    return None
            else:
                snippet = (r.text[:600] + "...") if r.text and len(r.text) > 600 else (r.text or "")
                # If telegram configured, send diag; else print
                if not send_telegram(f"[poller diagnostic] HTTP {r.status_code} attempt {attempt}\nBody: {snippet}"):
                    print("[poller diagnostic] HTTP", r.status_code, "Body:", snippet)
        except Exception as e:
            print("[poller] request exception:", e)
        time.sleep(1)
    return None

class Poller(threading.Thread):
    def __init__(self, symbols, interval=POLL_INTERVAL):
        super().__init__(daemon=True)
        self.symbols = [s.strip() for s in symbols.split(',') if s.strip()]
        self.interval = interval

    def run(self):
        # try CSV fallback once if mapping incomplete
        payload_map = build_security_payload(self.symbols)
        if not payload_map:
            try:
                r = requests.get(INSTRUMENT_CSV_URL, timeout=15); r.raise_for_status()
                from csv import DictReader
                reader = DictReader(r.text.splitlines())
                csv_map = {}
                for row in reader:
                    name = (row.get('SM_SYMBOL_NAME') or row.get('TRADING_SYMBOL') or row.get('SYMBOL') or '').strip().upper()
                    sid = row.get('SECURITY_ID') or row.get('SM_INSTRUMENT_ID') or row.get('EXCH_TOKEN')
                    if name and sid: csv_map[name] = sid
                for s in self.symbols:
                    found = csv_map.get(s.strip().upper())
                    if found:
                        COMMODITY_IDS[s.strip().upper()] = found
                        print("[poller] patched mapping", s, "->", found)
            except Exception as e:
                print("CSV fallback failed:", e)

        while True:
            try:
                payload = build_security_payload(self.symbols)
                print("[poller] payload:", payload)
                resp = call_dhan_ltp(payload)
                msg = format_poll_message(resp)
                # always try to send snapshot
                if not send_telegram(msg):
                    print(msg)
            except Exception as e:
                print("Poller exception:", e)
            time.sleep(self.interval)

# ---------------- WebSocket client ----------------
def build_effective_ws_url_and_headers():
    """
    Return (ws_url, headers_list or None, use_headers_bool)
    Only include header entries if value non-empty.
    """
    base = "wss://api-feed.dhan.co"
    # If user provided the official host or query, prefer it
    if WS_URL_RAW.startswith("wss://api-feed.dhan.co"):
        if '?' in WS_URL_RAW:
            return WS_URL_RAW, None, False
        # if raw host and WS_AUTH_MODE == query, build query
        if WS_AUTH_MODE == "query" and ACCESS_TOKEN and CLIENT_ID:
            q = {"version":"2","token":ACCESS_TOKEN,"clientId":CLIENT_ID,"authType":"2"}
            return base + "?" + urllib.parse.urlencode(q), None, False
        # else header mode if tokens present
        headers = []
        if ACCESS_TOKEN: headers.append(f"access-token: {ACCESS_TOKEN}")
        if CLIENT_ID: headers.append(f"client-id: {CLIENT_ID}")
        return WS_URL_RAW, headers if headers else None, bool(headers)
    # If placeholder or unknown host: build recommended
    if "dhan.websocket.endpoint" in WS_URL_RAW or "placeholder" in WS_URL_RAW or WS_URL_RAW.endswith("/marketfeed"):
        if WS_AUTH_MODE == "header":
            headers = []
            if ACCESS_TOKEN: headers.append(f"access-token: {ACCESS_TOKEN}")
            if CLIENT_ID: headers.append(f"client-id: {CLIENT_ID}")
            return base, headers if headers else None, bool(headers)
        # default try query if we have both token+client
        if ACCESS_TOKEN and CLIENT_ID:
            q = {"version":"2","token":ACCESS_TOKEN,"clientId":CLIENT_ID,"authType":"2"}
            return base + "?" + urllib.parse.urlencode(q), None, False
        else:
            # neither complete -> return base and headers if any non-empty, else None
            headers = []
            if ACCESS_TOKEN: headers.append(f"access-token: {ACCESS_TOKEN}")
            if CLIENT_ID: headers.append(f"client-id: {CLIENT_ID}")
            return base, headers if headers else None, bool(headers)
    # fallback: if https provided convert to wss
    if WS_URL_RAW.startswith("https://"):
        wsurl = "wss://" + WS_URL_RAW[len("https://"):]
        headers = []
        if ACCESS_TOKEN: headers.append(f"access-token: {ACCESS_TOKEN}")
        if CLIENT_ID: headers.append(f"client-id: {CLIENT_ID}")
        return wsurl, headers if headers else None, bool(headers)
    headers = []
    if ACCESS_TOKEN: headers.append(f"access-token: {ACCESS_TOKEN}")
    if CLIENT_ID: headers.append(f"client-id: {CLIENT_ID}")
    return WS_URL_RAW, headers if headers else None, bool(headers)

def build_subscribe_payload(symbols):
    out=[]
    for s in symbols:
        sid = get_security_id(s)
        if not sid:
            print("[ws-warn] missing id for", s); continue
        out.append({"segment":"NSE_EQ","instrument_id": str(sid)})
    return {"action":"subscribe","instruments": out}

class DhanWS:
    def __init__(self, url, headers=None, use_headers=False, symbols=SYMBOLS):
        self.url = url
        self.headers = headers
        self.use_headers = use_headers
        self.symbols = [s.strip() for s in symbols.split(',') if s.strip()]
        self.ws = None
        self.last = {}

    def on_open(self, ws):
        print("WS opened - sending optional JSON auth + subscribe")
        try:
            # safe: also send JSON auth even if header-auth is used
            ws.send(json.dumps({"action":"auth","client_id": CLIENT_ID, "access_token": ACCESS_TOKEN}))
        except Exception:
            pass
        try:
            ws.send(json.dumps(build_subscribe_payload(self.symbols)))
            print("Subscribe sent")
        except Exception as e:
            print("Subscribe send failed:", e)

    def on_message(self, ws, message):
        try:
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            data = json.loads(message)
        except Exception as e:
            print("WS msg parse fail:", e, str(message)[:200])
            return
        ltp=None; label=None
        if isinstance(data, dict):
            payload = data.get('data') or data
            if isinstance(payload, dict):
                for seg, bucket in payload.items():
                    if isinstance(bucket, dict):
                        for instid, info in bucket.items():
                            if isinstance(info, dict):
                                ltp = info.get('last_price') or info.get('ltp') or info.get('lastPrice')
                                if ltp is not None:
                                    label = instid; break
                        if ltp is not None: break
            if ltp is None:
                ltp = data.get('ltp') or data.get('last_price') or data.get('price')
                label = data.get('symbol') or data.get('instrument') or label
        if ltp is not None:
            readable = label
            for k,v in COMMODITY_IDS.items():
                if str(v) == str(label):
                    readable = k; break
            key = f"{readable}:{label}"
            prev = self.last.get(key)
            self.last[key] = ltp
            text = f"⏱ {time.strftime('%Y-%m-%d %H:%M:%S')} — <b>{readable}</b> LTP: <code>{ltp}</code>"
            if prev != ltp:
                send_telegram(text); print("Sent ws alert:", text)

    def on_error(self, ws, err):
        print("WS error:", err)
        errstr = str(err)
        if "Name or service not known" in errstr or "getaddrinfo" in errstr:
            send_telegram("[ws diagnostic] DNS or host resolution failed for WS URL: " + str(self.url))
        if "Client-id is missing" in errstr or "Client-id is missing or contains only whitespace" in errstr:
            send_telegram("[ws diagnostic] WS handshake failed: client-id missing or blank. Check DHAN_CLIENT_ID env var.")
        # let outer loop reconnect

    def on_close(self, ws, code, reason):
        print("WS closed:", code, reason)

    def run_forever(self):
        while True:
            try:
                if self.use_headers and self.headers:
                    print("Connecting WS (header-auth) to:", self.url, " headers:", bool(self.headers))
                    self.ws = websocket.WebSocketApp(self.url,
                                                     header=self.headers,
                                                     on_open=self.on_open,
                                                     on_message=self.on_message,
                                                     on_error=self.on_error,
                                                     on_close=self.on_close)
                else:
                    print("Connecting WS (url-auth/plain) to:", self.url)
                    self.ws = websocket.WebSocketApp(self.url,
                                                     on_open=self.on_open,
                                                     on_message=self.on_message,
                                                     on_error=self.on_error,
                                                     on_close=self.on_close)
                self.ws.run_forever(ping_interval=25, ping_timeout=10)
            except Exception as e:
                print("Exception in WS loop:", e)
                send_telegram("[ws diagnostic] Exception while connecting to WS: " + str(e)[:400])
            print(f"Reconnecting to WS in {RECONNECT_DELAY}s...")
            time.sleep(RECONNECT_DELAY)

# ---------------- Launcher ----------------
if __name__ == "__main__":
    print("Starting commodity bot. Symbols:", SYMBOLS)
    # Always run poller thread (fallback)
    poller = Poller(SYMBOLS, interval=POLL_INTERVAL)
    poller.start()

    if DISABLE_WS:
        print("DISABLE_WS is set -> running poller-only")
        send_telegram("⚠️ WS disabled - running poller-only.")
        try:
            while True:
                time.sleep(600)
        except KeyboardInterrupt:
            print("Stopping poller-only.")
            raise SystemExit(0)

    # Build ws url and headers intelligently
    effective_url, headers, use_headers = build_effective_ws_url_and_headers()

    # If CLIENT_ID missing or ACCESS_TOKEN missing -> notify and run poller-only
    if not CLIENT_ID or not ACCESS_TOKEN:
        msg = ("[credential warning] DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN missing/empty. "
               "WS/REST will fail until valid credentials are set. Running poller attempts (will show errors).")
        print(msg)
        send_telegram(msg)
        # run poller-only (poller already started); keep main alive
        try:
            while True:
                time.sleep(600)
        except KeyboardInterrupt:
            raise SystemExit(0)

    # Start WS client
    wsclient = DhanWS(effective_url, headers=headers, use_headers=use_headers, symbols=SYMBOLS)
    try:
        wsclient.run_forever()
    except KeyboardInterrupt:
        print("Exiting")
    except Exception as e:
        print("Fatal WS error:", e)
        send_telegram("[ws diagnostic] Fatal WS error: " + str(e)[:400])
