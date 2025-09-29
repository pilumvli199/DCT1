#!/usr/bin/env python3
# dhan_commodity_bot.py
# WebSocket + 1-min poller for Indian commodity LTP -> Telegram
# Railway-ready: read config from env vars, print diagnostics to logs, and send Telegram alerts.

import os, time, json, threading, traceback, requests

try:
    import websocket
except Exception:
    raise RuntimeError("Install websocket-client: pip install websocket-client")

# ---------------- CONFIG (env) ----------------
CLIENT_ID = (os.getenv("DHAN_CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("DHAN_ACCESS_TOKEN") or "").strip()
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
WS_URL = (os.getenv("WS_URL") or "wss://dhan.websocket.placeholder/marketfeed").strip()
SYMBOLS = (os.getenv("SYMBOLS") or "GOLD,SILVER,CRUDEOIL").strip()
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL") or 60)

DHAN_LTP_ENDPOINT = (os.getenv("DHAN_LTP_ENDPOINT") or "https://api.dhan.co/v2/marketfeed/ltp").strip()
INSTRUMENT_CSV_URL = (os.getenv("INSTRUMENT_CSV_URL") or "https://images.dhan.co/api-data/api-scrip-master-detailed.csv").strip()

# ---------------- Sample mapping: SYMBOL -> SECURITY_ID ----------------
# IMPORTANT: Replace these placeholders with real SECURITY_ID values from Dhan's instrument CSV.
COMMODITY_IDS = {
    "GOLD": "5001",
    "SILVER": "5002",
    "CRUDEOIL": "5003",
    "NATGAS": "5004",
}

def get_security_id(symbol: str):
    return COMMODITY_IDS.get(symbol.upper())

# ---------------- Telegram helper ----------------
TELEGRAM_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"
def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[telegram skipped]", text)
        return
    try:
        r = requests.post(TELEGRAM_SEND_URL.format(token=TELEGRAM_BOT_TOKEN),
                          data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send error:", e)

# ---------------- Poller (1-min snapshot) ----------------
def build_payload_for_poll(symbols):
    seg_map = {}
    for s in symbols:
        sid = get_security_id(s)
        if not sid:
            print("[poller-warn] security id missing for", s)
            continue
        seg = 'NSE_EQ'  # provider-specific: change if Dhan expects different segment for commodities
        try:
            seg_map.setdefault(seg, []).append(int(sid))
        except Exception:
            seg_map.setdefault(seg, []).append(sid)
    return seg_map

def call_dhan_ltp(payload, retries=2):
    headers = {'access-token': ACCESS_TOKEN or '', 'client-id': CLIENT_ID or '', 'Accept': 'application/json', 'Content-Type': 'application/json'}
    for attempt in range(1, retries+1):
        try:
            r = requests.post(DHAN_LTP_ENDPOINT, json=payload, headers=headers, timeout=10)
            print("[poller] HTTP", r.status_code)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception as e:
                    print("JSON parse failed:", e)
                    return None
            else:
                send_telegram(f"[poller diagnostic] HTTP {r.status_code} attempt {attempt}\nBody: {r.text[:600]}")
        except Exception as e:
            print("Poller exception:", e)
        time.sleep(1)
    return None

def format_and_send_poll(resp):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    if not resp:
        send_telegram(f"⏱ {now} — Poller: no response from Dhan")
        return
    data = resp.get("data") if isinstance(resp, dict) else resp
    lines = []
    if isinstance(data, dict):
        for seg, bucket in data.items():
            if not isinstance(bucket, dict):
                continue
            for secid, info in bucket.items():
                if not isinstance(info, dict):
                    continue
                ltp = info.get('last_price') or info.get('ltp') or info.get('lastPrice') or info.get('price')
                if ltp is None: continue
                readable = None
                for k,v in COMMODITY_IDS.items():
                    if str(v) == str(secid):
                        readable = k; break
                lines.append(f"<b>{readable or secid}</b> LTP: <code>{ltp}</code> ({seg})")
    if lines:
        send_telegram(f"⏱ {now} — Poller snapshot:\n" + "\n".join(lines))
    else:
        send_telegram(f"⏱ {now} — Poller: no LTP fields found (see logs)")

class Poller(threading.Thread):
    def __init__(self, symbols, interval=POLL_INTERVAL):
        super().__init__(daemon=True)
        self.symbols = [s.strip() for s in symbols.split(',') if s.strip()]
        self.interval = interval

    def run(self):
        # try CSV fallback once if mapping incomplete
        payload_map = build_payload_for_poll(self.symbols)
        if not payload_map:
            try:
                r = requests.get(INSTRUMENT_CSV_URL, timeout=15); r.raise_for_status()
                from csv import DictReader
                reader = DictReader(r.text.splitlines())
                csv_map = {}
                for row in reader:
                    name = (row.get('SM_SYMBOL_NAME') or row.get('TRADING_SYMBOL') or '').strip().upper()
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
                payload = build_payload_for_poll(self.symbols)
                print("[poller] payload:", payload)
                resp = call_dhan_ltp(payload)
                format_and_send_poll(resp)
            except Exception as e:
                print("Poller exception:", e)
            time.sleep(self.interval)

# ---------------- WebSocket client ----------------
def build_auth_payload():
    return {'action':'auth','client_id': CLIENT_ID, 'access_token': ACCESS_TOKEN}

def build_subscribe_payload(symbols):
    out = []
    for s in symbols:
        sid = get_security_id(s)
        if not sid:
            print("[ws-warn] missing id for", s); continue
        seg = 'NSE_EQ'
        out.append({'segment': seg, 'instrument_id': str(sid)})
    return {'action':'subscribe', 'instruments': out}

class DhanWS:
    def __init__(self, url, symbols):
        self.url = url
        self.symbols = [s.strip() for s in symbols.split(',') if s.strip()]
        self.ws = None
        self.last = {}

    def on_open(self, ws):
        print("WS opened - auth")
        auth = build_auth_payload(); ws.send(json.dumps(auth)); print("Auth sent")
        sub = build_subscribe_payload(self.symbols); ws.send(json.dumps(sub)); print("Subscribe sent")

    def on_message(self, ws, message):
        try:
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            data = json.loads(message)
        except Exception as e:
            print("WS msg parse fail:", e, str(message)[:200])
            return
        ltp = None; label = None
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

    def on_error(self, ws, err): print("WS error:", err)
    def on_close(self, ws, code, reason): print("WS closed:", code, reason)

    def run(self):
        headers = [f"access-token: {ACCESS_TOKEN}", f"client-id: {CLIENT_ID}"]
        self.ws = websocket.WebSocketApp(self.url,
                                        on_open=self.on_open,
                                        on_message=self.on_message,
                                        on_error=self.on_error,
                                        on_close=self.on_close,
                                        header=headers)
        self.ws.run_forever(ping_interval=25, ping_timeout=10)

# ---------------- Launcher ----------------
if __name__ == "__main__":
    print("Starting commodity bot. Symbols:", SYMBOLS)
    poller = Poller(SYMBOLS, interval=POLL_INTERVAL)
    poller.start()
    wsclient = DhanWS(WS_URL, SYMBOLS)
    try:
        wsclient.run()
    except KeyboardInterrupt:
        print("Exiting")
