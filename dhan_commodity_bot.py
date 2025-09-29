#!/usr/bin/env python3
# dhan_commodity_bot.py (updated: smart WS URL/auth logic + poller fallback + diagnostics)
# Paste this file and redeploy on Railway (worker process). Uses env vars described below.

import os
import time
import json
import threading
import traceback
import urllib.parse
import requests

try:
    import websocket
except Exception:
    raise RuntimeError("Missing dependency 'websocket-client'. Install with: pip install websocket-client")

# ---------------- CONFIG (env) ----------------
CLIENT_ID = (os.getenv("DHAN_CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("DHAN_ACCESS_TOKEN") or "").strip()
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

# WS_URL can be:
# - full query form: wss://api-feed.dhan.co?version=2&token=...&clientId=...&authType=2
# - simple host: wss://api-feed.dhan.co  (use header auth)
# - placeholder (e.g. wss://dhan.websocket.endpoint/marketfeed) -> code will replace with recommended host
WS_URL_RAW = (os.getenv("WS_URL") or "wss://dhan.websocket.placeholder/marketfeed").strip()

# WS auth mode: "auto" (try query if token present, else header),
# "query" (force query-param URL), "header" (force header auth)
WS_AUTH_MODE = (os.getenv("WS_AUTH_MODE") or "auto").strip().lower()

SYMBOLS = (os.getenv("SYMBOLS") or "GOLD,SILVER,CRUDEOIL").strip()
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL") or 60)
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY") or 5)
DISABLE_WS = (os.getenv("DISABLE_WS") or "").strip().lower() in ("1","true","yes")

# REST endpoints / CSV
DHAN_LTP_ENDPOINT = (os.getenv("DHAN_LTP_ENDPOINT") or "https://api.dhan.co/v2/marketfeed/ltp").strip()
INSTRUMENT_CSV_URL = (os.getenv("INSTRUMENT_CSV_URL") or "https://images.dhan.co/api-data/api-scrip-master-detailed.csv").strip()

# ---------------- Sample mapping: SYMBOL -> SECURITY_ID ----------------
# IMPORTANT: Replace placeholders with real SECURITY_ID values from Dhan instrument CSV.
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
        print("[telegram skipped] ", text)
        return
    try:
        r = requests.post(TELEGRAM_SEND_URL.format(token=TELEGRAM_BOT_TOKEN),
                          data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
        r.raise_for_status()
        print("[telegram] sent")
    except Exception as e:
        print("[telegram error]", e)

# ---------------- Helpers ----------------
def build_effective_ws_url():
    """
    Build an appropriate WS URL based on WS_URL_RAW, WS_AUTH_MODE, CLIENT_ID and ACCESS_TOKEN.
    Returns (url, use_headers_bool, headers_list)
    """
    base_recommended = "wss://api-feed.dhan.co"
    # If user provided full query form already, keep it
    if WS_URL_RAW.startswith("wss://api-feed.dhan.co"):
        # If user provided token in URL, keep as is
        if '?' in WS_URL_RAW:
            return WS_URL_RAW, False, None
        # plain host given: use headers (unless mode forces query)
        if WS_AUTH_MODE == "query":
            if not ACCESS_TOKEN or not CLIENT_ID:
                return WS_URL_RAW, True, [f"access-token: {ACCESS_TOKEN}", f"client-id: {CLIENT_ID}"]
            q = {
                "version": "2",
                "token": ACCESS_TOKEN,
                "clientId": CLIENT_ID,
                "authType": "2"
            }
            url = base_recommended + "?" + urllib.parse.urlencode(q)
            return url, False, None
        else:
            return WS_URL_RAW, True, [f"access-token: {ACCESS_TOKEN}", f"client-id: {CLIENT_ID}"]
    # If placeholder or unknown host, build recommended URL
    if "dhan.websocket.endpoint" in WS_URL_RAW or WS_URL_RAW.endswith("/marketfeed") or "placeholder" in WS_URL_RAW:
        if WS_AUTH_MODE == "header":
            return base_recommended, True, [f"access-token: {ACCESS_TOKEN}", f"client-id: {CLIENT_ID}"]
        # default: attempt query if token present
        if ACCESS_TOKEN and CLIENT_ID and WS_AUTH_MODE != "header":
            q = {
                "version": "2",
                "token": ACCESS_TOKEN,
                "clientId": CLIENT_ID,
                "authType": "2"
            }
            url = base_recommended + "?" + urllib.parse.urlencode(q)
            return url, False, None
        else:
            return base_recommended, True, [f"access-token: {ACCESS_TOKEN}", f"client-id: {CLIENT_ID}"]
    # Fallback: use provided
    # if it looks like http(s) -> convert to wss
    if WS_URL_RAW.startswith("https://"):
        wsurl = "wss://" + WS_URL_RAW[len("https://"):]
        return wsurl, True, [f"access-token: {ACCESS_TOKEN}", f"client-id: {CLIENT_ID}"]
    return WS_URL_RAW, True, [f"access-token: {ACCESS_TOKEN}", f"client-id: {CLIENT_ID}"]

# ---------------- Poller (1-min snapshot) ----------------
def get_security_id(symbol: str):
    return COMMODITY_IDS.get(symbol.upper())

def build_payload_for_poll(symbols):
    seg_map = {}
    for s in symbols:
        sid = get_security_id(s)
        if not sid:
            print("[poller-warn] security id missing for", s)
            continue
        seg = 'NSE_EQ'  # change if Dhan expects a commodity-specific segment
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
                send_telegram(f"[poller diagnostic] HTTP {r.status_code} attempt {attempt}\\nBody: {r.text[:600]}")
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
    def __init__(self, url, use_headers=False, headers=None, symbols=SYMBOLS):
        self.url = url
        self.use_headers = use_headers
        self.headers = headers
        self.symbols = [s.strip() for s in symbols.split(',') if s.strip()]
        self.ws = None
        self.last = {}

    def on_open(self, ws):
        print("WS opened - sending auth & subscribe")
        # send JSON auth for extra compatibility (safe even if server ignores)
        try:
            ws.send(json.dumps(build_auth_payload()))
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

    def on_error(self, ws, err):
        print("WS error:", err)
        # If DNS or name resolution error, notify
        errstr = str(err)
        if "Name or service not known" in errstr or "getaddrinfo" in errstr:
            send_telegram("[ws diagnostic] DNS or host resolution failed for WS URL: " + str(self.url))
            # raise to let outer loop decide fallback/retry

    def on_close(self, ws, code, reason):
        print("WS closed:", code, reason)

    def run_forever(self):
        # wrapper to select header vs normal connect
        while True:
            try:
                if self.use_headers and self.headers:
                    print("Connecting WS (header-auth) to:", self.url)
                    self.ws = websocket.WebSocketApp(self.url,
                                                     header=self.headers,
                                                     on_open=self.on_open,
                                                     on_message=self.on_message,
                                                     on_error=self.on_error,
                                                     on_close=self.on_close)
                else:
                    print("Connecting WS (url-auth or plain) to:", self.url)
                    self.ws = websocket.WebSocketApp(self.url,
                                                     on_open=self.on_open,
                                                     on_message=self.on_message,
                                                     on_error=self.on_error,
                                                     on_close=self.on_close)
                self.ws.run_forever(ping_interval=25, ping_timeout=10)
            except Exception as e:
                print("Exception in WS loop:", e)
                # notify once
                send_telegram("[ws diagnostic] Exception while connecting to WS: " + str(e)[:400])
            # reconnect delay
            print(f"Reconnecting to WS in {RECONNECT_DELAY}s...")
            time.sleep(RECONNECT_DELAY)

# ---------------- Launcher ----------------
if __name__ == "__main__":
    print("Starting commodity bot. Symbols:", SYMBOLS)
    # Start poller always (acts as fallback)
    poller = Poller(SYMBOLS, interval=POLL_INTERVAL)
    poller.start()

    if DISABLE_WS:
        print("WS disabled via DISABLE_WS env var — running poller-only.")
        send_telegram("⚠️ WS disabled — running poller-only.")
        try:
            while True:
                time.sleep(600)
        except KeyboardInterrupt:
            print("Stopping (poller-only).")
            raise SystemExit(0)

    # Build effective ws url and decide header vs url auth
    effective_url, use_headers, headers = build_effective_ws_url()
    # headers is None for query-param URL, or a list for header auth
    wsclient = DhanWS(effective_url, use_headers=use_headers, headers=headers, symbols=SYMBOLS)

    # Try running websocket; if DNS/host problem occurs, the WS on_error will send diagnostics and WS will keep retrying.
    try:
        wsclient.run_forever()
    except KeyboardInterrupt:
        print("Exiting")
    except Exception as e:
        print("Fatal error starting WS:", e)
        send_telegram("[ws diagnostic] Fatal error starting WS: " + str(e)[:400])
