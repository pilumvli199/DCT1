Dhan Commodity WebSocket + Poller Bot (Railway-ready)

Files:
- dhan_commodity_bot.py     # Main bot (WebSocket + 1-min poller)
- requirements.txt
- Procfile                  # worker: python dhan_commodity_bot.py
- Dockerfile                # optional Docker deployment
- .env.example              # template for Railway env vars
- README.md                 # this file

Quickstart (Railway):
1. Push repo to GitHub and connect to Railway, or upload files directly.
2. In Railway Project -> Settings -> Variables, add:
   DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, WS_URL, SYMBOLS
3. Deploy. Railway will run the `worker` process defined in Procfile.
4. Check logs for poller/WS output and Telegram alerts.

Notes:
- Replace COMMODITY_IDS placeholders with real SECURITY_IDs from Dhan instrument CSV.
- If Dhan requires a different segment key for commodities, update 'seg' used in build_payload_for_poll and build_subscribe_payload.
