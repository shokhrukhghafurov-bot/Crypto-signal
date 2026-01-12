# PRO Auto-Scanner Bot — Multi-Exchange 2/3 + News Risk Filter
Timeframes: **15m + 1h + 4h**

✅ Scans Binance + Bybit + OKX and emits signals only if confirmed by **>=2/3 exchanges**  
✅ Indicators: EMA / RSI / MACD / ADX / ATR  
✅ Calculates Entry/SL/TP1/TP2 + RR + Confidence  
✅ Broadcasts to all users who pressed `/start`  
✅ Button **✅ ОТКРЫЛ СДЕЛКУ** → tracking + AUTO CLOSED (TP1/TP2/BE/SL) via Binance WS  
✅ **News risk filter** (optional): blocks signals during recent high-impact news

---

## News filter (optional)

This repo implements a **risk filter**, not "AI news prediction".

### Provider: CryptoPanic (recommended)
- Set `CRYPTOPANIC_TOKEN` in Railway Variables (or `.env` locally)
- Bot will check for **important** recent posts for the base coin (BTC, ETH, etc.)
- If a fresh important post appears within `NEWS_LOOKBACK_MIN`, the bot will apply `NEWS_ACTION`.

### Actions
- `NEWS_ACTION=FUTURES_OFF` → do not publish FUTURES, allow only SPOT signals
- `NEWS_ACTION=PAUSE_ALL` → do not publish any signals during the window

Defaults are safe:
- enabled only if `NEWS_FILTER=1` and `CRYPTOPANIC_TOKEN` is present.

---

## Railway variables (minimum)
Required:
- `BOT_TOKEN`

Optional:
- `ADMIN_IDS` for `/status`
- Scanner tuning: `TOP_N`, `SCAN_INTERVAL_SECONDS`, `CONFIDENCE_MIN`, `COOLDOWN_MINUTES`
- Tracking: `USE_REAL_PRICE=1`

News filter:
- `NEWS_FILTER=1`
- `CRYPTOPANIC_TOKEN=...`
- `NEWS_LOOKBACK_MIN=60`
- `NEWS_ACTION=FUTURES_OFF` (or `PAUSE_ALL`)


