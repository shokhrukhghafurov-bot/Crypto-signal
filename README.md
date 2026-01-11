# VIP Signals Telegram Bot (2 files) — Real Binance Price (SPOT/FUTURES) + Railway ready

Minimal VIP-only signals bot:
- Posts a signal with inline button **✅ ОТКРЫЛ СДЕЛКУ**
- After user clicks, bot starts tracking and sends updates:
  - **TP1 hit** → message (partial close) + move SL to BE (message)
  - **TP2 hit** → AUTO CLOSED WIN
  - **SL hit before TP1** → AUTO CLOSED LOSS
  - **BE after TP1** → AUTO CLOSED SAFE

✅ Uses **real-time Binance WebSocket price** (optional).  
✅ Ready for **GitHub + Railway**.

---

## Local run

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
python bot.py
```

Telegram:
- `/start`
- `/signal` — posts demo signal + button

---

## Environment variables

Required:
- `BOT_TOKEN`

Optional:
- `USE_REAL_PRICE=1` to enable Binance WebSocket price (default: 0 -> mock)
- `BINANCE_MARKET=FUTURES` or `SPOT` (default: FUTURES)
- `TRACK_INTERVAL_SECONDS=3`
- `TP1_PARTIAL_CLOSE_PCT=50`

---

## Railway deploy

1. Push to GitHub
2. Railway → New Project → Deploy from GitHub repo
3. Railway → Variables:
   - `BOT_TOKEN` = your token
   - `USE_REAL_PRICE` = 1
   - `BINANCE_MARKET` = FUTURES (or SPOT)
4. Deploy — Railway runs `python bot.py` (see `railway.json`)

---

## Notes
- Binance WS endpoints used:
  - SPOT: `wss://stream.binance.com:9443/ws/<symbol>@trade`
  - FUTURES: `wss://fstream.binance.com/ws/<symbol>@trade`
- This bot **does not place orders**. It tracks levels and sends notifications.
