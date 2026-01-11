# VIP Telegram Signals Bot (SPOT + FUTURES) — VIP only

A GitHub-ready starter template for a **VIP-only** Telegram signals service:
- Publishes signals (SPOT / FUTURES) to a VIP channel
- Each signal includes an inline button: **✅ ОТКРЫЛ СДЕЛКУ**
- After the user clicks "Opened", the bot starts **tracking** the signal for that user and sends **AUTO CLOSED** updates:
  - TP1 hit → partial close + move SL to BE
  - TP2 hit → close remainder (WIN)
  - SL hit before TP1 → (LOSS)
  - Return to Entry after TP1 → (BREAK EVEN)

> This repo **does not** execute trades on exchanges. It tracks price levels and posts updates.
> You can integrate real price feeds (WebSocket/REST) later.

## Tech
- Python 3.11+
- aiogram 3 (Telegram Bot API)
- SQLite (aiosqlite) for persistence

## Quick start

1) Create bot and get token via @BotFather.

2) Copy env example:
```bash
cp .env.example .env
```

3) Fill `.env`:
- `BOT_TOKEN`
- `VIP_CHANNEL_ID` (numeric, like `-1001234567890`)
- Optional: `ADMIN_IDS`

4) Install and run:
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m src.main
```

## How it works

### Posting a signal
Use `/signal` in a private chat with the bot (admin only). The bot will ask for fields and post into the VIP channel with the button.

### Open tracking
When a user taps **✅ ОТКРЫЛ СДЕЛКУ**, we create a "user position" linked to the signal and start tracking.

### Price feed
The project includes a **mock price feed** (random walk) for demo purposes.
Replace `src/exchanges/price_feed.py` with a real exchange feed.

## Project structure
- `src/main.py` — entrypoint
- `src/handlers/*` — bot handlers
- `src/services/*` — business logic (signals + tracking)
- `src/db.py` — SQLite storage
- `src/exchanges/price_feed.py` — mock feed (replace with real)

## Notes / TODO
- Add payment/subscription system (Stripe, Crypto payments, etc.)
- Add membership verification (VIP subscription check)
- Add multi-language templates (RU/EN)
- Add PnL and statistics reporting

## License
MIT
