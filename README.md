# VIP Signals Telegram Bot (2 files) — GitHub + Railway ready

This is a **minimal** VIP-only signals bot:
- Posts a signal with an inline button **✅ ОТКРЫЛ СДЕЛКУ**
- After user clicks, bot starts tracking and sends updates:
  - **TP1 hit** (partial close + move SL to BE message)
  - **TP2 hit** (AUTO CLOSED WIN)
  - **SL hit before TP1** (AUTO CLOSED LOSS)
  - **BE after TP1** (AUTO CLOSED SAFE)

✅ Ready to push to **GitHub** and deploy on **Railway**.

---

## 1) Local run

### Install
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Configure
```bash
cp .env.example .env
```
Put your real values in `.env`.

### Start
```bash
python bot.py
```

Telegram:
- `/start`
- `/signal` — posts a demo signal with a button

---

## 2) Deploy on Railway

1. Push this repo to GitHub
2. Railway → **New Project** → **Deploy from GitHub repo**
3. Railway → **Variables**:
   - `BOT_TOKEN` = your token from @BotFather
   - optional: `TRACK_INTERVAL_SECONDS` = 3
   - optional: `TP1_PARTIAL_CLOSE_PCT` = 50
4. Deploy — Railway runs `python bot.py` (see `railway.json`)

---

## Files
- `bot.py` — Telegram bot + button handlers
- `backend.py` — tracking logic (mock price feed)
- `requirements.txt` — dependencies
- `.env.example` — env template
- `railway.json` — Railway start command
