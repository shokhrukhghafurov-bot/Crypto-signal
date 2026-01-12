# PRO Auto-Scanner Bot — UI Buttons + My Trades + Multi-Exchange 2/3 + News + Macro AUTO

New features:
- /start shows **menu buttons** (no need to type /status)
- Buttons:
  - 📊 Status
  - 🟢 Spot Live Signal
  - 🔴 Futures Live Signal
  - 📂 My Opened Trades (shows status of trades you opened)
- Auto scanner 24/7 (15m/1h/4h), multi-exchange 2/3 confirmation (Binance+Bybit+OKX)
- News risk filter (CryptoPanic, optional) + macro blackout AUTO (BLS + Fed)

Deploy on Railway:
- Set `BOT_TOKEN`
Optional:
- `ADMIN_IDS` (only admins see some extra scanner info)
- News: `NEWS_FILTER=1`, `CRYPTOPANIC_TOKEN=...`
- Macro: `MACRO_FILTER=1`, `MACRO_ACTION=FUTURES_OFF`, `BLACKOUT_BEFORE_MIN`, `BLACKOUT_AFTER_MIN`, `TZ_NAME`

