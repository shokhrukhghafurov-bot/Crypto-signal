# Crypto-signal bot (Railway, Python 3.11)

Этот бот использует smart setup-логику для выборочных входов, а не для «количества ради количества».

## Как работает quality-логика сигналов
- Каждый сигнал проходит фильтры качества (trend/volume/ADX/RSI/VWAP/ATR/MACD/BB/RR + setup-specific confirmations).
- При слабом качестве сигнал блокируется с причиной (например: `ADX_TOO_LOW`, `RR_TOO_LOW`, `MARKET_NOT_SUITABLE`).
- Логика адаптации применяется адресно: по конкретному setup, стороне (LONG/SHORT) и symbol, без глобальной деградации всех setup.
- `SETUP_TARGET_WINRATE=90` — целевая метрика оптимизации качества, **не гарантия прибыли**.

## 24/7 проверка открытых сигналов
- Фоновый review loop проверяет открытые сигналы постоянно на Railway.
- Интервал задаётся через `SIGNAL_REVIEW_INTERVAL_SEC` (рекомендуется 60 сек).
- Проверка включает TP/SL/BE/EXPIRED/UNCERTAIN и сохраняет результат в хранилище сигналов.
- Если TP и SL попали в одну свечу — используется дополнительная проверка на меньшем ТФ; при неопределённости ставится `UNCERTAIN` (без фейкового WIN).

## Раздельный анализ setup
Поддерживаются 11 setup-маршрутов:
- `smc_liquidity_reclaim`
- `smc_ob_fvg_overlap`
- `smc_htf_ob_ltf_fvg`
- `smc_bos_retest_confirm`
- `smc_displacement_origin`
- `smc_dual_fvg_origin`
- `origin_fastpath`
- `breakout_fastpath`
- `zone_retest`
- `breakout`
- `fast_continuation`

Диагностика и блокировки трекаются по каждому setup отдельно (в т.ч. per-side/per-symbol при включённых флагах).

## Переменные для Railway (.env)
Добавьте/проверьте:
- `SIGNAL_REVIEW_LOOP_ENABLED=1`
- `SIGNAL_REVIEW_INTERVAL_SEC=60`
- `SIGNAL_REVIEW_MAX_OPEN_HOURS=24`
- `SIGNAL_REVIEW_FETCH_LIMIT=500`
- `SIGNAL_REVIEW_MARK_EXPIRED=1`
- `SETUP_ENGINE_ENABLED=1`
- `SETUP_TARGET_WINRATE=90`
- `SETUP_DIAGNOSTICS_ENABLED=1`
- `SETUP_DAILY_TUNING_ENABLED=1`
- `SETUP_SAFE_RELAX_ENABLED=1`
- `SETUP_BAD_WINRATE_BLOCK_ENABLED=1`
- `SETUP_LOSS_STREAK_LIMIT=2`
- `SETUP_PER_SIDE_STATS_ENABLED=1`
- `SETUP_PER_SYMBOL_STATS_ENABLED=1`
- `SETUP_BLOCK_REASON_TRACKING=1`
- `SETUP_MIN_CONFIDENCE_DEFAULT=70`
- `SETUP_MIN_TA_SCORE_DEFAULT=65`
- `SETUP_MIN_RR_DEFAULT=1.4`
- `SETUP_MIN_ADX_DEFAULT=18`
- `SETUP_MIN_VOLUME_DEFAULT=1.0`
- `CODEX_AUTO_MAINTENANCE_ENABLED=1`
- `CODEX_AUTO_MAINTENANCE_REPORT=1`

## Как быстро отключить новую логику
Если нужно аварийно ослабить/выключить новую механику:
- `SETUP_ENGINE_ENABLED=0` — выключить setup quality engine.
- `SETUP_DIAGNOSTICS_ENABLED=0` — выключить детальную диагностику блокировок.
- `SIGNAL_REVIEW_LOOP_ENABLED=0` — выключить loop проверки открытых сигналов.

## Recurring Codex automation
Рекомендуемый режим автоподдержки репозитория:
- Базовый schedule: **ежедневно в 03:00 UTC**.
- Опционально (если лимиты позволяют): **каждые 6 часов**.
- Задача automation: проверка состояния main, точечные улучшения signal quality/торговой логики, commit+push и отчёт.
