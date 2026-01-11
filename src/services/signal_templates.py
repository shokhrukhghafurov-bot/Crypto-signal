from __future__ import annotations

from .utils import fmt_price

def render_signal_text(
    market: str,
    symbol: str,
    direction: str,
    timeframe: str,
    entry: float,
    sl: float,
    tp1: float,
    tp2: float,
    rr: float,
    confidence: int,
) -> str:
    header = "🟢 SPOT SIGNAL" if market == "SPOT" else "🔴 FUTURES SIGNAL"
    arrow = "📈 LONG" if direction == "LONG" else "📉 SHORT"
    return (
        f"{header}\n\n"
        f"🪙 {symbol}\n"
        f"{arrow}\n"
        f"⏱ TF: {timeframe}\n\n"
        f"Entry: {fmt_price(entry)}\n"
        f"SL: {fmt_price(sl)}\n"
        f"TP1: {fmt_price(tp1)}\n"
        f"TP2: {fmt_price(tp2)}\n\n"
        f"RR: 1:{rr:.2f}\n"
        f"Confidence: {confidence}/100"
    )

def render_tp1_hit(symbol: str, close_pct: int, moved_to_be: bool) -> str:
    extra = "SL moved to Entry (BE)" if moved_to_be else ""
    return (
        "🟡 TP1 HIT\n\n"
        f"🪙 {symbol}\n"
        f"Closed: {close_pct}%\n"
        f"{extra}".strip()
    )

def render_closed(symbol: str, reason: str, result_text: str) -> str:
    if reason == "TP2":
        title = "✅ SIGNAL AUTO CLOSED — TP2 HIT"
        status = "Status: WIN 🟢"
    elif reason == "BE":
        title = "⚪ SIGNAL AUTO CLOSED — BREAK EVEN"
        status = "Status: SAFE ⚪"
    else:
        title = "❌ SIGNAL AUTO CLOSED — STOP LOSS"
        status = "Status: LOSS 🔴"
    return (
        f"{title}\n\n"
        f"🪙 {symbol}\n"
        f"{result_text}\n"
        f"{status}"
    )
