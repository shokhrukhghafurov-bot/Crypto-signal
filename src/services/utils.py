from __future__ import annotations

def fmt_price(x: float) -> str:
    # pretty formatting for crypto prices
    if x >= 1000:
        return f"{x:,.0f}".replace(",", " ")
    if x >= 1:
        return f"{x:.2f}"
    if x >= 0.01:
        return f"{x:.4f}"
    return f"{x:.8f}".rstrip("0").rstrip(".")
