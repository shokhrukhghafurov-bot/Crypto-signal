from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

def opened_keyboard(signal_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ ОТКРЫЛ СДЕЛКУ", callback_data=f"opened:{signal_id}")
    return kb.as_markup()

def noop_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="—", callback_data="noop")
    return kb.as_markup()
