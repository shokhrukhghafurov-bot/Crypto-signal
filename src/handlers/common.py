from __future__ import annotations

from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message

router = Router()

@router.message(CommandStart())
async def start(message: Message) -> None:
    await message.answer(
        "VIP Signals Bot\n\n"
        "• Signals are posted in the VIP channel.\n"
        "• Tap ✅ ОТКРЫЛ СДЕЛКУ under a signal to start tracking.\n\n"
        "Admins: use /signal to publish a new signal."
    )
