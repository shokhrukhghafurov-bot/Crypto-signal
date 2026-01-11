from __future__ import annotations

from aiogram import Router
from aiogram.types import CallbackQuery

from ..config import Settings
from ..services.tracking import ensure_user_position

router = Router()

@router.callback_query()
async def on_callback(call: CallbackQuery, settings: Settings) -> None:
    data = (call.data or "").strip()
    if data == "noop":
        await call.answer()
        return

    if data.startswith("opened:"):
        try:
            signal_id = int(data.split(":", 1)[1])
        except Exception:
            await call.answer("Ошибка", show_alert=True)
            return

        # create tracking entry for this user
        await ensure_user_position(call.from_user.id, signal_id, settings.tp1_partial_close_pct)

        await call.answer("✅ Зафиксировано. Бот начал сопровождение.", show_alert=False)
        # optional: send DM confirmation
        try:
            await call.message.bot.send_message(
                chat_id=call.from_user.id,
                text="✅ Сделка зафиксирована. Бот начал сопровождение этого сигнала и сообщит авто-закрытие (TP1/TP2/BE/SL).",
            )
        except Exception:
            # user might have blocked bot; nothing to do
            pass
        return

    await call.answer()
