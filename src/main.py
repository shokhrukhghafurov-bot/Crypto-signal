from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from .config import load_settings, Settings
from .db import init_db
from .exchanges.price_feed import MockPriceFeed
from .handlers.common import router as common_router
from .handlers.callbacks import router as callbacks_router
from .handlers.signal_wizard import router as signal_router
from .services.tracking import tracking_loop

logging.basicConfig(level=logging.INFO)

async def main() -> None:
    settings: Settings = load_settings()
    await init_db()

    bot = Bot(token=settings.bot_token)
    dp = Dispatcher(storage=MemoryStorage())

    # Inject settings into handlers via middleware-like simple context
    dp["settings"] = settings

    # Routers
    dp.include_router(common_router)
    dp.include_router(signal_router)
    dp.include_router(callbacks_router)

    # Mock price feed (demo). Seed with a couple symbols if you want.
    feed = MockPriceFeed()

    # Background tracking loop
    asyncio.create_task(tracking_loop(bot, settings, feed))

    # Start polling
    await dp.start_polling(bot, settings=settings)

if __name__ == "__main__":
    asyncio.run(main())
