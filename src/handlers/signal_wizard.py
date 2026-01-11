from __future__ import annotations

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message

from ..config import Settings
from ..services.signals import create_and_post_signal

router = Router()

class SignalWizard(StatesGroup):
    symbol = State()
    market = State()
    direction = State()
    timeframe = State()
    entry = State()
    sl = State()
    tp1 = State()
    tp2 = State()
    rr = State()
    confidence = State()

def _is_admin(message: Message, settings: Settings) -> bool:
    return message.from_user is not None and message.from_user.id in settings.admin_ids

@router.message(Command("signal"))
async def signal_cmd(message: Message, state: FSMContext, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("⛔️ Только админ может создавать сигналы.")
        return
    await state.set_state(SignalWizard.symbol)
    await message.answer("Symbol (e.g. BTCUSDT):")

@router.message(SignalWizard.symbol)
async def step_symbol(message: Message, state: FSMContext) -> None:
    await state.update_data(symbol=message.text.strip().upper())
    await state.set_state(SignalWizard.market)
    await message.answer("Market: SPOT or FUTURES")

@router.message(SignalWizard.market)
async def step_market(message: Message, state: FSMContext) -> None:
    m = message.text.strip().upper()
    if m not in ("SPOT", "FUTURES"):
        await message.answer("Введите SPOT или FUTURES")
        return
    await state.update_data(market=m)
    await state.set_state(SignalWizard.direction)
    await message.answer("Direction: LONG or SHORT")

@router.message(SignalWizard.direction)
async def step_direction(message: Message, state: FSMContext) -> None:
    d = message.text.strip().upper()
    if d not in ("LONG", "SHORT"):
        await message.answer("Введите LONG или SHORT")
        return
    await state.update_data(direction=d)
    await state.set_state(SignalWizard.timeframe)
    await message.answer("Timeframe (e.g. 15m/1h):")

@router.message(SignalWizard.timeframe)
async def step_timeframe(message: Message, state: FSMContext) -> None:
    await state.update_data(timeframe=message.text.strip())
    await state.set_state(SignalWizard.entry)
    await message.answer("Entry price:")

def _to_float(text: str) -> float:
    t = text.replace(" ", "").replace(",", ".")
    return float(t)

@router.message(SignalWizard.entry)
async def step_entry(message: Message, state: FSMContext) -> None:
    await state.update_data(entry=_to_float(message.text))
    await state.set_state(SignalWizard.sl)
    await message.answer("Stop Loss (SL):")

@router.message(SignalWizard.sl)
async def step_sl(message: Message, state: FSMContext) -> None:
    await state.update_data(sl=_to_float(message.text))
    await state.set_state(SignalWizard.tp1)
    await message.answer("Take Profit 1 (TP1):")

@router.message(SignalWizard.tp1)
async def step_tp1(message: Message, state: FSMContext) -> None:
    await state.update_data(tp1=_to_float(message.text))
    await state.set_state(SignalWizard.tp2)
    await message.answer("Take Profit 2 (TP2):")

@router.message(SignalWizard.tp2)
async def step_tp2(message: Message, state: FSMContext) -> None:
    await state.update_data(tp2=_to_float(message.text))
    await state.set_state(SignalWizard.rr)
    await message.answer("RR (number after 1: , e.g. 3.0):")

@router.message(SignalWizard.rr)
async def step_rr(message: Message, state: FSMContext) -> None:
    await state.update_data(rr=_to_float(message.text))
    await state.set_state(SignalWizard.confidence)
    await message.answer("Confidence 0-100:")

@router.message(SignalWizard.confidence)
async def step_confidence(message: Message, state: FSMContext, settings: Settings) -> None:
    c = int(message.text.strip())
    c = max(0, min(100, c))
    data = await state.get_data()
    await state.clear()

    signal_id = await create_and_post_signal(
        bot=message.bot,
        vip_channel_id=settings.vip_channel_id,
        symbol=data["symbol"],
        market=data["market"],
        direction=data["direction"],
        timeframe=data["timeframe"],
        entry=float(data["entry"]),
        sl=float(data["sl"]),
        tp1=float(data["tp1"]),
        tp2=float(data["tp2"]),
        rr=float(data["rr"]),
        confidence=c,
    )

    await message.answer(f"✅ Сигнал опубликован в VIP-канал. signal_id={signal_id}")
