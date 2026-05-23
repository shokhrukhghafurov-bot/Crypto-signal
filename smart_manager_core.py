from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


def clamp01(value: float) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except Exception:
        return 0.0


@dataclass(slots=True)
class SmartPositionState:
    direction: str = "LONG"
    entry_price: float = 0.0
    current_price: float = 0.0
    now_ts: float = 0.0
    sm_state: str = "INIT"
    armed_sl: bool = False
    tp1_seen: bool = False
    tp1_hit: bool = False
    tp1_partial: bool = False
    tp1_ts: float = 0.0
    tp1_mode: str = ""
    tp2_prob: float = 0.0
    be_pending: bool = False
    be_moved: bool = False
    be_price: float = 0.0
    last_px: float = 0.0
    last_px_ts: float = 0.0
    best_px: float = 0.0
    hard_sl: float = 0.0
    tp2_runner_active: bool = False
    tp2_runner_peak_px: float = 0.0
    tp2_runner_floor_px: float = 0.0
    tp2_runner_armed_ts: float = 0.0
    neg_count: int = 0
    sl_hits: int = 0
    sl_first_ts: float = 0.0
    sl_extreme: float = 0.0

    @classmethod
    def from_ref(
        cls,
        ref: Dict[str, Any],
        *,
        direction: str,
        entry_price: float,
        current_price: float,
        now_ts: float,
    ) -> "SmartPositionState":
        return cls(
            direction=str(direction or "LONG").upper(),
            entry_price=float(entry_price or 0.0),
            current_price=float(current_price or 0.0),
            now_ts=float(now_ts or 0.0),
            sm_state=str(ref.get("sm_state") or "INIT").upper(),
            armed_sl=bool(ref.get("armed_sl")),
            tp1_seen=bool(ref.get("tp1_seen")),
            tp1_hit=bool(ref.get("tp1_hit")),
            tp1_partial=bool(ref.get("tp1_partial")),
            tp1_ts=float(ref.get("tp1_ts") or 0.0),
            tp1_mode=str(ref.get("tp1_mode") or "").upper(),
            tp2_prob=float(ref.get("tp2_prob") or 0.0),
            be_pending=bool(ref.get("be_pending")),
            be_moved=bool(ref.get("be_moved")),
            be_price=float(ref.get("be_price") or 0.0),
            last_px=float(ref.get("last_px") or current_price or 0.0),
            last_px_ts=float(ref.get("last_px_ts") or now_ts or 0.0),
            best_px=float(ref.get("best_px") or current_price or 0.0),
            hard_sl=float(ref.get("hard_sl") or 0.0),
            tp2_runner_active=bool(ref.get("tp2_runner_active")),
            tp2_runner_peak_px=float(ref.get("tp2_runner_peak_px") or current_price or 0.0),
            tp2_runner_floor_px=float(ref.get("tp2_runner_floor_px") or 0.0),
            tp2_runner_armed_ts=float(ref.get("tp2_runner_armed_ts") or 0.0),
            neg_count=int(ref.get("neg_count") or 0),
            sl_hits=int(ref.get("sl_hits") or 0),
            sl_first_ts=float(ref.get("sl_first_ts") or 0.0),
            sl_extreme=float(ref.get("sl_extreme") or 0.0),
        )

    def to_ref_patch(self) -> Dict[str, Any]:
        return {
            "sm_state": str(self.sm_state or "INIT").upper(),
            "armed_sl": bool(self.armed_sl),
            "tp1_seen": bool(self.tp1_seen),
            "tp1_hit": bool(self.tp1_hit),
            "tp1_partial": bool(self.tp1_partial),
            "tp1_ts": float(self.tp1_ts or 0.0),
            "tp1_mode": str(self.tp1_mode or "").upper(),
            "tp2_prob": float(self.tp2_prob or 0.0),
            "be_pending": bool(self.be_pending),
            "be_moved": bool(self.be_moved),
            "be_price": float(self.be_price or 0.0),
            "last_px": float(self.last_px or 0.0),
            "last_px_ts": float(self.last_px_ts or 0.0),
            "best_px": float(self.best_px or 0.0),
            "hard_sl": float(self.hard_sl or 0.0),
            "tp2_runner_active": bool(self.tp2_runner_active),
            "tp2_runner_peak_px": float(self.tp2_runner_peak_px or 0.0),
            "tp2_runner_floor_px": float(self.tp2_runner_floor_px or 0.0),
            "tp2_runner_armed_ts": float(self.tp2_runner_armed_ts or 0.0),
            "neg_count": int(self.neg_count or 0),
            "sl_hits": int(self.sl_hits or 0),
            "sl_first_ts": float(self.sl_first_ts or 0.0),
            "sl_extreme": float(self.sl_extreme or 0.0),
        }


@dataclass(slots=True)
class TP1Decision:
    mode: str
    probability: float
    progress_to_tp2: float
    distance_to_tp2_pct: float
    giveback_fraction: float
    momentum_favor_pct: float
    momentum_adverse_pct: float
    partial_pct: float


@dataclass(slots=True)
class WeaknessExitDecision:
    should_close: bool
    best_gain_pct: float
    giveback_pct: float
    giveback_frac: float
    progress_to_tp1: float
    reason: str


@dataclass(slots=True)
class ReversalExitDecision:
    should_close: bool
    gain_pct: float
    retrace_pct: float
    gain_threshold: float
    retrace_threshold: float


@dataclass(slots=True)
class InvalidationExitDecision:
    should_close: bool
    score: int
    loss_now_pct: float
    gain_now_pct: float
    full_sl_loss_pct: float
    max_early_loss_pct: float
    progress_to_tp1: float
    bars_since_entry: int
    components: str
    reason: str


@dataclass(slots=True)
class StructuralSLDecision:
    should_close: bool
    crossed: bool
    hard_hit: bool
    deep_breach: bool
    reclaimed: bool
    bounced: bool
    age_sec: float
    sl_hits: int
    sl_first_ts: float
    sl_extreme: float


@dataclass(slots=True)
class TP2RunnerDecision:
    should_close: bool
    reason: str
    peak_extension_pct: float
    giveback_from_peak_pct: float
    back_to_tp2: bool


class SmartDecisionEngine:
    @staticmethod
    def compute_tp2_probability(*, entry_price: float, current_price: float, best_price: float, last_price: float, tp2_price: float, direction: str) -> TP1Decision:
        direction = str(direction or "LONG").upper()
        entry = float(entry_price or 0.0)
        px = float(current_price or 0.0)
        best_px = float(best_price or px)
        last_px = float(last_price or px)
        tp2 = float(tp2_price or 0.0)

        mom_fav_pct = 0.0
        mom_adv_pct = 0.0
        if last_px > 0:
            if direction == "LONG":
                mom_fav_pct = max(0.0, (px / last_px - 1.0) * 100.0)
                mom_adv_pct = max(0.0, (1.0 - (px / last_px)) * 100.0)
            else:
                mom_fav_pct = max(0.0, (1.0 - (px / last_px)) * 100.0)
                mom_adv_pct = max(0.0, (px / last_px - 1.0) * 100.0)

        prob_tp2 = 0.0
        prog = 0.0
        dist = 0.0
        giveback_frac = 0.0
        if tp2 > 0 and entry > 0 and tp2 != entry and px > 0:
            if direction == "LONG":
                prog = (px - entry) / (tp2 - entry)
                dist = max(0.0, (tp2 - px) / max(1e-9, px)) * 100.0
                best_gain = max(0.0, (best_px / entry - 1.0) * 100.0) if best_px > 0 else 0.0
                gain_now = max(0.0, (px / entry - 1.0) * 100.0)
            else:
                prog = (entry - px) / (entry - tp2)
                dist = max(0.0, (px - tp2) / max(1e-9, px)) * 100.0
                best_gain = max(0.0, (1.0 - (best_px / entry)) * 100.0) if best_px > 0 else 0.0
                gain_now = max(0.0, (1.0 - (px / entry)) * 100.0)
            prog = clamp01(prog)
            if best_gain > 0:
                giveback_frac = clamp01((best_gain - gain_now) / max(1e-9, best_gain))
            mom_factor = clamp01(mom_fav_pct / max(0.05, dist)) if dist > 0 else 0.0
            fade_factor = 1.0 - giveback_frac
            prob_tp2 = clamp01(0.50 * prog + 0.35 * mom_factor + 0.15 * fade_factor)

        return TP1Decision(
            mode="",
            probability=prob_tp2,
            progress_to_tp2=prog,
            distance_to_tp2_pct=dist,
            giveback_fraction=giveback_frac,
            momentum_favor_pct=mom_fav_pct,
            momentum_adverse_pct=mom_adv_pct,
            partial_pct=0.0,
        )

    @staticmethod
    def select_tp1_mode(*, probability: float, tp2_price: float, force_full_if_no_tp2: bool, prob_strong: float, prob_med: float) -> str:
        if float(tp2_price or 0.0) <= 0 and bool(force_full_if_no_tp2):
            return "FULL_TP1_NO_TP2"
        if float(probability or 0.0) >= float(prob_strong):
            return "HOLD_TO_TP2"
        if float(probability or 0.0) >= float(prob_med):
            return "PARTIAL_TP1"
        return "FULL_TP1"

    @staticmethod
    def compute_dynamic_partial_pct(*, prob_tp2: float, prog_to_tp2: float, mom_fav_pct: float, dist_pct: float, giveback_frac: float, tp1_partial_pct: float, tp1_partial_min_pct: float, tp1_partial_max_pct: float) -> float:
        try:
            p_min = max(0.05, min(0.95, float(tp1_partial_min_pct)))
            p_max = max(p_min, min(0.95, float(tp1_partial_max_pct)))
            base_pct = max(p_min, min(p_max, float(tp1_partial_pct)))
            hold_score = 0.70 * clamp01(prob_tp2) + 0.20 * clamp01(prog_to_tp2)
            if dist_pct > 0:
                hold_score += 0.10 * clamp01(float(mom_fav_pct) / max(0.05, float(dist_pct)))
            hold_score -= 0.20 * clamp01(giveback_frac)
            hold_score = clamp01(hold_score)
            dyn_pct = p_max - ((p_max - p_min) * hold_score)
            return max(p_min, min(p_max, (float(base_pct) + float(dyn_pct)) / 2.0))
        except Exception:
            return max(0.05, min(0.95, float(tp1_partial_pct)))

    @staticmethod
    def finalize_tp1_decision(*, base: TP1Decision, tp2_price: float, force_full_if_no_tp2: bool, prob_strong: float, prob_med: float, tp1_partial_pct: float, tp1_partial_min_pct: float, tp1_partial_max_pct: float) -> TP1Decision:
        mode = SmartDecisionEngine.select_tp1_mode(
            probability=base.probability,
            tp2_price=tp2_price,
            force_full_if_no_tp2=force_full_if_no_tp2,
            prob_strong=prob_strong,
            prob_med=prob_med,
        )
        partial = 0.0
        if mode == "PARTIAL_TP1":
            partial = SmartDecisionEngine.compute_dynamic_partial_pct(
                prob_tp2=base.probability,
                prog_to_tp2=base.progress_to_tp2,
                mom_fav_pct=base.momentum_favor_pct,
                dist_pct=base.distance_to_tp2_pct,
                giveback_frac=base.giveback_fraction,
                tp1_partial_pct=tp1_partial_pct,
                tp1_partial_min_pct=tp1_partial_min_pct,
                tp1_partial_max_pct=tp1_partial_max_pct,
            )
        return TP1Decision(
            mode=mode,
            probability=base.probability,
            progress_to_tp2=base.progress_to_tp2,
            distance_to_tp2_pct=base.distance_to_tp2_pct,
            giveback_fraction=base.giveback_fraction,
            momentum_favor_pct=base.momentum_favor_pct,
            momentum_adverse_pct=base.momentum_adverse_pct,
            partial_pct=partial,
        )

    @staticmethod
    def should_arm_sl(*, sl_price: float, armed_sl: bool) -> bool:
        return (not bool(armed_sl)) and float(sl_price or 0.0) > 0.0

    @staticmethod
    def compute_be_price(*, entry_price: float, current_price: float, last_price: float, direction: str, be_min_pct: float, be_max_pct: float, be_vol_mult: float, be_fee_price: float, profit_lock_enabled: bool, profit_lock_min_pct: float, profit_lock_max_pct: float, tp2_probability: float) -> float:
        entry = float(entry_price or 0.0)
        px = float(current_price or 0.0)
        last_px = float(last_price or px)
        if entry <= 0:
            return 0.0
        vol_pct = abs(px / last_px - 1.0) * 100.0 if last_px > 0 else 0.0
        buf_pct = max(float(be_min_pct), min(float(be_max_pct), float(be_min_pct) + float(be_vol_mult) * float(vol_pct)))
        prob = clamp01(tp2_probability)
        profit_lock_share = max(
            0.0,
            min(
                0.95,
                float(profit_lock_max_pct) - ((float(profit_lock_max_pct) - float(profit_lock_min_pct)) * prob),
            ),
        )
        direction = str(direction or "LONG").upper()
        if direction == "LONG":
            be_dyn = entry * (1.0 + (buf_pct / 100.0))
            be_lock = 0.0
            if profit_lock_enabled and px > entry:
                be_lock = entry + max(0.0, px - entry) * profit_lock_share
            return max(float(be_fee_price or 0.0), be_dyn, be_lock)
        be_dyn = entry * (1.0 - (buf_pct / 100.0))
        be_lock = 0.0
        if profit_lock_enabled and px < entry:
            be_lock = entry - max(0.0, entry - px) * profit_lock_share
        candidates = [be_dyn]
        if float(be_fee_price or 0.0) > 0:
            candidates.append(float(be_fee_price))
        if be_lock > 0:
            candidates.append(be_lock)
        return min(candidates)


class SmartExitEngine:
    @staticmethod
    def evaluate_reversal_exit(*, direction: str, entry_price: float, current_price: float, best_price: float, peak_min_gain_pct: float, reversal_exit_pct: float, tp1_mode: str = "", tp1_partial: bool = False, tp2_probability: float = 0.0, tp1_seen: bool = False) -> ReversalExitDecision:
        direction = str(direction or "LONG").upper()
        entry = float(entry_price or 0.0)
        px = float(current_price or 0.0)
        best_px = float(best_price or px)
        gain_need = float(peak_min_gain_pct)
        retr_need = float(reversal_exit_pct)
        mode = str(tp1_mode or "").upper()
        prob = clamp01(tp2_probability)
        if mode == "HOLD_TO_TP2":
            gain_need *= (1.15 + 0.35 * prob)
            retr_need *= (1.25 + 0.55 * prob)
        elif tp1_partial:
            gain_need *= 1.05
            retr_need *= 1.10
        elif not tp1_seen:
            gain_need *= 0.95
        if entry <= 0 or best_px <= 0:
            return ReversalExitDecision(False, 0.0, 0.0, gain_need, retr_need)
        if direction == "LONG":
            gain_pct = (best_px / entry - 1.0) * 100.0
            retr_pct = (1.0 - (px / best_px)) * 100.0 if best_px > 0 else 0.0
        else:
            gain_pct = (1.0 - (best_px / entry)) * 100.0
            retr_pct = ((px / best_px) - 1.0) * 100.0 if best_px > 0 else 0.0
        return ReversalExitDecision(gain_pct >= gain_need and retr_pct >= retr_need, gain_pct, retr_pct, gain_need, retr_need)

    @staticmethod
    def evaluate_weakness_exit(*, direction: str, entry_price: float, current_price: float, best_price: float, tp1_price: float, tp1_seen: bool, neg_count: int, weakness_enabled: bool, weakness_min_gain_pct: float, weakness_min_giveback_pct: float, weakness_giveback_fraction: float, weakness_min_tp1_progress: float, weakness_tp1_progress_max: float, weakness_neg_hits: int, weakness_min_retain_pct: float) -> WeaknessExitDecision:
        direction = str(direction or "LONG").upper()
        entry = float(entry_price or 0.0)
        px = float(current_price or 0.0)
        best_px = float(best_price or px)
        tp1 = float(tp1_price or 0.0)
        gain_now_pct = 0.0
        best_gain_pct = 0.0
        giveback_pct = 0.0
        giveback_frac = 0.0
        progress_to_tp1 = 0.0
        if entry > 0:
            if direction == "LONG":
                gain_now_pct = (px / entry - 1.0) * 100.0
                if best_px > 0:
                    best_gain_pct = (best_px / entry - 1.0) * 100.0
            else:
                gain_now_pct = (1.0 - (px / entry)) * 100.0
                if best_px > 0:
                    best_gain_pct = (1.0 - (best_px / entry)) * 100.0
        if tp1 > 0 and entry > 0 and tp1 != entry:
            if direction == "LONG":
                progress_to_tp1 = (best_px - entry) / (tp1 - entry)
            else:
                progress_to_tp1 = (entry - best_px) / (entry - tp1)
            progress_to_tp1 = max(0.0, min(1.5, float(progress_to_tp1)))
        if best_gain_pct > 0:
            giveback_pct = max(0.0, best_gain_pct - gain_now_pct)
            giveback_frac = giveback_pct / max(1e-9, best_gain_pct)
        should_close = bool(
            weakness_enabled
            and tp1 > 0
            and (not tp1_seen)
            and gain_now_pct >= max(float(weakness_min_retain_pct), 0.0)
            and best_gain_pct >= float(weakness_min_gain_pct)
            and progress_to_tp1 >= float(weakness_min_tp1_progress)
            and progress_to_tp1 <= float(weakness_tp1_progress_max)
            and giveback_pct >= float(weakness_min_giveback_pct)
            and giveback_frac >= float(weakness_giveback_fraction)
            and int(neg_count or 0) >= max(1, int(weakness_neg_hits))
        )
        reason = (
            f"before_tp1_weakness peak={best_gain_pct:.2f}% now={gain_now_pct:.2f}% "
            f"giveback={giveback_pct:.2f}%({giveback_frac * 100.0:.0f}%) tp1_progress={progress_to_tp1:.2f} neg_hits={int(neg_count or 0)}"
        )
        return WeaknessExitDecision(should_close, best_gain_pct, giveback_pct, giveback_frac, progress_to_tp1, reason)

    @staticmethod
    def evaluate_invalidation_exit(*, direction: str, entry_price: float, current_price: float, best_price: float, tp1_price: float, sl_price: float, tp1_seen: bool, open_age_sec: float, neg_count: int, bos_against: bool, reclaim_loss: bool, vwap_loss: bool, ema20_loss: bool, trend_loss: bool, invalidation_enabled: bool, invalidation_min_bars: int, invalidation_max_bars: int, invalidation_score_close: int, invalidation_score_on_loss: int, invalidation_neg_hits: int, invalidation_min_loss_pct: float, invalidation_max_loss_pct: float, invalidation_sl_frac_cap: float, invalidation_score_override: int = 4) -> InvalidationExitDecision:
        direction = str(direction or "LONG").upper()
        entry = float(entry_price or 0.0)
        px = float(current_price or 0.0)
        best_px = float(best_price or px)
        tp1 = float(tp1_price or 0.0)
        sl = float(sl_price or 0.0)
        bars_since_entry = max(0, int(float(open_age_sec or 0.0) // 300.0))

        gain_now_pct = 0.0
        loss_now_pct = 0.0
        best_gain_pct = 0.0
        progress_to_tp1 = 0.0
        full_sl_loss_pct = 0.0

        if entry > 0:
            if direction == "LONG":
                gain_now_pct = (px / entry - 1.0) * 100.0 if px > 0 else 0.0
                loss_now_pct = max(0.0, (1.0 - (px / entry)) * 100.0) if px > 0 else 0.0
                if best_px > 0:
                    best_gain_pct = max(0.0, (best_px / entry - 1.0) * 100.0)
                if sl > 0 and sl < entry:
                    full_sl_loss_pct = max(0.0, (1.0 - (sl / entry)) * 100.0)
            else:
                gain_now_pct = (1.0 - (px / entry)) * 100.0 if px > 0 else 0.0
                loss_now_pct = max(0.0, (px / entry - 1.0) * 100.0) if px > 0 else 0.0
                if best_px > 0:
                    best_gain_pct = max(0.0, (1.0 - (best_px / entry)) * 100.0)
                if sl > 0 and sl > entry:
                    full_sl_loss_pct = max(0.0, (sl / entry - 1.0) * 100.0)

        if tp1 > 0 and entry > 0 and tp1 != entry:
            try:
                if direction == "LONG":
                    progress_to_tp1 = (best_px - entry) / (tp1 - entry)
                else:
                    progress_to_tp1 = (entry - best_px) / (entry - tp1)
            except Exception:
                progress_to_tp1 = 0.0
            progress_to_tp1 = max(0.0, min(1.5, float(progress_to_tp1)))

        max_early_loss_pct = max(0.0, float(invalidation_max_loss_pct or 0.0))
        sl_frac_cap = max(0.0, float(invalidation_sl_frac_cap or 0.0))
        if full_sl_loss_pct > 0 and sl_frac_cap > 0:
            dyn_cap = full_sl_loss_pct * sl_frac_cap
            max_early_loss_pct = min(max_early_loss_pct, dyn_cap) if max_early_loss_pct > 0 else dyn_cap

        score = 0
        components: list[str] = []
        if bool(bos_against):
            score += 2
            components.append("bos_against")
        if bool(reclaim_loss):
            score += 1
            components.append("reclaim_loss")
        if bool(vwap_loss):
            score += 1
            components.append("vwap_loss")
        if bool(ema20_loss):
            score += 1
            components.append("ema20_loss")
        if bool(trend_loss):
            score += 1
            components.append("trend_loss")

        slow_drift = int(neg_count or 0) >= max(1, int(invalidation_neg_hits or 1))
        if slow_drift:
            score += 1
            components.append("slow_drift")

        min_bars = max(0, int(invalidation_min_bars or 0))
        max_bars = int(invalidation_max_bars or 0)
        within_window = bars_since_entry >= min_bars and (max_bars <= 0 or bars_since_entry <= max_bars)

        soft_cap_ok = (max_early_loss_pct <= 0.0) or (loss_now_pct <= max_early_loss_pct)
        score_close = score >= max(1, int(invalidation_score_close or 1))
        score_on_loss = score >= max(1, int(invalidation_score_on_loss or 1)) and loss_now_pct >= max(0.0, float(invalidation_min_loss_pct or 0.0))
        override_close = False
        if score >= max(1, int(invalidation_score_override or 1)):
            if full_sl_loss_pct > 0:
                override_close = loss_now_pct < max(max_early_loss_pct, full_sl_loss_pct * 0.92)
            else:
                override_close = soft_cap_ok

        should_close = bool(
            invalidation_enabled
            and (not tp1_seen)
            and within_window
            and ((score_close and soft_cap_ok) or (score_on_loss and soft_cap_ok) or override_close)
        )

        reason = (
            f"post_entry_invalidation score={score} bars={bars_since_entry} "
            f"loss={loss_now_pct:.2f}% cap={max_early_loss_pct:.2f}% full_sl={full_sl_loss_pct:.2f}% "
            f"progress={progress_to_tp1:.2f} neg_hits={int(neg_count or 0)} best_gain={best_gain_pct:.2f}% "
            f"flags={','.join(components) if components else 'none'}"
        )
        return InvalidationExitDecision(
            should_close,
            score,
            loss_now_pct,
            gain_now_pct,
            full_sl_loss_pct,
            max_early_loss_pct,
            progress_to_tp1,
            bars_since_entry,
            ",".join(components),
            reason,
        )

    @staticmethod
    def evaluate_structural_sl(*, direction: str, entry_price: float, current_price: float, sl_price: float, hard_sl_price: float, now_ts: float, sl_hits: int, sl_first_ts: float, sl_extreme: float, sl_confirm_hits: int, sl_confirm_sec: float, sl_deep_pct: float, sl_grace_sec: float, sl_reclaim_pct: float, sl_bounce_pct: float, allow_struct_close: bool = True) -> StructuralSLDecision:
        direction = str(direction or "LONG").upper()
        entry = float(entry_price or 0.0)
        px = float(current_price or 0.0)
        sl_struct = float(sl_price or 0.0)
        sl_hard = float(hard_sl_price or 0.0)
        now = float(now_ts or 0.0)
        hits = int(sl_hits or 0)
        first_ts = float(sl_first_ts or 0.0)
        extreme = float(sl_extreme or 0.0)

        def hit_hard() -> bool:
            if sl_hard <= 0:
                return False
            return (px <= sl_hard) if direction == "LONG" else (px >= sl_hard)

        def hit_struct() -> bool:
            if sl_struct <= 0:
                return False
            return (px <= sl_struct) if direction == "LONG" else (px >= sl_struct)

        def reclaimed() -> bool:
            if sl_struct <= 0:
                return False
            if direction == "LONG":
                return px >= sl_struct * (1.0 + float(sl_reclaim_pct) / 100.0)
            return px <= sl_struct * (1.0 - float(sl_reclaim_pct) / 100.0)

        def deep_breach() -> bool:
            if sl_struct <= 0 or entry <= 0:
                return False
            d = (sl_struct - px) if direction == "LONG" else (px - sl_struct)
            if d <= 0:
                return False
            return (d / entry) * 100.0 >= float(sl_deep_pct)

        crossed = hit_struct()
        if crossed:
            if first_ts <= 0:
                first_ts = now
                extreme = px
            if direction == "LONG":
                extreme = min(extreme, px) if extreme else px
            else:
                extreme = max(extreme, px) if extreme else px
            hits += 1
        else:
            hits = 0
            first_ts = 0.0
            extreme = 0.0

        age = (now - first_ts) if first_ts > 0 else 0.0
        bounced = False
        if crossed and extreme and entry > 0:
            if direction == "LONG":
                bounced = ((px - extreme) / entry) * 100.0 >= float(sl_bounce_pct)
            else:
                bounced = ((extreme - px) / entry) * 100.0 >= float(sl_bounce_pct)

        hard = hit_hard()
        deep = crossed and deep_breach()
        recl = crossed and reclaimed()
        should_close = False
        if hard or deep:
            should_close = True
        elif crossed:
            if age <= float(sl_grace_sec) and (recl or bounced):
                hits = 0
                first_ts = 0.0
                extreme = 0.0
            elif hits >= int(sl_confirm_hits):
                min_age = min(float(sl_confirm_sec), float(sl_grace_sec)) if float(sl_grace_sec) > 0 else float(sl_confirm_sec)
                should_close = age >= max(0.0, float(min_age)) and bool(allow_struct_close)
            elif first_ts > 0:
                should_close = age >= max(0.0, float(sl_confirm_sec)) and age >= max(0.0, float(sl_grace_sec)) and bool(allow_struct_close)

        return StructuralSLDecision(should_close, crossed, hard, deep, recl, bounced, age, hits, first_ts, extreme)

    @staticmethod
    def evaluate_tp2_runner(*, direction: str, current_price: float, runner_floor_price: float, runner_peak_price: float, now_ts: float, armed_ts: float, min_hold_sec: float, min_peak_extension_pct: float, peak_giveback_pct: float) -> TP2RunnerDecision:
        direction = str(direction or "LONG").upper()
        px = float(current_price or 0.0)
        floor_px = float(runner_floor_price or 0.0)
        peak_px = float(runner_peak_price or px)
        ready = (float(now_ts or 0.0) - float(armed_ts or 0.0)) >= max(0.0, float(min_hold_sec or 0.0))
        if direction == "LONG":
            peak_extension_pct = max(0.0, (peak_px / max(1e-9, floor_px) - 1.0) * 100.0) if floor_px > 0 else 0.0
            giveback_pct = max(0.0, (1.0 - (px / max(1e-9, peak_px))) * 100.0) if peak_px > 0 else 0.0
            back_to_tp2 = floor_px > 0 and px <= floor_px
        else:
            peak_extension_pct = max(0.0, (1.0 - (peak_px / max(1e-9, floor_px))) * 100.0) if floor_px > 0 else 0.0
            giveback_pct = max(0.0, (px / max(1e-9, peak_px) - 1.0) * 100.0) if peak_px > 0 else 0.0
            back_to_tp2 = floor_px > 0 and px >= floor_px
        if back_to_tp2:
            return TP2RunnerDecision(True, "tp2_return", peak_extension_pct, giveback_pct, True)
        if ready and peak_extension_pct >= float(min_peak_extension_pct or 0.0) and giveback_pct >= float(peak_giveback_pct or 0.0):
            return TP2RunnerDecision(True, "tp2_peak_giveback", peak_extension_pct, giveback_pct, False)
        return TP2RunnerDecision(False, "", peak_extension_pct, giveback_pct, back_to_tp2)
