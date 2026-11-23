"""
mp.py - Real-time Python API client & streaming engine for MindPillar CVD (/cvd/btc/).

Reverse-engineered via REA (reverse-engineer-anything) from https://mindpillar.com/cvd/btc/.
All endpoints are public and require zero authentication or API keys.

Backends:
  - Candles / CVD / OI / Footprint / Zones / Levels / Volume Profiles:
      REST: https://candles.bitcoinsensus.com
      WS:   wss://candles.bitcoinsensus.com/ws?ticket=<uuid>  (from GET /ws/ticket)
  - Liquidation Heatmap & Map Tensor (7 exchanges, 6h..180d):
      CDN:  https://charts.prd.bitcoinsensus.com/v1/liq-tensor/{SYMBOL}_{INTERVAL}.json
  - Live Forced Liquidations (direct exchange public WS):
      Binance USDT-M: wss://fstream.binance.com/ws/{symbol}@forceOrder
      Binance COIN-M: wss://dstream.binance.com/ws/{symbol}@forceOrder
      Bybit Linear:   wss://stream.bybit.com/v5/public/linear  (allLiquidation.{SYMBOL})
      Bybit Inverse:  wss://stream.bybit.com/v5/public/inverse (allLiquidation.{SYMBOL})

REST methods use Python stdlib only. Real-time streaming (`MindPillarLiveFeed`) uses `websockets`.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import gzip
import json
import math
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

CANDLES_API = "https://candles.bitcoinsensus.com"
CHARTS_CDN = "https://charts.prd.bitcoinsensus.com"
ORIGIN = "https://mindpillar.com"
REFERER = "https://mindpillar.com/"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
)


class MindPillarError(Exception):
    """Base error for MindPillar API failures."""


class MindPillarHTTPError(MindPillarError):
    def __init__(self, url: str, status: int, body: str = ""):
        self.url = url
        self.status = status
        self.body = body
        super().__init__(f"HTTP {status} on {url}: {body[:500]}")


@dataclass(frozen=True)
class MarketSpec:
    id: str
    key: str
    label: str
    ticker: str
    asset: str
    venue: str
    unit: str  # "base" (qty in coin -> * close for USD) or "usd" (qty already in USD)
    kind: str = "perp"  # "perp" or "spot"


# Exact market definitions from MindPillar's shipped bundle (`dg` & `Cg`)
PERP_MARKETS: Tuple[MarketSpec, ...] = (
    # BTC
    MarketSpec("binance-btc", "BINANCE_FUTURES:btcusdt", "Binance USDT", "BTCUSDT.P", "btc", "binance", "base", "perp"),
    MarketSpec("bybit-btc", "BYBIT:BTCUSDT", "ByBit USDT", "BTCUSDT.P", "btc", "bybit", "base", "perp"),
    MarketSpec("binance_coinm-btc", "BINANCE_FUTURES:btcusd_perp", "Binance USD", "BTCUSD.P", "btc", "binance_coinm", "usd", "perp"),
    MarketSpec("bybit_inverse-btc", "BYBIT:BTCUSD", "ByBit USD", "BTCUSD.P", "btc", "bybit_inverse", "usd", "perp"),
    # ETH
    MarketSpec("binance-eth", "BINANCE_FUTURES:ethusdt", "Binance USDT", "ETHUSDT.P", "eth", "binance", "base", "perp"),
    MarketSpec("bybit-eth", "BYBIT:ETHUSDT", "ByBit USDT", "ETHUSDT.P", "eth", "bybit", "base", "perp"),
    MarketSpec("binance_coinm-eth", "BINANCE_FUTURES:ethusd_perp", "Binance USD", "ETHUSD.P", "eth", "binance_coinm", "usd", "perp"),
    MarketSpec("bybit_inverse-eth", "BYBIT:ETHUSD", "ByBit USD", "ETHUSD.P", "eth", "bybit_inverse", "usd", "perp"),
    # SOL
    MarketSpec("binance-sol", "BINANCE_FUTURES:solusdt", "Binance USDT", "SOLUSDT.P", "sol", "binance", "base", "perp"),
    MarketSpec("bybit-sol", "BYBIT:SOLUSDT", "ByBit USDT", "SOLUSDT.P", "sol", "bybit", "base", "perp"),
    MarketSpec("binance_coinm-sol", "BINANCE_FUTURES:solusd_perp", "Binance USD", "SOLUSD.P", "sol", "binance_coinm", "usd", "perp"),
    MarketSpec("bybit_inverse-sol", "BYBIT:SOLUSD", "ByBit USD", "SOLUSD.P", "sol", "bybit_inverse", "usd", "perp"),
)

SPOT_MARKETS: Tuple[MarketSpec, ...] = (
    MarketSpec("binance_spot-btc", "BINANCE:btcusdt", "Binance Spot", "BTCUSDT", "btc", "binance_spot", "base", "spot"),
    MarketSpec("bybit_spot-btc", "BYBIT_SPOT:BTCUSDT", "ByBit Spot", "BTCUSDT", "btc", "bybit_spot", "base", "spot"),
    MarketSpec("binance_spot-eth", "BINANCE:ethusdt", "Binance Spot", "ETHUSDT", "eth", "binance_spot", "base", "spot"),
    MarketSpec("bybit_spot-eth", "BYBIT_SPOT:ETHUSDT", "ByBit Spot", "ETHUSDT", "eth", "bybit_spot", "base", "spot"),
)

ALL_MARKETS: Tuple[MarketSpec, ...] = PERP_MARKETS + SPOT_MARKETS
MARKETS_BY_ID: Dict[str, MarketSpec] = {m.id: m for m in ALL_MARKETS}
MARKETS_BY_KEY: Dict[str, MarketSpec] = {m.key.lower(): m for m in ALL_MARKETS}

# Exact 17 timeframes (`Wh` in shipped JS)
TIMEFRAMES: Dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "3h": 10_800_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "2D": 172_800_000,
    "3D": 259_200_000,
    "1w": 604_800_000,
    "1M": 2_592_000_000,
}

# Case-insensitive timeframe lookup -> canonical wire label
_TF_ALIASES: Dict[str, str] = {k.lower(): k for k in TIMEFRAMES}
_TF_ALIASES["1mo"] = "1M"

LIQ_TENSOR_INTERVALS: Tuple[str, ...] = ("6h", "12h", "1d", "2d", "3d", "7d", "14d", "30d", "90d", "180d")
ZONES_PERIODS: Tuple[str, ...] = ("day", "week", "month", "year")
LEVEL_FAMILIES: Tuple[str, ...] = ("npoc", "tvpoc", "dlevels", "wlevels", "mlevels", "ob")

# Footprint default tick steps (`bg` / `hg` * `_g`)
DEFAULT_FOOTPRINT_STEP: Dict[str, float] = {"btc": 25.0, "eth": 2.5, "sol": 0.25}
DEFAULT_PROFILE_STEP: Dict[str, float] = {"btc": 25.0, "eth": 2.5, "sol": 0.25}

# Direct exchange forced liquidation streams
FORCE_ORDER_CONFIG: Dict[str, Dict[str, Any]] = {
    "btc": {
        "binance_linear": "wss://fstream.binance.com/ws/btcusdt@forceOrder",
        "binance_coinm": "wss://dstream.binance.com/ws/btcusd_perp@forceOrder",
        "coinm_contract_usd": 100.0,
        "bybit_linear_topic": "allLiquidation.BTCUSDT",
        "bybit_inverse_topic": "allLiquidation.BTCUSD",
    },
    "eth": {
        "binance_linear": "wss://fstream.binance.com/ws/ethusdt@forceOrder",
        "binance_coinm": "wss://dstream.binance.com/ws/ethusd_perp@forceOrder",
        "coinm_contract_usd": 10.0,
        "bybit_linear_topic": "allLiquidation.ETHUSDT",
        "bybit_inverse_topic": "allLiquidation.ETHUSD",
    },
    "sol": {
        "binance_linear": "wss://fstream.binance.com/ws/solusdt@forceOrder",
        "binance_coinm": "wss://dstream.binance.com/ws/solusd_perp@forceOrder",
        "coinm_contract_usd": 10.0,
        "bybit_linear_topic": "allLiquidation.SOLUSDT",
        "bybit_inverse_topic": "allLiquidation.SOLUSD",
    },
}


def resolve_tf(tf: str | int) -> Tuple[str, int]:
    """Return canonical `(wire_label, tf_ms)` for a timeframe string (e.g. '15m', '1d') or ms int."""
    if isinstance(tf, int):
        for label, ms in TIMEFRAMES.items():
            if ms == tf:
                return label, ms
        raise ValueError(f"Unsupported timeframe ms: {tf}. Supported: {sorted(TIMEFRAMES.values())}")
    raw = tf.strip()
    if raw in TIMEFRAMES:
        return raw, TIMEFRAMES[raw]
    canon = _TF_ALIASES.get(raw.lower())
    if canon:
        return canon, TIMEFRAMES[canon]
    raise ValueError(f"Unsupported timeframe '{tf}'. Supported: {list(TIMEFRAMES.keys())}")


def resolve_market(market: str | MarketSpec) -> MarketSpec:
    """Resolve market id (e.g. 'binance-btc'), key (e.g. 'BINANCE_FUTURES:btcusdt'), or MarketSpec."""
    if isinstance(market, MarketSpec):
        return market
    key = market.strip()
    if key in MARKETS_BY_ID:
        return MARKETS_BY_ID[key]
    low = key.lower()
    if low in MARKETS_BY_KEY:
        return MARKETS_BY_KEY[low]
    # Convenience aliases for BTC
    aliases = {
        "binance": "binance-btc",
        "binance_usdt": "binance-btc",
        "binance_usd": "binance_coinm-btc",
        "binance_coinm": "binance_coinm-btc",
        "bybit": "bybit-btc",
        "bybit_usdt": "bybit-btc",
        "bybit_usd": "bybit_inverse-btc",
        "bybit_inverse": "bybit_inverse-btc",
        "binance_spot": "binance_spot-btc",
        "bybit_spot": "bybit_spot-btc",
    }
    if low in aliases:
        return MARKETS_BY_ID[aliases[low]]
    raise ValueError(f"Unknown market '{market}'. Valid ids: {list(MARKETS_BY_ID.keys())}")


def asset_markets(asset: str = "btc", include_spot: bool = False) -> List[MarketSpec]:
    """Return ordered list of markets for an asset ('btc', 'eth', 'sol')."""
    a = asset.strip().lower()
    perps = [m for m in PERP_MARKETS if m.asset == a]
    if not perps:
        raise ValueError(f"Unsupported asset '{asset}'. Supported: btc, eth, sol")
    if include_spot:
        return perps + [m for m in SPOT_MARKETS if m.asset == a]
    return perps


def align_bar_ms(tf_ms: int, ts_ms: int) -> int:
    """Align epoch milliseconds to MindPillar bar boundary (`ig` in shipped JS)."""
    if tf_ms == 604_800_000:  # 1w (Monday UTC: epoch + 4d)
        week = 604_800_000
        offset = 4 * 86_400_000
        return ((ts_ms - offset) // week) * week + offset
    if tf_ms == 2_592_000_000:  # 1M (1st of UTC month)
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return int(datetime(dt.year, dt.month, 1, tzinfo=timezone.utc).timestamp() * 1000)
    if tf_ms in (172_800_000, 259_200_000):  # 2D / 3D year-grid
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        jan1 = int(datetime(dt.year, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        return jan1 + ((ts_ms - jan1) // tf_ms) * tf_ms
    return (ts_ms // tf_ms) * tf_ms


# ---------------------------------------------------------------------------
# Exact MindPillar CVD Math (ported verbatim from `Yk`, `Xk`, `Zk`, `$k`, `eA`)
# ---------------------------------------------------------------------------

def bar_buy_sell_delta(candle: Dict[str, Any], market: MarketSpec) -> Dict[str, float]:
    """Compute exact MindPillar USD and Base buy, sell, volume, and delta for one bar."""
    close = float(candle.get("close") or 0.0)
    vbuy = float(candle.get("vbuy") or 0.0)
    vsell = float(candle.get("vsell") or 0.0)
    raw_delta = vbuy - vsell
    raw_vol = vbuy + vsell

    if market.unit == "base":
        buy_base, sell_base, delta_base, vol_base = vbuy, vsell, raw_delta, raw_vol
        buy_usd, sell_usd, delta_usd, vol_usd = vbuy * close, vsell * close, raw_delta * close, raw_vol * close
    else:
        buy_usd, sell_usd, delta_usd, vol_usd = vbuy, vsell, raw_delta, raw_vol
        inv = (1.0 / close) if close > 0 else 0.0
        buy_base, sell_base, delta_base, vol_base = vbuy * inv, vsell * inv, raw_delta * inv, raw_vol * inv

    return {
        "buy_usd": buy_usd,
        "sell_usd": sell_usd,
        "volume_usd": vol_usd,
        "delta_usd": delta_usd,
        "buy_base": buy_base,
        "sell_base": sell_base,
        "volume_base": vol_base,
        "delta_base": delta_base,
    }


def compute_cvd_series(
    times: Sequence[int],
    deltas: Sequence[float],
    mode: str = "all",
    rolling_bars: int = 96,
) -> List[Dict[str, Any]]:
    """Compute cumulative volume delta series (`Zk` in shipped JS).

    Modes:
      - 'all' (or 'continuous' / 'infinite'): cumulative sum across the loaded window (MindPillar 'CVD ∞')
      - 'daily': resets to 0 at 00:00 UTC each day (MindPillar 'CVD 1D')
      - 'rolling': shifted so value at (len - 1 - rolling_bars) is 0 (MindPillar 'CVD 24H')
    """
    n = len(times)
    if n == 0:
        return []
    m = mode.lower()
    if m == "rolling":
        prefix = [0.0] * n
        acc = 0.0
        for i in range(n):
            acc += deltas[i]
            prefix[i] = acc
        anchor_idx = n - 1 - int(rolling_bars)
        base = prefix[anchor_idx] if anchor_idx >= 0 else 0.0
        return [{"time": times[i], "value": prefix[i] - base} for i in range(n)]
    if m == "daily":
        out: List[Dict[str, Any]] = []
        acc = 0.0
        cur_day = ""
        for i in range(n):
            day = datetime.fromtimestamp(times[i] / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
            if day != cur_day:
                acc = 0.0
                cur_day = day
            acc += deltas[i]
            out.append({"time": times[i], "value": acc})
        return out
    out = []
    acc = 0.0
    for i in range(n):
        acc += deltas[i]
        out.append({"time": times[i], "value": acc})
    return out


def compute_market_cvd(
    candles: Sequence[Dict[str, Any]],
    market: MarketSpec,
    unit: str = "usd",
    mode: str = "all",
    rolling_bars: int = 96,
) -> List[Dict[str, Any]]:
    """Compute per-market CVD series matching MindPillar `Qk`."""
    key = "delta_usd" if unit == "usd" else "delta_base"
    times = [int(c["time"]) for c in candles]
    deltas = [bar_buy_sell_delta(c, market)[key] for c in candles]
    return compute_cvd_series(times, deltas, mode=mode, rolling_bars=rolling_bars)


def compute_aggregate_cvd(
    candles_by_market: Dict[str, List[Dict[str, Any]]],
    markets: Sequence[MarketSpec],
    unit: str = "usd",
    mode: str = "all",
    rolling_bars: int = 96,
) -> List[Dict[str, Any]]:
    """Compute Aggregate CVD across multiple venues matching MindPillar `$k` + `eA`."""
    ref: List[Dict[str, Any]] = []
    for m in markets:
        arr = candles_by_market.get(m.id, [])
        if len(arr) > len(ref):
            ref = arr
    if not ref:
        return []
    key = "delta_usd" if unit == "usd" else "delta_base"
    delta_maps: Dict[str, Dict[int, float]] = {}
    for m in markets:
        dmap: Dict[int, float] = {}
        for c in candles_by_market.get(m.id, []):
            dmap[int(c["time"])] = bar_buy_sell_delta(c, m)[key]
        delta_maps[m.id] = dmap

    times = [int(c["time"]) for c in ref]
    deltas = [sum(delta_maps[m.id].get(t, 0.0) for m in markets) for t in times]
    return compute_cvd_series(times, deltas, mode=mode, rolling_bars=rolling_bars)


# ---------------------------------------------------------------------------
# Exact MindPillar Auto-Divergence Engine (ported from `Mc`, `Iee`, `Pc`, `Ac`, `kc`)
# ---------------------------------------------------------------------------

def _calc_atr(candles: Sequence[Dict[str, Any]], period: int = 14) -> List[float]:
    n = len(candles)
    out = [0.0] * n
    buf: List[float] = []
    acc = 0.0
    for i, c in enumerate(candles):
        hi, lo = float(c["high"]), float(c["low"])
        if i == 0:
            tr = hi - lo
        else:
            prev_c = float(candles[i - 1]["close"])
            tr = max(hi - lo, abs(hi - prev_c), abs(lo - prev_c))
        buf.append(tr)
        acc += tr
        if len(buf) > period:
            acc -= buf[len(buf) - 1 - period]
        out[i] = acc / min(len(buf), period)
    return out


def _fractal_highs(series: Sequence[float], window: int, min_dist: float) -> List[Dict[str, Any]]:
    n = len(series)
    if n < 2 * window + 1:
        return []
    out = []
    for i in range(window, n - window):
        left_max = max(series[i - window : i])
        right_max = max(series[i + 1 : i + window + 1])
        if series[i] > left_max and series[i] > right_max and min(series[i] - left_max, series[i] - right_max) >= min_dist:
            out.append({"barIndex": i, "price": series[i]})
    return out


def _fractal_lows(series: Sequence[float], window: int, min_dist: float) -> List[Dict[str, Any]]:
    n = len(series)
    if n < 2 * window + 1:
        return []
    out = []
    for i in range(window, n - window):
        left_min = min(series[i - window : i])
        right_min = min(series[i + 1 : i + window + 1])
        if series[i] < left_min and series[i] < right_min and min(left_min - series[i], right_min - series[i]) >= min_dist:
            out.append({"barIndex": i, "price": series[i]})
    return out


def _zigzag_pivots(candles: Sequence[Dict[str, Any]], atr: Sequence[float], atr_mult: float) -> Dict[str, Any]:
    n = len(candles)
    highs: List[Dict[str, Any]] = []
    lows: List[Dict[str, Any]] = []
    if n < 3:
        return {"highs": highs, "lows": lows, "pendingHigh": None, "pendingLow": None}
    trend = 0
    hi_p, hi_i = float(candles[0]["high"]), 0
    lo_p, lo_i = float(candles[0]["low"]), 0

    def mk(idx: int, price: float, lock: Optional[int] = None) -> Dict[str, Any]:
        d: Dict[str, Any] = {"barIndex": idx, "price": price, "timeMs": int(candles[idx]["time"])}
        if lock is not None:
            d["lockBarIndex"] = lock
        return d

    for i in range(1, n):
        h = float(candles[i]["high"])
        l = float(candles[i]["low"])
        if h > hi_p:
            hi_p, hi_i = h, i
        if l < lo_p:
            lo_p, lo_i = l, i
        thresh = atr_mult * atr[i]
        if trend >= 0 and (hi_p - l) >= thresh and hi_i < i:
            if hi_i > 0:
                highs.append(mk(hi_i, hi_p, i))
            trend = -1
            lo_p, lo_i = l, i
            hi_p, hi_i = h, i
        elif trend <= 0 and (h - lo_p) >= thresh and lo_i < i:
            if lo_i > 0:
                lows.append(mk(lo_i, lo_p, i))
            trend = 1
            hi_p, hi_i = h, i
            lo_p, lo_i = l, i

    return {
        "highs": highs,
        "lows": lows,
        "pendingHigh": mk(hi_i, hi_p) if trend >= 0 else None,
        "pendingLow": mk(lo_i, lo_p) if trend <= 0 else None,
    }


def _line_crossed(
    i1: int, p1: float, i2: int, p2: float, series: Sequence[float], bears: bool, tol: float
) -> bool:
    span = i2 - i1
    if span == 0:
        return False
    slope = (p2 - p1) / span
    for j in range(i1 + 1, i2):
        line_val = p1 + slope * (j - i1)
        if bears:
            if line_val + tol < series[j]:
                return True
        else:
            if line_val - tol > series[j]:
                return True
    return False


def _detect_sfp(
    pair: Dict[str, Any], candles: Sequence[Dict[str, Any]], bears: bool, resolved_idx: int, cfg: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    anchor_p = pair["prev"]["price"]
    n = len(candles)
    limit = min(resolved_idx + cfg["sfpReclaimLen"] + 1, n)
    extreme = -math.inf if bears else math.inf
    reclaim_idx = -1
    for j in range(resolved_idx, limit):
        c = candles[j]
        if bears:
            if c["high"] > extreme:
                extreme = c["high"]
            if c["close"] < anchor_p:
                reclaim_idx = j
                break
        else:
            if c["low"] < extreme:
                extreme = c["low"]
            if c["close"] > anchor_p:
                reclaim_idx = j
                break
    if reclaim_idx < 0:
        return None
    struct_ext = math.inf if bears else -math.inf
    for j in range(pair["prev"]["barIndex"], resolved_idx + 1):
        v = candles[j]["low"] if bears else candles[j]["high"]
        if (bears and v < struct_ext) or (not bears and v > struct_ext):
            struct_ext = v
    rng = (extreme - struct_ext) if bears else (struct_ext - extreme)
    if rng <= 0:
        return None
    t618 = (extreme - 0.618 * rng) if bears else (extreme + 0.618 * rng)
    t786 = (extreme - 0.786 * rng) if bears else (extreme + 0.786 * rng)
    hit618 = False
    hit786 = False
    failed_ms = None
    scan_end = min(reclaim_idx + 1 + cfg["scanLen"], n)
    for j in range(reclaim_idx + 1, scan_end):
        c = candles[j]
        if (bears and c["high"] > extreme) or (not bears and c["low"] < extreme):
            failed_ms = int(c["time"])
            break
        if (bears and c["low"] <= t618) or (not bears and c["high"] >= t618):
            hit618 = True
        if (bears and c["low"] <= t786) or (not bears and c["high"] >= t786):
            hit786 = True
            break
    return {
        "reclaimTimeMs": int(candles[reclaim_idx]["time"]),
        "barsToReclaim": reclaim_idx - resolved_idx,
        "sfpExtreme": extreme,
        "structureExtreme": struct_ext,
        "target618": t618,
        "target786": t786,
        "hitTarget618": hit618,
        "hitTarget786": hit786,
        "failedTimeMs": failed_ms,
    }


def _resolve_divergence(
    pair: Dict[str, Any], candles: Sequence[Dict[str, Any]], bears: bool, cfg: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    if pair["curr"].get("pending") or pair["prev"].get("pending"):
        return {"status": "pending", "resolvedIndex": -1, "mfePct": 0.0, "maePct": 0.0, "sfp": None}
    i1 = pair["prev"]["barIndex"]
    i2 = pair["curr"]["barIndex"]
    curr_p = pair["curr"]["price"]
    between_ext = math.inf if bears else -math.inf
    for j in range(i1 + 1, i2):
        v = candles[j]["low"] if bears else candles[j]["high"]
        if (bears and v < between_ext) or (not bears and v > between_ext):
            between_ext = v

    lock_idx = pair["curr"].get("lockBarIndex")
    if lock_idx is not None:
        for j in range(i2 + 1, min(lock_idx + 1, len(candles))):
            c = candles[j]
            if bears:
                if c["low"] < between_ext or c["high"] > pair["prev"]["price"]:
                    return None
            else:
                if c["high"] > between_ext or c["low"] < pair["prev"]["price"]:
                    return None

    start = (i2 + 1) if lock_idx is None else (max(i2, lock_idx) + 1)
    end = min(i2 + 1 + cfg["scanLen"], len(candles))
    status = "pending"
    resolved_idx = -1
    mfe = 0.0
    mae = 0.0
    for j in range(start, end):
        c = candles[j]
        fav = (curr_p - c["low"]) if bears else (c["high"] - curr_p)
        adv = (c["high"] - curr_p) if bears else (curr_p - c["low"])
        if fav > mfe:
            mfe = fav
        if adv > mae:
            mae = adv
        if bears:
            if c["low"] < between_ext:
                status, resolved_idx = "confirmed", j
                break
            if c["high"] > pair["prev"]["price"]:
                status, resolved_idx = "rejected", j
                break
        else:
            if c["high"] > between_ext:
                status, resolved_idx = "confirmed", j
                break
            if c["low"] < pair["prev"]["price"]:
                status, resolved_idx = "rejected", j
                break

    mfe_pct = 0.0 if curr_p == 0 else (mfe / curr_p) * 100.0
    mae_pct = 0.0 if curr_p == 0 else (mae / curr_p) * 100.0
    sfp = _detect_sfp(pair, candles, bears, resolved_idx, cfg) if status == "rejected" else None
    return {
        "status": status,
        "resolvedIndex": resolved_idx,
        "referenceLevel": between_ext if math.isfinite(between_ext) else None,
        "mfePct": mfe_pct,
        "maePct": mae_pct,
        "sfp": sfp,
    }


def _remove_inner_divs(divs: List[Dict[str, Any]], window_size: int) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for kind in ("bearish", "bullish"):
        subset = [d for d in divs if d["kind"] == kind]
        settled = [d for d in subset if not d["prev"].get("pending") and not d["curr"].get("pending")]
        pending = [d for d in subset if d["prev"].get("pending") or d["curr"].get("pending")]

        def dedupe(arr: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            by_curr: Dict[int, Dict[str, Any]] = {}
            for d in arr:
                ci = d["curr"]["barIndex"]
                ex = by_curr.get(ci)
                if ex is None or (
                    abs(d["cvdDelta"]) > abs(ex["cvdDelta"])
                    if d["prev"]["barIndex"] == ex["prev"]["barIndex"]
                    else d["prev"]["barIndex"] < ex["prev"]["barIndex"]
                ):
                    by_curr[ci] = d
            return list(by_curr.values())

        def resolve_bar(d: Dict[str, Any]) -> float:
            return math.inf if d["barsToResolve"] is None else (d["curr"]["barIndex"] + d["barsToResolve"])

        kept: List[Dict[str, Any]] = []
        for d in sorted(dedupe(settled), key=lambda x: x["curr"]["barIndex"]):
            lock = d["curr"].get("lockBarIndex", d["curr"]["barIndex"] + window_size)
            if not any(
                d["prev"]["barIndex"] >= k["prev"]["barIndex"]
                and d["curr"]["barIndex"] <= k["curr"]["barIndex"]
                and resolve_bar(k) > lock
                for k in kept
            ):
                kept.append(d)
        result.extend(kept)
        for d in dedupe(pending):
            if not any(
                d["prev"]["barIndex"] >= k["prev"]["barIndex"]
                and d["curr"]["barIndex"] <= k["curr"]["barIndex"]
                and resolve_bar(k) > d["curr"]["barIndex"]
                for k in kept
            ):
                result.append(d)
    return result


def _detect_single_cvd_divergences(
    candles: Sequence[Dict[str, Any]],
    cvd_series: Sequence[Dict[str, Any]],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    if not candles or not cvd_series:
        return {"divergences": [], "highPivots": [], "lowPivots": []}
    cvd_by_time = {int(pt["time"]): float(pt["value"]) for pt in cvd_series}
    n = len(candles)
    cvd_arr: List[Optional[float]] = [cvd_by_time.get(int(c["time"])) for c in candles]

    def sample_cvd(idx: int, is_high: bool) -> Optional[float]:
        if cfg["cvdSampling"] == "pivot":
            return cvd_arr[idx]
        lo = max(0, idx - cfg["windowSize"])
        hi = min(n - 1, idx + cfg["windowSize"])
        best = None
        for j in range(lo, hi + 1):
            v = cvd_arr[j]
            if v is not None and (best is None or (v > best if is_high else v < best)):
                best = v
        return best

    highs_s = [float(c["high"]) for c in candles]
    lows_s = [float(c["low"]) for c in candles]

    def enrich(pivs: Sequence[Dict[str, Any]], is_high: bool) -> List[Dict[str, Any]]:
        out = []
        for p in pivs:
            idx = p["barIndex"]
            cv = sample_cvd(idx, is_high)
            if cv is not None:
                item: Dict[str, Any] = {
                    "barIndex": idx,
                    "price": p["price"],
                    "timeMs": int(candles[idx]["time"]),
                    "cvd": cv,
                }
                if "lockBarIndex" in p:
                    item["lockBarIndex"] = p["lockBarIndex"]
                out.append(item)
        return out

    cap = lambda arr: arr[-cfg["lastPivotCount"] :] if len(arr) > cfg["lastPivotCount"] else arr
    atr = None
    if cfg["pivotMode"] == "zigzag":
        atr = _calc_atr(candles, cfg["zigzagAtrPeriod"])
        zz = _zigzag_pivots(candles, atr, cfg["zigzagAtrMult"])
        hi_pivs = cap(enrich(zz["highs"], True))
        lo_pivs = cap(enrich(zz["lows"], False))
        for target, pend, is_h in ((hi_pivs, zz["pendingHigh"], True), (lo_pivs, zz["pendingLow"], False)):
            if pend:
                last_i = target[-1]["barIndex"] if target else -1
                if pend["barIndex"] > last_i:
                    cv = sample_cvd(pend["barIndex"], is_h)
                    if cv is not None:
                        target.append({**pend, "cvd": cv, "pending": True})
    else:
        hi_pivs = cap(enrich(_fractal_highs(highs_s, cfg["windowSize"], cfg["minDistance"]), True))
        lo_pivs = cap(enrich(_fractal_lows(lows_s, cfg["windowSize"], cfg["minDistance"] + 1), False))
        # Append pending tail pivot (`Ec`)
        for target, ser, is_h in ((hi_pivs, highs_s, True), (lo_pivs, lows_s, False)):
            last_i = target[-1]["barIndex"] if target else -10**9
            if last_i < n - 1 - cfg["windowSize"]:
                start = max(n - cfg["windowSize"], last_i + 1)
                if start < n:
                    best_i = start
                    best_v = ser[start]
                    for j in range(start + 1, n):
                        if (is_h and ser[j] > best_v) or (not is_h and ser[j] < best_v):
                            best_v, best_i = ser[j], j
                    cv = sample_cvd(best_i, is_h)
                    if cv is not None:
                        target.append(
                            {"barIndex": best_i, "price": best_v, "cvd": cv, "timeMs": int(candles[best_i]["time"]), "pending": True}
                        )

    def pair_side(bears: bool, pivs: List[Dict[str, Any]], ser: Sequence[float]) -> List[Dict[str, Any]]:
        m = len(pivs)
        if m < 2:
            return []
        use_gap = cfg["pairRule"] == "gap-else-prev" and cfg["pivotMode"] == "zigzag" and atr is not None
        raw_pairs: List[Dict[str, Any]] = []
        for idx in range(m - 1, 0, -1):
            curr = pivs[idx]
            best_prev_p = None
            floor = max(0, idx - cfg["engulfLen"])
            local: List[Dict[str, Any]] = []
            for j in range(idx - 1, floor - 1, -1):
                prev = pivs[j]
                if best_prev_p is not None and ((bears and prev["price"] < best_prev_p) or (not bears and prev["price"] > best_prev_p)):
                    continue
                price_ok = (curr["price"] < prev["price"]) if bears else (curr["price"] > prev["price"])
                cvd_ok = (curr["cvd"] > prev["cvd"]) if bears else (curr["cvd"] < prev["cvd"])
                if price_ok and cvd_ok:
                    tol = 0.0
                    if cfg["lineCrossTolerance"] > 0:
                        avg_rng = sum(candles[k]["high"] - candles[k]["low"] for k in range(prev["barIndex"], curr["barIndex"] + 1)) / (
                            curr["barIndex"] - prev["barIndex"] + 1
                        )
                        tol = cfg["lineCrossTolerance"] * avg_rng
                    if not _line_crossed(prev["barIndex"], prev["price"], curr["barIndex"], curr["price"], ser, bears, tol):
                        local.append({"prev": prev, "curr": curr})
                best_prev_p = prev["price"]
            if use_gap and local:
                far = local[-1]
                atr_v = atr[curr.get("lockBarIndex", n - 1)]
                if atr_v > 0 and abs(curr["price"] - far["prev"]["price"]) / atr_v >= cfg["minGapAtr"]:
                    raw_pairs.append(far)
                elif cfg["keepAdjacent"] and local[0]["prev"]["barIndex"] == pivs[idx - 1]["barIndex"]:
                    raw_pairs.append(local[0])
            else:
                raw_pairs.extend(local)

        raw_pairs.sort(key=lambda x: (-x["curr"]["barIndex"], x["prev"]["barIndex"]))
        seen = set()
        out = []
        for pair in raw_pairs:
            k = (pair["prev"]["barIndex"], pair["curr"]["barIndex"])
            if k in seen:
                continue
            seen.add(k)
            res = _resolve_divergence(pair, candles, bears, cfg)
            if res is None:
                continue
            p_prev = pair["prev"]["price"]
            out.append(
                {
                    "kind": "bearish" if bears else "bullish",
                    "status": res["status"],
                    "prev": pair["prev"],
                    "curr": pair["curr"],
                    "referenceLevel": res.get("referenceLevel"),
                    "cvdDelta": pair["curr"]["cvd"] - pair["prev"]["cvd"],
                    "priceDeltaPct": 0.0 if p_prev == 0 else ((pair["curr"]["price"] - p_prev) / p_prev) * 100.0,
                    "barSpan": pair["curr"]["barIndex"] - pair["prev"]["barIndex"],
                    "strength": 0.0,
                    "mfePct": res["mfePct"],
                    "maePct": res["maePct"],
                    "resolvedTimeMs": int(candles[res["resolvedIndex"]]["time"]) if res["resolvedIndex"] >= 0 else None,
                    "barsToResolve": (res["resolvedIndex"] - pair["curr"]["barIndex"]) if res["resolvedIndex"] >= 0 else None,
                    "sfp": res["sfp"],
                }
            )
        return out

    divs = pair_side(True, hi_pivs, highs_s) + pair_side(False, lo_pivs, lows_s)
    if cfg["removeInnerDivs"]:
        divs = _remove_inner_divs(divs, cfg["windowSize"])
    max_cvd = max((abs(d["cvdDelta"]) for d in divs), default=0.0)
    max_px = max((abs(d["priceDeltaPct"]) for d in divs), default=0.0)
    for d in divs:
        sc = abs(d["cvdDelta"]) / max_cvd if max_cvd > 0 else 0.0
        sp = abs(d["priceDeltaPct"]) / max_px if max_px > 0 else 0.0
        d["strength"] = round(0.6 * sc + 0.4 * sp, 6)
    return {"divergences": divs, "highPivots": hi_pivs, "lowPivots": lo_pivs}


MINDPILLAR_DIV_PRESETS: Dict[str, Dict[str, Any]] = {
    # Exact UI defaults from MindPillar's `Yc` (fractal) and `Jc` (zigzag)
    "fractal": {
        "pivot_mode": "fractal",
        "window_size": 2,
        "engulf_len": 50,
        "status_filter": "confirmed",  # hides rejected; keeps pending + confirmed
        "show_pending": True,
        "show_confirmed": True,
        "min_lines": 1,
        "min_size_atr": 0.0,
        "max_span_bars": 20,
        "min_rr": 0.0,
        "one_line_per_anchor": False,
        "hide_played_out": False,
    },
    "zigzag": {
        "pivot_mode": "zigzag",
        "zigzag_atr_mult": 2.0,  # 3.0 on 3m timeframe per MindPillar `aee`
        "zigzag_atr_period": 14,
        "engulf_len": 50,
        "pair_rule": "gap-else-prev",
        "min_gap_atr": 16.0,
        "keep_adjacent": True,
        "status_filter": "confirmed",
        "show_pending": True,
        "show_confirmed": True,
        "min_lines": 1,
        "min_size_atr": 3.0,
        "max_span_bars": 0,
        "min_rr": 1.0,
        "one_line_per_anchor": True,
        "hide_played_out": True,
    },
    "raw": {
        "pivot_mode": "fractal",
        "window_size": 2,
        "engulf_len": 50,
        "status_filter": "all",
        "show_pending": True,
        "show_confirmed": True,
        "min_lines": 1,
        "min_size_atr": 0.0,
        "max_span_bars": 0,
        "min_rr": 0.0,
        "one_line_per_anchor": False,
        "hide_played_out": False,
    },
}


def _sign(x: float) -> int:
    return 1 if x > 0 else (-1 if x < 0 else 0)


def _clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else (1.0 if x > 1.0 else x)


def _atr14_at(candles: Sequence[Dict[str, Any]], bar_index: int, period: int = 14) -> float:
    """Exact port of MindPillar `qC`: 14-bar SMA of True Range ending at `bar_index`."""
    end = min(bar_index, len(candles) - 1)
    if end < 1:
        return 0.0
    start = max(1, end - period + 1)
    total = 0.0
    cnt = 0
    for i in range(start, end + 1):
        c = candles[i]
        pc = float(candles[i - 1]["close"])
        tr = max(float(c["high"]) - float(c["low"]), abs(float(c["high"]) - pc), abs(float(c["low"]) - pc))
        total += tr
        cnt += 1
    return (total / cnt) if cnt > 0 else 0.0


def _target_price(candles: Sequence[Dict[str, Any]], div: Dict[str, Any]) -> float:
    """Exact port of MindPillar `YC`: extreme opposite wick strictly between prev and curr."""
    bull = div["kind"] == "bullish"
    ext = -math.inf if bull else math.inf
    for i in range(div["prev"]["barIndex"] + 1, div["curr"]["barIndex"]):
        v = float(candles[i]["high"]) if bull else float(candles[i]["low"])
        if (bull and v > ext) or (not bull and v < ext):
            ext = v
    return ext if math.isfinite(ext) else float(div["curr"]["price"])


def _trade_metrics(candles: Sequence[Dict[str, Any]], div: Dict[str, Any], target: float, lock_idx: int) -> Dict[str, Any]:
    """Exact port of MindPillar `JC`: zero-lookahead entry at lock bar close, stop at curr pivot."""
    empty = {"entryPrice": None, "rewardPct": None, "riskPct": None, "rr": None}
    if lock_idx < 0 or not candles:
        return empty
    entry = float(candles[min(lock_idx, len(candles) - 1)]["close"])
    if entry <= 0:
        return empty
    bull = div["kind"] == "bullish"
    reward = (target - entry) if bull else (entry - target)
    risk = (entry - float(div["curr"]["price"])) if bull else (float(div["curr"]["price"]) - entry)
    return {
        "entryPrice": entry,
        "rewardPct": round((reward / entry) * 100.0, 6),
        "riskPct": round((risk / entry) * 100.0, 6),
        "rr": round(reward / risk, 6) if (risk > 0 and reward > 0) else None,
    }


def _project_venues_by_bar(
    candle_times: Sequence[int],
    prev_piv: Dict[str, Any],
    curr_piv: Dict[str, Any],
    cvd_maps: Dict[str, Dict[int, float]],
    venue_ids: Sequence[str],
    sample_radius: int = 0,
) -> Dict[str, Any]:
    """Exact port of MindPillar `hk` (`Pe`): multi-venue projection with range-normalized strength."""
    a_piv, b_piv = (prev_piv, curr_piv) if prev_piv["barIndex"] <= curr_piv["barIndex"] else (curr_piv, prev_piv)
    price_slope = _sign(float(b_piv["price"]) - float(a_piv["price"]))
    price_up = price_slope > 0
    n = len(candle_times)
    lines = []
    div_count = 0
    strength_sum = 0.0

    for vid in venue_ids:
        cmap = cvd_maps.get(vid)

        def sample(idx: int) -> Optional[Tuple[float, int]]:
            if not cmap:
                return None
            best_v: Optional[float] = None
            best_t = 0
            lo = max(0, idx - sample_radius)
            hi = min(n - 1, idx + sample_radius)
            for j in range(lo, hi + 1):
                t = candle_times[j]
                v = cmap.get(t)
                if v is not None and (best_v is None or (v < best_v if price_up else v > best_v)):
                    best_v, best_t = v, t
            return None if best_v is None else (best_v, best_t)

        s1 = sample(a_piv["barIndex"])
        s2 = sample(b_piv["barIndex"])
        v1 = s1[0] if s1 else None
        v2 = s2[0] if s2 else None
        is_div = False
        strength = 0.0
        if v1 is not None and v2 is not None:
            cvd_slope = _sign(v2 - v1)
            is_div = price_slope != 0 and cvd_slope != 0 and price_slope != cvd_slope
            if is_div and cmap:
                mn, mx = math.inf, -math.inf
                for j in range(a_piv["barIndex"], b_piv["barIndex"] + 1):
                    val = cmap.get(candle_times[j])
                    if val is not None:
                        if val < mn:
                            mn = val
                        if val > mx:
                            mx = val
                span = mx - mn
                strength = _clamp01(abs(v2 - v1) / span) if (math.isfinite(span) and span > 0) else 0.0
                div_count += 1
                strength_sum += strength

        lines.append(
            {
                "id": vid,
                "cvd1": v1,
                "cvd2": v2,
                "tMs1": s1[1] if s1 else None,
                "tMs2": s2[1] if s2 else None,
                "delta": None if (v1 is None or v2 is None) else (v2 - v1),
                "diverging": is_div,
                "strength": round(strength, 6),
            }
        )

    return {
        "a": {"tMs": a_piv["timeMs"], "price": a_piv["price"]},
        "b": {"tMs": b_piv["timeMs"], "price": b_piv["price"]},
        "priceSlope": price_slope,
        "lines": lines,
        "divergingCount": div_count,
        "total": len(lines),
        "strengthAvg": round(strength_sum / div_count, 6) if div_count > 0 else 0.0,
    }


def project_divergence(
    cvd_by_venue: Dict[str, List[Dict[str, Any]]],
    t1_ms: int,
    price1: float,
    t2_ms: int,
    price2: float,
    window_ms: int = 0,
) -> Dict[str, Any]:
    """Exact port of MindPillar's manual Measure-Divergence tool (`_k` / `pk` / `mk`).

    Projects any two user-chosen price/time points onto all venue CVD lines and reports
    per-venue agreement, `divergingCount`/`total`, and range-normalized `strengthAvg`.
    Detects BOTH absorption (LH/HH, HL/LL) and classic (HH/LH, LL/HL) divergences.
    """
    a, b = ({"tMs": int(t1_ms), "price": float(price1)}, {"tMs": int(t2_ms), "price": float(price2)})
    if a["tMs"] > b["tMs"]:
        a, b = b, a
    price_slope = _sign(b["price"] - a["price"])
    price_up = price_slope > 0

    def sample_series(ser: Sequence[Dict[str, Any]], target_ms: int) -> Tuple[Optional[float], int]:
        if not ser:
            return None, -1
        # Floor binary search (`lk`)
        lo, hi, floor_i = 0, len(ser) - 1, -1
        while lo <= hi:
            mid = (lo + hi) >> 1
            if int(ser[mid]["time"]) <= target_ms:
                floor_i = mid
                lo = mid + 1
            else:
                hi = mid - 1
        if window_ms <= 0:
            return (float(ser[floor_i]["value"]), floor_i) if floor_i >= 0 else (None, -1)
        # Window scan [target_ms - window_ms .. target_ms + window_ms]
        idxs = [i for i, pt in enumerate(ser) if target_ms - window_ms <= int(pt["time"]) <= target_ms + window_ms]
        if not idxs:
            return (float(ser[floor_i]["value"]), floor_i) if floor_i >= 0 else (None, -1)
        best_i = idxs[0]
        best_v = float(ser[best_i]["value"])
        for i in idxs[1:]:
            v = float(ser[i]["value"])
            if (price_up and v < best_v) or (not price_up and v > best_v):
                best_v, best_i = v, i
        return best_v, best_i

    lines = []
    div_count = 0
    strength_sum = 0.0
    for vid, ser in cvd_by_venue.items():
        v1, i1 = sample_series(ser, a["tMs"])
        v2, i2 = sample_series(ser, b["tMs"])
        is_div = False
        strength = 0.0
        if v1 is not None and v2 is not None:
            cvd_slope = _sign(v2 - v1)
            is_div = price_slope != 0 and cvd_slope != 0 and price_slope != cvd_slope
            if is_div and i1 >= 0 and i2 >= 0:
                lo_i, hi_i = min(i1, i2), max(i1, i2)
                vals = [float(ser[j]["value"]) for j in range(lo_i, hi_i + 1)]
                span = max(vals) - min(vals)
                strength = _clamp01(abs(v2 - v1) / span) if span > 0 else 0.0
                div_count += 1
                strength_sum += strength
        lines.append(
            {
                "id": vid,
                "cvd1": v1,
                "cvd2": v2,
                "tMs1": int(ser[i1]["time"]) if (v1 is not None and i1 >= 0) else None,
                "tMs2": int(ser[i2]["time"]) if (v2 is not None and i2 >= 0) else None,
                "delta": None if (v1 is None or v2 is None) else (v2 - v1),
                "diverging": is_div,
                "strength": round(strength, 6),
            }
        )

    return {
        "a": a,
        "b": b,
        "priceSlope": price_slope,
        "lines": lines,
        "divergingCount": div_count,
        "total": len(lines),
        "strengthAvg": round(strength_sum / div_count, 6) if div_count > 0 else 0.0,
    }


def detect_divergences(
    candles: Sequence[Dict[str, Any]],
    cvd_by_venue: Dict[str, List[Dict[str, Any]]],
    *,
    preset: Optional[str] = None,
    tf_ms: Optional[int] = None,
    pivot_mode: Optional[str] = None,
    window_size: Optional[int] = None,
    engulf_len: Optional[int] = None,
    zigzag_atr_mult: Optional[float] = None,
    zigzag_atr_period: int = 14,
    pair_rule: Optional[str] = None,
    min_gap_atr: Optional[float] = None,
    keep_adjacent: Optional[bool] = None,
    remove_inner_divs: bool = True,
    status_filter: Optional[str] = None,
    show_pending: Optional[bool] = None,
    show_confirmed: Optional[bool] = None,
    min_lines: Optional[int] = None,
    min_size_atr: Optional[float] = None,
    max_span_bars: Optional[int] = None,
    min_rr: Optional[float] = None,
    one_line_per_anchor: Optional[bool] = None,
    hide_played_out: Optional[bool] = None,
) -> Dict[str, Any]:
    """Exact port of MindPillar's full CVD divergence pipeline (`Iee` + `XC` + `Xc`/`Zc`/`Qc`/`tl`).

    Presets:
      - `preset="fractal"` -> MindPillar default Fractal chart preset (`Yc`: maxSpanBars=20, hides rejected)
      - `preset="zigzag"`  -> MindPillar default Swing/ATR chart preset (`Jc`: minSizeAtr=3, minRr=1, oneLinePerAnchor, hidePlayedOut)
      - `preset="raw"` (or `None`) -> returns all detected divergences with full MindPillar enrichment fields
    """
    base = MINDPILLAR_DIV_PRESETS.get(preset or "raw", MINDPILLAR_DIV_PRESETS["raw"])
    p_mode = pivot_mode if pivot_mode is not None else base.get("pivot_mode", "fractal")
    w_size = window_size if window_size is not None else base.get("window_size", 2)
    e_len = engulf_len if engulf_len is not None else base.get("engulf_len", 50)
    # MindPillar `aee`: 3m timeframe defaults zigzag reversal to 3*ATR, all others 2*ATR
    default_zz_mult = 3.0 if tf_ms == 180_000 else base.get("zigzag_atr_mult", 2.0)
    zz_mult = zigzag_atr_mult if zigzag_atr_mult is not None else default_zz_mult
    p_rule = pair_rule if pair_rule is not None else ("gap-else-prev" if p_mode == "zigzag" else "depth")
    m_gap = min_gap_atr if min_gap_atr is not None else base.get("min_gap_atr", 16.0)
    k_adj = keep_adjacent if keep_adjacent is not None else base.get("keep_adjacent", True)

    f_status = status_filter if status_filter is not None else base.get("status_filter", "all")
    f_show_pending = show_pending if show_pending is not None else base.get("show_pending", True)
    f_show_confirmed = show_confirmed if show_confirmed is not None else base.get("show_confirmed", True)
    f_min_lines = min_lines if min_lines is not None else base.get("min_lines", 1)
    f_min_size_atr = min_size_atr if min_size_atr is not None else base.get("min_size_atr", 0.0)
    f_max_span = max_span_bars if max_span_bars is not None else base.get("max_span_bars", 0)
    f_min_rr = min_rr if min_rr is not None else base.get("min_rr", 0.0)
    f_one_per_anchor = one_line_per_anchor if one_line_per_anchor is not None else base.get("one_line_per_anchor", False)
    f_hide_played_out = hide_played_out if hide_played_out is not None else base.get("hide_played_out", False)

    cfg = {
        "windowSize": w_size,
        "minDistance": 0,
        "engulfLen": e_len,
        "lastPivotCount": 2000,
        "removeInnerDivs": False,
        "scanLen": 1000,
        "cvdSampling": "pivot",
        "sfpReclaimLen": 50,
        "lineCrossTolerance": 0.5,
        "pivotMode": p_mode,
        "zigzagAtrMult": zz_mult,
        "zigzagAtrPeriod": zigzag_atr_period,
        "pairRule": p_rule,
        "minGapAtr": m_gap,
        "keepAdjacent": k_adj,
    }
    venue_ids = list(cvd_by_venue.keys())
    if not candles or not venue_ids:
        return {"divergences": [], "highPivots": [], "lowPivots": []}

    merged: Dict[str, Dict[str, Any]] = {}
    hi_pivs: List[Dict[str, Any]] = []
    lo_pivs: List[Dict[str, Any]] = []

    candle_times = [int(c["time"]) for c in candles]
    cvd_lookup: Dict[str, Dict[int, float]] = {
        vid: {int(pt["time"]): float(pt["value"]) for pt in ser}
        for vid, ser in cvd_by_venue.items()
    }

    for i, vid in enumerate(venue_ids):
        res = _detect_single_cvd_divergences(candles, cvd_by_venue[vid], cfg)
        if i == 0:
            hi_pivs, lo_pivs = res["highPivots"], res["lowPivots"]
        for d in res["divergences"]:
            key = f"{d['kind']}|{d['prev']['timeMs']}|{d['curr']['timeMs']}"
            ex = merged.get(key)
            if ex is None or abs(d["cvdDelta"]) > abs(ex["cvdDelta"]):
                merged[key] = d

    divs = list(merged.values())
    if remove_inner_divs:
        divs = _remove_inner_divs(divs, w_size)

    # Global strength normalization (`Nc`)
    max_cvd = max((abs(d["cvdDelta"]) for d in divs), default=0.0)
    max_px = max((abs(d["priceDeltaPct"]) for d in divs), default=0.0)

    # Full MindPillar enrichment (`XC`: targetPrice, invalidationPrice, structurePct, structureAtr, entryPrice, rewardPct, riskPct, rr, lockIndex, projection)
    sample_radius = 0 if cfg["cvdSampling"] == "pivot" else w_size
    enriched_all: List[Dict[str, Any]] = []
    for d in divs:
        sc = abs(d["cvdDelta"]) / max_cvd if max_cvd > 0 else 0.0
        sp = abs(d["priceDeltaPct"]) / max_px if max_px > 0 else 0.0
        d["strength"] = round(0.6 * sc + 0.4 * sp, 6)

        target = _target_price(candles, d)
        struct_dist = abs(target - float(d["curr"]["price"]))
        atr_here = _atr14_at(candles, d["curr"]["barIndex"], 14)
        base_lock = d["curr"].get("lockBarIndex", d["curr"]["barIndex"] + w_size)
        lock_idx = max(len(candles), base_lock) if d["curr"].get("pending") else base_lock
        tm = _trade_metrics(candles, d, target, lock_idx)
        proj = _project_venues_by_bar(candle_times, d["prev"], d["curr"], cvd_lookup, venue_ids, sample_radius)

        # Lifecycle state (`iw`): confirmed | rejected | forming | armed
        if d["status"] == "confirmed":
            state = "confirmed"
        elif d["status"] == "rejected":
            state = "rejected"
        elif d["curr"].get("pending") or d["prev"].get("pending"):
            state = "forming"
        else:
            state = "armed"

        d["key"] = f"{d['kind']}|{d['prev']['timeMs']}|{d['curr']['timeMs']}"
        d["state"] = state
        d["projection"] = proj
        d["targetPrice"] = target
        d["invalidationPrice"] = float(d["prev"]["price"])
        d["structurePct"] = round(0.0 if d["curr"]["price"] == 0 else (struct_dist / d["curr"]["price"]) * 100.0, 6)
        d["structureAtr"] = round(struct_dist / atr_here, 4) if atr_here > 0 else 0.0
        d["entryPrice"] = tm["entryPrice"]
        d["rewardPct"] = tm["rewardPct"]
        d["riskPct"] = tm["riskPct"]
        d["rr"] = tm["rr"]
        d["lockIndex"] = lock_idx
        d["armedTimeMs"] = candle_times[base_lock] if base_lock < len(candle_times) else None
        d["anchorConfirms"] = []
        enriched_all.append(d)

    # Build `anchorConfirms` index (`XC`) for `hidePlayedOut` (`Qc`)
    confirms_by_anchor: Dict[str, List[Dict[str, Any]]] = {}
    for d in enriched_all:
        if d["status"] != "confirmed" or d["barsToResolve"] is None or d["prev"].get("pending"):
            continue
        akey = f"{d['kind']}|{d['prev']['timeMs']}"
        confirms_by_anchor.setdefault(akey, []).append(
            {
                "key": d["key"],
                "resolvedIndex": d["curr"]["barIndex"] + d["barsToResolve"],
                "projection": d["projection"],
                "structureAtr": d["structureAtr"],
                "barSpan": d["barSpan"],
                "rr": d["rr"],
            }
        )
    for arr in confirms_by_anchor.values():
        arr.sort(key=lambda x: x["resolvedIndex"])
    for d in enriched_all:
        if not d["prev"].get("pending"):
            akey = f"{d['kind']}|{d['prev']['timeMs']}"
            if akey in confirms_by_anchor:
                d["anchorConfirms"] = confirms_by_anchor[akey]

    def passes_quality(item: Dict[str, Any]) -> bool:
        """Exact port of MindPillar `Zc`."""
        proj = item.get("projection")
        if proj:
            req_lines = min(f_min_lines, proj["total"])
            if req_lines > 1 and proj["divergingCount"] < req_lines:
                return False
        if f_min_size_atr > 0 and item["structureAtr"] > 0 and item["structureAtr"] < f_min_size_atr:
            return False
        if f_max_span > 0 and item["barSpan"] > f_max_span:
            return False
        if f_min_rr > 0 and item["rr"] is not None and item["rr"] < f_min_rr:
            return False
        return True

    def is_played_out(item: Dict[str, Any]) -> bool:
        """Exact port of MindPillar `Qc`."""
        if item["prev"].get("pending") or not item["anchorConfirms"]:
            return False
        for c in item["anchorConfirms"]:
            if c["key"] == item["key"] or not passes_quality(c):
                continue
            return item["lockIndex"] >= c["resolvedIndex"]
        return False

    # Apply MindPillar filter predicate `Xc`
    filtered: List[Dict[str, Any]] = []
    for d in enriched_all:
        if d["status"] == "rejected" and f_status != "all":
            continue
        if d["status"] == "pending" and not f_show_pending:
            continue
        if d["status"] == "confirmed" and not f_show_confirmed:
            continue
        if f_status in ("pending", "rejected") and d["status"] != f_status:
            continue
        if not passes_quality(d):
            continue
        if f_hide_played_out and is_played_out(d):
            continue
        filtered.append(d)

    # `oneLinePerAnchor` (`tl`): keep only latest curr bar per anchor (prefer pending on tie)
    if f_one_per_anchor:
        by_anchor: Dict[str, Dict[str, Any]] = {}
        for d in filtered:
            akey = f"{d['kind']}|{d['prev']['timeMs']}"
            ex = by_anchor.get(akey)
            if (
                ex is None
                or d["curr"]["barIndex"] > ex["curr"]["barIndex"]
                or (d["curr"]["barIndex"] == ex["curr"]["barIndex"] and d["curr"].get("pending") and not ex["curr"].get("pending"))
            ):
                by_anchor[akey] = d
        keep_keys = {v["key"] for v in by_anchor.values()}
        filtered = [d for d in filtered if d["key"] in keep_keys]

    # Strip internal anchorConfirms list length bloat to summary count
    for d in filtered:
        d["anchorConfirmCount"] = len(d.pop("anchorConfirms", []))

    filtered.sort(key=lambda x: x["curr"]["timeMs"], reverse=True)
    return {"divergences": filtered, "highPivots": hi_pivs, "lowPivots": lo_pivs}


# ---------------------------------------------------------------------------
# Liquidation Tensor Summary Helper
# ---------------------------------------------------------------------------

def summarize_liq_tensor(
    raw: Dict[str, Any],
    exchange: str = "aggregated",
    top_levels: int = 25,
) -> Dict[str, Any]:
    """Summarize a 4-10 MB `liq-tensor` JSON artifact into compact numeric trading metrics."""
    spot = float(raw.get("spot_price") or 0.0)
    cells = raw.get("cells") or []
    if exchange != "aggregated":
        cells = [c for c in cells if c.get("exchange") == exchange]

    # Deduplicate/aggregate liquidation map cells by price bin + side + leverage
    by_price: Dict[float, Dict[str, float]] = {}
    by_leverage: Dict[int, Dict[str, float]] = {}
    notional_above = 0.0
    notional_below = 0.0
    long_total = 0.0
    short_total = 0.0

    for c in cells:
        p = float(c.get("price") or 0.0)
        usd = float(c.get("notional_usd") or 0.0)
        side = str(c.get("side") or "")
        lev = int(c.get("leverage") or 0)
        if usd <= 0 or p <= 0:
            continue
        if p >= spot:
            notional_above += usd
        else:
            notional_below += usd
        if side == "long":
            long_total += usd
        elif side == "short":
            short_total += usd

        bucket = round(p, -1) if spot >= 1000 else round(p, 1)
        bp = by_price.setdefault(bucket, {"price": bucket, "total_usd": 0.0, "long_usd": 0.0, "short_usd": 0.0})
        bp["total_usd"] += usd
        if side == "long":
            bp["long_usd"] += usd
        elif side == "short":
            bp["short_usd"] += usd

        bl = by_leverage.setdefault(lev, {"leverage": lev, "total_usd": 0.0, "long_usd": 0.0, "short_usd": 0.0})
        bl["total_usd"] += usd
        if side == "long":
            bl["long_usd"] += usd
        elif side == "short":
            bl["short_usd"] += usd

    clusters_above = sorted(
        [v for v in by_price.values() if v["price"] >= spot], key=lambda x: x["total_usd"], reverse=True
    )[:top_levels]
    clusters_below = sorted(
        [v for v in by_price.values() if v["price"] < spot], key=lambda x: x["total_usd"], reverse=True
    )[:top_levels]

    # Latest heatmap cohort snapshot
    cohorts = raw.get("heatmap_cohorts") or []
    latest_ts = max((int(c.get("ts") or 0) for c in cohorts), default=0)
    latest_cohorts = [c for c in cohorts if int(c.get("ts") or 0) == latest_ts]

    return {
        "symbol": raw.get("symbol"),
        "interval": raw.get("interval"),
        "exchange": exchange,
        "updated_at": raw.get("updated_at"),
        "generated_at": raw.get("generated_at"),
        "spot_price": spot,
        "vwap_4h": raw.get("vwap_4h"),
        "coverage_pct": raw.get("coverage_pct"),
        "included_exchanges": raw.get("included_exchanges") or raw.get("exchanges"),
        "leverage_buckets": raw.get("leverage_buckets"),
        "price_bin_range": [raw["price_bins"][0], raw["price_bins"][-1]] if raw.get("price_bins") else None,
        "time_bin_range": [raw["time_bins"][0], raw["time_bins"][-1]] if raw.get("time_bins") else None,
        "cell_count": len(cells),
        "heatmap_cell_count": len(raw.get("heatmap_cells") or []),
        "notional_above_usd": round(notional_above, 2),
        "notional_below_usd": round(notional_below, 2),
        "long_liq_usd": round(long_total, 2),
        "short_liq_usd": round(short_total, 2),
        "above_to_below_ratio": round(notional_above / notional_below, 4) if notional_below > 0 else None,
        "by_leverage": sorted(by_leverage.values(), key=lambda x: x["leverage"]),
        "top_clusters_above": clusters_above,
        "top_clusters_below": clusters_below,
        "latest_cohorts": latest_cohorts,
    }


# ---------------------------------------------------------------------------
# Synchronous Stdlib REST Client
# ---------------------------------------------------------------------------

class MindPillarClient:
    """Synchronous stdlib-only client for all MindPillar / Bitcoinsensus public numeric APIs."""

    def __init__(self, timeout: float = 20.0, max_workers: int = 6):
        self.timeout = timeout
        self.max_workers = max_workers
        self._etag_cache: Dict[str, Tuple[Optional[str], Dict[str, Any]]] = {}

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        h = {
            "User-Agent": UA,
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "Origin": ORIGIN,
            "Referer": REFERER,
        }
        if extra:
            h.update(extra)
        return h

    def _get_json(self, url: str, extra_headers: Optional[Dict[str, str]] = None) -> Tuple[int, Dict[str, str], Any]:
        if not url.startswith((CANDLES_API + "/", CHARTS_CDN + "/")):
            raise ValueError(f"Disallowed upstream URL: {url}")
        req = urllib.request.Request(url, headers=self._headers(extra_headers), method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                if raw[:2] == b"\x1f\x8b" or str(resp.headers.get("Content-Encoding", "")).lower() == "gzip":
                    raw = gzip.decompress(raw)
                return resp.status, dict(resp.headers), json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 304:
                return 304, dict(e.headers), None
            body = ""
            try:
                raw = e.read()
                if raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                body = raw.decode("utf-8", errors="replace")
            except Exception:
                pass
            raise MindPillarHTTPError(url, e.code, body) from e
        except urllib.error.URLError as e:
            raise MindPillarError(f"Network error on {url}: {e}") from e

    # ---- Metadata & Feed Health ----

    def config(self) -> Dict[str, Any]:
        """GET /config — supported timeframes, bar limits, tier limits."""
        _, _, data = self._get_json(f"{CANDLES_API}/config")
        return data

    def stats(self) -> Dict[str, Any]:
        """GET /stats — live feed latency (ms), 24h trade counts & USD notional."""
        _, _, data = self._get_json(f"{CANDLES_API}/stats")
        return data

    def ws_ticket(self) -> str:
        """GET /ws/ticket — fetch a fresh anonymous WebSocket ticket UUID."""
        _, _, data = self._get_json(f"{CANDLES_API}/ws/ticket")
        return str(data["ticket"])

    # ---- Historical Candles + Buy/Sell Volume ----

    @staticmethod
    def _parse_historical(payload: Dict[str, Any], market: MarketSpec) -> List[Dict[str, Any]]:
        cols = payload.get("columns") or {}
        ti = cols.get("time", 0)
        oi = cols.get("open", 9)
        hi = cols.get("high", 4)
        li = cols.get("low", 6)
        ci = cols.get("close", 2)
        bi = cols.get("vbuy", 10)
        si = cols.get("vsell", 11)
        rows = payload.get("results") or []
        by_time: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            t = int(row[ti])
            if t <= 0:
                continue
            bar = {
                "time": t,
                "open": float(row[oi] or 0.0),
                "high": float(row[hi] or 0.0),
                "low": float(row[li] or 0.0),
                "close": float(row[ci] or 0.0),
                "vbuy": float(row[bi] or 0.0),
                "vsell": float(row[si] or 0.0),
            }
            bar.update(bar_buy_sell_delta(bar, market))
            by_time[t] = bar
        return [by_time[t] for t in sorted(by_time)]

    def candles(
        self,
        market: str | MarketSpec = "binance-btc",
        tf: str | int = "15m",
        bars: int = 1000,
        from_ms: Optional[int] = None,
        to_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """GET /historical/{from}/{to}/{tf_ms}/{market} with automatic chunking above 2000 bars."""
        spec = resolve_market(market)
        _, tf_ms = resolve_tf(tf)
        now_ms = int(time.time() * 1000)
        aligned_now = align_bar_ms(tf_ms, now_ms)
        end = int(to_ms) if to_ms is not None else (aligned_now + tf_ms)
        start = int(from_ms) if from_ms is not None else max(0, end - int(bars) * tf_ms)

        # Server returns cleanly in chunks up to ~2000 bars per request
        chunk_bars = 2000
        chunk_span = chunk_bars * tf_ms
        encoded = urllib.parse.quote(spec.key, safe="")
        merged: Dict[int, Dict[str, Any]] = {}

        cur = start
        while cur < end:
            nxt = min(cur + chunk_span, end)
            url = f"{CANDLES_API}/historical/{cur}/{nxt}/{tf_ms}/{encoded}"
            _, _, payload = self._get_json(url)
            for bar in self._parse_historical(payload, spec):
                merged[bar["time"]] = bar
            if nxt >= end:
                break
            cur = nxt

        ordered = [merged[t] for t in sorted(merged)]
        if from_ms is None and to_ms is None and len(ordered) > bars:
            ordered = ordered[-bars:]
        return ordered

    # ---- CVD & Aggregate CVD Snapshot ----

    def cvd_snapshot(
        self,
        asset: str = "btc",
        tf: str | int = "15m",
        bars: int = 1000,
        mode: str = "all",
        unit: str = "usd",
        rolling_hours: float = 24.0,
        include_spot: bool = False,
        include_series: bool = True,
        include_divergences: bool = False,
        divergence_mode: str = "fractal",
    ) -> Dict[str, Any]:
        """Fetch all venues concurrently and compute exact MindPillar CVD, Aggregate CVD, and optional divergences."""
        tf_label, tf_ms = resolve_tf(tf)
        markets = asset_markets(asset, include_spot=include_spot)
        perp_list = [m for m in markets if m.kind == "perp"]
        spot_list = [m for m in markets if m.kind == "spot"]
        rolling_bars = max(1, int(round((rolling_hours * 3_600_000) / tf_ms)))

        candles_by_id: Dict[str, List[Dict[str, Any]]] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(markets), self.max_workers)) as pool:
            futs = {pool.submit(self.candles, m, tf_ms, bars): m for m in markets}
            for fut in concurrent.futures.as_completed(futs):
                m = futs[fut]
                candles_by_id[m.id] = fut.result()

        # Primary price series (Binance USDT Perp, matching MindPillar default)
        primary_m = perp_list[0]
        primary_candles = candles_by_id.get(primary_m.id, [])
        last_bar = primary_candles[-1] if primary_candles else None

        cvd_by_id: Dict[str, List[Dict[str, Any]]] = {}
        venues_summary: Dict[str, Dict[str, Any]] = {}

        for m in markets:
            arr = candles_by_id.get(m.id, [])
            ser = compute_market_cvd(arr, m, unit=unit, mode=mode, rolling_bars=rolling_bars)
            ser_all = ser if mode == "all" else compute_market_cvd(arr, m, unit=unit, mode="all", rolling_bars=rolling_bars)
            ser_daily = ser if mode == "daily" else compute_market_cvd(arr, m, unit=unit, mode="daily", rolling_bars=rolling_bars)
            ser_24h = ser if mode == "rolling" else compute_market_cvd(arr, m, unit=unit, mode="rolling", rolling_bars=rolling_bars)
            cvd_by_id[m.id] = ser
            lb = arr[-1] if arr else None
            venues_summary[m.id] = {
                "id": m.id,
                "key": m.key,
                "label": m.label,
                "ticker": m.ticker,
                "venue": m.venue,
                "kind": m.kind,
                "unit": m.unit,
                "bars": len(arr),
                "last_time": lb["time"] if lb else None,
                "open": lb["open"] if lb else None,
                "high": lb["high"] if lb else None,
                "low": lb["low"] if lb else None,
                "close": lb["close"] if lb else None,
                "buy_usd": lb["buy_usd"] if lb else None,
                "sell_usd": lb["sell_usd"] if lb else None,
                "delta_usd": lb["delta_usd"] if lb else None,
                "volume_usd": lb["volume_usd"] if lb else None,
                "buy_base": lb["buy_base"] if lb else None,
                "sell_base": lb["sell_base"] if lb else None,
                "delta_base": lb["delta_base"] if lb else None,
                "cvd": ser[-1]["value"] if ser else None,
                "cvd_all": ser_all[-1]["value"] if ser_all else None,
                "cvd_daily": ser_daily[-1]["value"] if ser_daily else None,
                "cvd_24h": ser_24h[-1]["value"] if ser_24h else None,
            }

        agg_perp = compute_aggregate_cvd(candles_by_id, perp_list, unit=unit, mode=mode, rolling_bars=rolling_bars)
        agg_perp_all = compute_aggregate_cvd(candles_by_id, perp_list, unit=unit, mode="all", rolling_bars=rolling_bars)
        agg_perp_daily = compute_aggregate_cvd(candles_by_id, perp_list, unit=unit, mode="daily", rolling_bars=rolling_bars)
        agg_perp_24h = compute_aggregate_cvd(candles_by_id, perp_list, unit=unit, mode="rolling", rolling_bars=rolling_bars)

        agg_buy_usd = sum((venues_summary[m.id]["buy_usd"] or 0.0) for m in perp_list)
        agg_sell_usd = sum((venues_summary[m.id]["sell_usd"] or 0.0) for m in perp_list)
        agg_vol_usd = agg_buy_usd + agg_sell_usd

        spot_block = None
        if spot_list:
            agg_spot = compute_aggregate_cvd(candles_by_id, spot_list, unit=unit, mode=mode, rolling_bars=rolling_bars)
            spot_buy_usd = sum((venues_summary[m.id]["buy_usd"] or 0.0) for m in spot_list)
            spot_sell_usd = sum((venues_summary[m.id]["sell_usd"] or 0.0) for m in spot_list)
            spot_vol_usd = spot_buy_usd + spot_sell_usd
            tot_vol = agg_vol_usd + spot_vol_usd
            spot_block = {
                "cvd": agg_spot[-1]["value"] if agg_spot else None,
                "buy_usd": spot_buy_usd,
                "sell_usd": spot_sell_usd,
                "delta_usd": spot_buy_usd - spot_sell_usd,
                "volume_usd": spot_vol_usd,
                "spot_share_pct": round((spot_vol_usd / tot_vol) * 100.0, 4) if tot_vol > 0 else None,
            }

        price_block = None
        if last_bar:
            chg = last_bar["close"] - last_bar["open"]
            chg_pct = (chg / last_bar["open"] * 100.0) if last_bar["open"] else 0.0
            price_block = {
                "market": primary_m.id,
                "ticker": primary_m.ticker,
                "time": last_bar["time"],
                "open": last_bar["open"],
                "high": last_bar["high"],
                "low": last_bar["low"],
                "close": last_bar["close"],
                "change": chg,
                "change_pct": chg_pct,
                "volume_base": last_bar["volume_base"],
                "volume_usd": last_bar["volume_usd"],
            }

        out: Dict[str, Any] = {
            "as_of_ms": int(time.time() * 1000),
            "asset": asset.lower(),
            "timeframe": tf_label,
            "tf_ms": tf_ms,
            "bars": bars,
            "mode": mode,
            "unit": unit,
            "price": price_block,
            "aggregate_perp": {
                "cvd": agg_perp[-1]["value"] if agg_perp else None,
                "cvd_all": agg_perp_all[-1]["value"] if agg_perp_all else None,
                "cvd_daily": agg_perp_daily[-1]["value"] if agg_perp_daily else None,
                "cvd_24h": agg_perp_24h[-1]["value"] if agg_perp_24h else None,
                "buy_usd": agg_buy_usd,
                "sell_usd": agg_sell_usd,
                "delta_usd": agg_buy_usd - agg_sell_usd,
                "volume_usd": agg_vol_usd,
            },
            "venues": venues_summary,
        }
        if spot_block is not None:
            out["aggregate_spot"] = spot_block
        if include_series:
            out["series"] = {
                "candles": candles_by_id,
                "cvd": cvd_by_id,
                "aggregate_perp_cvd": agg_perp,
            }
        if include_divergences and primary_candles:
            perp_cvd_map = {m.id: cvd_by_id[m.id] for m in perp_list if m.id in cvd_by_id}
            out["divergences"] = detect_divergences(
                primary_candles,
                perp_cvd_map,
                preset=divergence_mode if divergence_mode in MINDPILLAR_DIV_PRESETS else "raw",
                tf_ms=tf_ms,
                pivot_mode=divergence_mode if divergence_mode in ("fractal", "zigzag") else "fractal",
            )
        return out

    # ---- Open Interest ----

    def open_interest(
        self,
        market: str | MarketSpec = "binance-btc",
        tf: str | int = "15m",
        bars: int = 200,
        from_ms: Optional[int] = None,
        to_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """GET /oi/{from}/{to}/{tf_ms}/{market} — OHLC Open Interest bars."""
        spec = resolve_market(market)
        _, tf_ms = resolve_tf(tf)
        now_ms = int(time.time() * 1000)
        aligned_now = align_bar_ms(tf_ms, now_ms)
        end = int(to_ms) if to_ms is not None else (aligned_now + tf_ms)
        start = int(from_ms) if from_ms is not None else max(0, end - int(bars) * tf_ms)
        encoded = urllib.parse.quote(spec.key, safe="")
        url = f"{CANDLES_API}/oi/{start}/{end}/{tf_ms}/{encoded}"
        _, _, payload = self._get_json(url)
        return list(payload.get("bars") or [])

    # ---- Footprint / Orderflow Clusters ----

    def footprint(
        self,
        market: str | MarketSpec = "binance-btc",
        tf: str | int = "15m",
        bars: int = 50,
        step: Optional[float] = None,
        from_ms: Optional[int] = None,
        to_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """GET /footprint/{from}/{to}/{tf_ms}/{market}?step={step} — per-price-level bid/ask clusters."""
        spec = resolve_market(market)
        _, tf_ms = resolve_tf(tf)
        tick_step = step if step is not None else DEFAULT_FOOTPRINT_STEP.get(spec.asset, 25.0)
        now_ms = int(time.time() * 1000)
        aligned_now = align_bar_ms(tf_ms, now_ms)
        end = int(to_ms) if to_ms is not None else (aligned_now + tf_ms)
        start = int(from_ms) if from_ms is not None else max(0, end - int(bars) * tf_ms)
        encoded = urllib.parse.quote(spec.key, safe="")
        url = f"{CANDLES_API}/footprint/{start}/{end}/{tf_ms}/{encoded}?step={tick_step:g}"
        _, _, payload = self._get_json(url)
        # Enrich each bar with POC price & buy/sell totals
        for b in payload.get("bars") or []:
            lp, lb, la = b.get("lp") or [], b.get("lb") or [], b.get("la") or []
            if lp and len(lp) == len(lb) == len(la):
                totals = [lb[i] + la[i] for i in range(len(lp))]
                poc_i = max(range(len(totals)), key=lambda i: totals[i])
                b["poc_price"] = lp[poc_i]
                b["poc_volume"] = totals[poc_i]
        return payload

    # ---- Volume Profile Zones (Daily / Weekly / Monthly / Yearly POC, VAH, VAL, EQ) ----

    def zones(
        self,
        market: str | MarketSpec = "binance-btc",
        period: str = "day",
        method: str = "trades",
        chart_tf_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """GET /zones/{market}?period={day|week|month|year} — previous & developing POC/VAH/VAL/OHLC/EQ."""
        if period not in ZONES_PERIODS:
            raise ValueError(f"Unsupported period '{period}'. Supported: {ZONES_PERIODS}")
        spec = resolve_market(market)
        encoded = urllib.parse.quote(spec.key, safe="")
        q = f"period={period}"
        if method == "candles" and chart_tf_ms:
            q += f"&method=candles&chartTf={int(chart_tf_ms)}"
        _, _, data = self._get_json(f"{CANDLES_API}/zones/{encoded}?{q}")
        return data

    # ---- Price Levels (nPOC, tvPOC, dLevels, wLevels, mLevels, Order Blocks) ----

    def levels(
        self,
        market: str | MarketSpec = "binance-btc",
        family: str = "npoc",
        period: str = "day",
        chart_tf: str | int = "15m",
        only_untapped: bool = False,
    ) -> Dict[str, Any]:
        """GET /levels/{market}?family={npoc|tvpoc|dlevels|wlevels|mlevels|ob}."""
        if family not in LEVEL_FAMILIES:
            raise ValueError(f"Unsupported level family '{family}'. Supported: {LEVEL_FAMILIES}")
        if period not in ("day", "week", "month"):
            raise ValueError(f"Unsupported levels period '{period}'. Supported: ('day', 'week', 'month')")
        spec = resolve_market(market)
        encoded = urllib.parse.quote(spec.key, safe="")
        if family == "tvpoc":
            _, tf_ms = resolve_tf(chart_tf)
            q = f"family=tvpoc&chartTf={tf_ms}"
        else:
            q = f"family={family}"
        if period != "day" and family in ("npoc", "tvpoc"):
            q += f"&period={period}"
        _, _, data = self._get_json(f"{CANDLES_API}/levels/{encoded}?{q}")
        if only_untapped and isinstance(data.get("levels"), list):
            data = {
                **data,
                "levels": [lv for lv in data["levels"] if lv.get("state") in ("naked", "live", "untapped")],
            }
        return data

    # ---- Range & Session Volume Profiles ----

    def range_profile(
        self,
        market: str | MarketSpec = "binance-btc",
        from_ms: Optional[int] = None,
        to_ms: Optional[int] = None,
        lookback_hours: float = 24.0,
        step: Optional[float] = None,
        value_area: float = 0.7,
    ) -> Dict[str, Any]:
        """GET /profile/{market}?from=...&to=...&step=...&valueArea=... — arbitrary range volume profile."""
        spec = resolve_market(market)
        tick_step = step if step is not None else DEFAULT_PROFILE_STEP.get(spec.asset, 25.0)
        now_ms = int(time.time() * 1000)
        end = int(to_ms) if to_ms is not None else now_ms
        start = int(from_ms) if from_ms is not None else int(end - lookback_hours * 3_600_000)
        encoded = urllib.parse.quote(spec.key, safe="")
        url = f"{CANDLES_API}/profile/{encoded}?from={start}&to={end}&step={tick_step:g}&valueArea={value_area:g}"
        _, _, data = self._get_json(url)
        return data

    def session_profiles(
        self,
        market: str | MarketSpec = "binance-btc",
        period: str = "day",
        days: int = 3,
        from_ms: Optional[int] = None,
        to_ms: Optional[int] = None,
        step: Optional[float] = None,
    ) -> Dict[str, Any]:
        """GET /profiles/{market}?period={day|week|month}&from=...&to=...&step=... — per-session profiles."""
        if period not in ("day", "week", "month"):
            raise ValueError(f"Unsupported profile period '{period}'. Supported: ('day', 'week', 'month')")
        spec = resolve_market(market)
        tick_step = step if step is not None else DEFAULT_PROFILE_STEP.get(spec.asset, 25.0)
        now_ms = int(time.time() * 1000)
        end = int(to_ms) if to_ms is not None else now_ms
        start = int(from_ms) if from_ms is not None else (end - int(days) * 86_400_000)
        encoded = urllib.parse.quote(spec.key, safe="")
        url = f"{CANDLES_API}/profiles/{encoded}?period={period}&from={start}&to={end}&step={tick_step:g}"
        _, _, data = self._get_json(url)
        return data

    # ---- Liquidation Heatmap & Liquidation Map Tensor ----

    def liq_tensor(
        self,
        symbol: str = "BTCUSDT",
        interval: str = "1d",
        exchange: str = "aggregated",
        summary_only: bool = True,
        top_levels: int = 25,
    ) -> Dict[str, Any]:
        """GET /v1/liq-tensor/{symbol}_{interval}.json (gzip + ETag cached).

        Set `summary_only=False` to include the full raw tensor (cells, heatmap_cells, heatmap_cohorts, ohlc).
        """
        if interval not in LIQ_TENSOR_INTERVALS:
            raise ValueError(f"Unsupported liq-tensor interval '{interval}'. Supported: {LIQ_TENSOR_INTERVALS}")
        sym = symbol.strip().upper()
        if not re.fullmatch(r"[A-Z0-9]{2,20}", sym):
            raise ValueError(f"Invalid symbol '{symbol}': must be 2-20 alphanumeric characters")
        cache_key = f"{sym}_{interval}"
        bucket = int(time.time() // 300)
        url = f"{CHARTS_CDN}/v1/liq-tensor/{sym}_{interval}.json?t={bucket}"
        cached = self._etag_cache.get(cache_key)
        extra = {"If-None-Match": cached[0]} if (cached and cached[0]) else None
        status, hdrs, data = self._get_json(url, extra_headers=extra)
        if status == 304 and cached is not None:
            raw = cached[1]
        else:
            raw = data
            etag = hdrs.get("ETag") or hdrs.get("etag")
            if len(self._etag_cache) >= 64:
                self._etag_cache.pop(next(iter(self._etag_cache)))
            self._etag_cache[cache_key] = (etag, raw)
        if summary_only:
            return summarize_liq_tensor(raw, exchange=exchange, top_levels=top_levels)
        return {"summary": summarize_liq_tensor(raw, exchange=exchange, top_levels=top_levels), "raw": raw}

    # ---- Unified Full Numeric Snapshot of /cvd/btc/ ----

    def full_snapshot(
        self,
        asset: str = "btc",
        tf: str | int = "15m",
        bars: int = 1000,
        mode: str = "all",
        unit: str = "usd",
        include_spot: bool = True,
        include_divergences: bool = True,
        include_oi: bool = True,
        include_footprint: bool = True,
        include_zones: bool = True,
        include_levels: bool = True,
        include_liq_tensor: bool = True,
        liq_interval: str = "1d",
        include_series: bool = False,
    ) -> Dict[str, Any]:
        """Fetch EVERY numeric dataset rendered on https://mindpillar.com/cvd/btc/ in parallel."""
        primary = asset_markets(asset, include_spot=False)[0]
        sym = f"{asset.upper()}USDT"

        result: Dict[str, Any] = {}
        tasks: Dict[str, Callable[[], Any]] = {
            "cvd": lambda: self.cvd_snapshot(
                asset=asset,
                tf=tf,
                bars=bars,
                mode=mode,
                unit=unit,
                include_spot=include_spot,
                include_series=include_series,
                include_divergences=include_divergences,
            ),
            "stats": self.stats,
        }
        if include_oi:
            tasks["open_interest"] = lambda: self.open_interest(primary, tf=tf, bars=50)
        if include_footprint:
            tasks["footprint"] = lambda: self.footprint(primary, tf=tf, bars=10)
        if include_zones:
            tasks["zones_day"] = lambda: self.zones(primary, period="day")
            tasks["zones_week"] = lambda: self.zones(primary, period="week")
        if include_levels:
            tasks["naked_pocs"] = lambda: self.levels(primary, family="npoc", only_untapped=True)
            tasks["order_blocks"] = lambda: self.levels(primary, family="ob", only_untapped=True)
        if include_liq_tensor:
            tasks["liq_tensor"] = lambda: self.liq_tensor(sym, interval=liq_interval, summary_only=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(tasks), 8)) as pool:
            futs = {pool.submit(fn): name for name, fn in tasks.items()}
            for fut in concurrent.futures.as_completed(futs):
                name = futs[fut]
                try:
                    result[name] = fut.result()
                except Exception as exc:
                    result[name] = {"error": str(exc)}

        # Compact top-level summary alongside detailed payloads
        oi_bars = result.get("open_interest")
        if isinstance(oi_bars, list) and oi_bars:
            last_oi = oi_bars[-1]
            px = ((result.get("cvd") or {}).get("price") or {}).get("close") or 0.0
            result["open_interest_latest"] = {
                **last_oi,
                "oi_usd": round(float(last_oi["c"]) * float(px), 2) if px else None,
            }

        fp_obj = result.get("footprint")
        if isinstance(fp_obj, dict) and fp_obj.get("bars"):
            result["footprint_latest"] = fp_obj["bars"][-1]

        return result


# ---------------------------------------------------------------------------
# Real-Time Async WebSocket Live Feed Engine
# ---------------------------------------------------------------------------

class MindPillarLiveFeed:
    """Real-time async streaming engine for https://mindpillar.com/cvd/btc/.

    Bootstraps historical bars via REST, connects to `wss://candles.bitcoinsensus.com/ws`
    (with automatic ticket refresh + exponential backoff reconnect + gap catchup),
    and optionally connects to direct Binance/Bybit forced liquidation streams.

    Usage:
        feed = MindPillarLiveFeed(asset="btc", tf="15m", bars=1000)
        async with feed:
            async for event in feed.stream():
                print(event["type"], feed.snapshot()["aggregate_perp"]["cvd_all"])
    """

    def __init__(
        self,
        asset: str = "btc",
        tf: str | int = "15m",
        bars: int = 1000,
        include_spot: bool = True,
        include_oi: bool = True,
        include_footprint: bool = True,
        include_liquidations: bool = True,
        include_liq_tensor: bool = True,
        liq_interval: str = "1d",
        include_zones: bool = True,
        client: Optional[MindPillarClient] = None,
    ):
        self.asset = asset.lower()
        self.tf_label, self.tf_ms = resolve_tf(tf)
        self.bars = int(bars)
        self.include_spot = include_spot
        self.include_oi = include_oi
        self.include_footprint = include_footprint
        self.include_liquidations = include_liquidations
        self.include_liq_tensor = include_liq_tensor
        self.liq_interval = liq_interval
        self.include_zones = include_zones
        self.client = client or MindPillarClient()

        self.markets = asset_markets(self.asset, include_spot=include_spot)
        self.perp_markets = [m for m in self.markets if m.kind == "perp"]
        self.spot_markets = [m for m in self.markets if m.kind == "spot"]
        self.primary_market = self.perp_markets[0]
        self.rolling_bars_24h = max(1, int(round(86_400_000 / self.tf_ms)))

        # In-memory live state
        self.candles: Dict[str, List[Dict[str, Any]]] = {m.id: [] for m in self.markets}
        self.oi_bars: List[Dict[str, Any]] = []
        self.footprint_latest: Optional[Dict[str, Any]] = None
        self.zones_day: Optional[Dict[str, Any]] = None
        self.liq_tensor_summary: Optional[Dict[str, Any]] = None
        self.recent_liquidations: List[Dict[str, Any]] = []
        self.connected: bool = False
        self.last_update_ms: int = 0
        self.tick_count: int = 0

        self._stop = asyncio.Event()
        self._tasks: List[asyncio.Task[Any]] = []
        self._subscribers: List[asyncio.Queue[Dict[str, Any]]] = []

    async def bootstrap(self) -> None:
        """Load initial historical state via REST in a background thread."""
        loop = asyncio.get_running_loop()

        def _load() -> None:
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                futs = {pool.submit(self.client.candles, m, self.tf_ms, self.bars): ("candle", m.id) for m in self.markets}
                if self.include_oi:
                    futs[pool.submit(self.client.open_interest, self.primary_market, self.tf_ms, 100)] = ("oi", None)
                if self.include_footprint:
                    futs[pool.submit(self.client.footprint, self.primary_market, self.tf_ms, 5)] = ("fp", None)
                if self.include_zones:
                    futs[pool.submit(self.client.zones, self.primary_market, "day")] = ("zones", None)
                if self.include_liq_tensor:
                    futs[pool.submit(self.client.liq_tensor, f"{self.asset.upper()}USDT", self.liq_interval, "aggregated", True)] = ("liq", None)

                for fut in concurrent.futures.as_completed(futs):
                    kind, mid = futs[fut]
                    try:
                        val = fut.result()
                    except Exception:
                        continue
                    if kind == "candle" and mid:
                        self.candles[mid] = val
                    elif kind == "oi":
                        self.oi_bars = val
                    elif kind == "fp" and isinstance(val, dict) and val.get("bars"):
                        self.footprint_latest = val["bars"][-1]
                    elif kind == "zones":
                        self.zones_day = val
                    elif kind == "liq":
                        self.liq_tensor_summary = val

        await loop.run_in_executor(None, _load)
        self.last_update_ms = int(time.time() * 1000)

    def _apply_candle_tick(self, market: MarketSpec, raw_candle: Dict[str, Any]) -> Dict[str, Any]:
        bar = {
            "time": int(raw_candle["time"]),
            "open": float(raw_candle["open"]),
            "high": float(raw_candle["high"]),
            "low": float(raw_candle["low"]),
            "close": float(raw_candle["close"]),
            "vbuy": float(raw_candle["vbuy"]),
            "vsell": float(raw_candle["vsell"]),
        }
        bar.update(bar_buy_sell_delta(bar, market))
        arr = self.candles[market.id]
        if arr and arr[-1]["time"] == bar["time"]:
            arr[-1] = bar
        elif not arr or bar["time"] > arr[-1]["time"]:
            arr.append(bar)
            if len(arr) > self.bars:
                del arr[0]
        else:
            # Out-of-order update to an earlier bar
            for i in range(len(arr) - 1, -1, -1):
                if arr[i]["time"] == bar["time"]:
                    arr[i] = bar
                    break
        return bar

    def _apply_oi_tick(self, bar: Dict[str, Any]) -> Dict[str, Any]:
        aligned_t = align_bar_ms(self.tf_ms, int(bar["t"]))
        if self.oi_bars and self.oi_bars[-1]["t"] == aligned_t:
            prev = self.oi_bars[-1]
            self.oi_bars[-1] = {
                "t": aligned_t,
                "o": prev["o"],
                "h": max(prev["h"], float(bar["h"])),
                "l": min(prev["l"], float(bar["l"])),
                "c": float(bar["c"]),
                "closed": bool(bar.get("closed", False)),
            }
        else:
            self.oi_bars.append(
                {
                    "t": aligned_t,
                    "o": float(bar["o"]),
                    "h": float(bar["h"]),
                    "l": float(bar["l"]),
                    "c": float(bar["c"]),
                    "closed": bool(bar.get("closed", False)),
                }
            )
            if len(self.oi_bars) > 500:
                del self.oi_bars[0]
        return self.oi_bars[-1]

    def _broadcast(self, event: Dict[str, Any]) -> None:
        self.last_update_ms = int(time.time() * 1000)
        self.tick_count += 1
        for q in list(self._subscribers):
            if q.full():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    def snapshot(self, unit: str = "usd") -> Dict[str, Any]:
        """Instantaneous zero-latency numeric snapshot of all live metrics."""
        venues_out: Dict[str, Dict[str, Any]] = {}
        for m in self.markets:
            arr = self.candles.get(m.id, [])
            ser_all = compute_market_cvd(arr, m, unit=unit, mode="all")
            ser_daily = compute_market_cvd(arr, m, unit=unit, mode="daily")
            ser_24h = compute_market_cvd(arr, m, unit=unit, mode="rolling", rolling_bars=self.rolling_bars_24h)
            lb = arr[-1] if arr else None
            venues_out[m.id] = {
                "id": m.id,
                "key": m.key,
                "label": m.label,
                "kind": m.kind,
                "bars": len(arr),
                "time": lb["time"] if lb else None,
                "open": lb["open"] if lb else None,
                "high": lb["high"] if lb else None,
                "low": lb["low"] if lb else None,
                "close": lb["close"] if lb else None,
                "buy_usd": lb["buy_usd"] if lb else None,
                "sell_usd": lb["sell_usd"] if lb else None,
                "delta_usd": lb["delta_usd"] if lb else None,
                "volume_usd": lb["volume_usd"] if lb else None,
                "cvd_all": ser_all[-1]["value"] if ser_all else None,
                "cvd_daily": ser_daily[-1]["value"] if ser_daily else None,
                "cvd_24h": ser_24h[-1]["value"] if ser_24h else None,
            }

        agg_all = compute_aggregate_cvd(self.candles, self.perp_markets, unit=unit, mode="all")
        agg_daily = compute_aggregate_cvd(self.candles, self.perp_markets, unit=unit, mode="daily")
        agg_24h = compute_aggregate_cvd(self.candles, self.perp_markets, unit=unit, mode="rolling", rolling_bars=self.rolling_bars_24h)

        agg_buy = sum((venues_out[m.id]["buy_usd"] or 0.0) for m in self.perp_markets)
        agg_sell = sum((venues_out[m.id]["sell_usd"] or 0.0) for m in self.perp_markets)
        agg_vol = agg_buy + agg_sell

        primary_bars = self.candles.get(self.primary_market.id, [])
        pb = primary_bars[-1] if primary_bars else None
        price_out = None
        if pb:
            chg = pb["close"] - pb["open"]
            price_out = {
                "market": self.primary_market.id,
                "ticker": self.primary_market.ticker,
                "time": pb["time"],
                "open": pb["open"],
                "high": pb["high"],
                "low": pb["low"],
                "close": pb["close"],
                "change": chg,
                "change_pct": (chg / pb["open"] * 100.0) if pb["open"] else 0.0,
                "volume_base": pb["volume_base"],
                "volume_usd": pb["volume_usd"],
            }

        spot_out = None
        if self.spot_markets:
            sp_all = compute_aggregate_cvd(self.candles, self.spot_markets, unit=unit, mode="all")
            sp_daily = compute_aggregate_cvd(self.candles, self.spot_markets, unit=unit, mode="daily")
            sp_buy = sum((venues_out[m.id]["buy_usd"] or 0.0) for m in self.spot_markets)
            sp_sell = sum((venues_out[m.id]["sell_usd"] or 0.0) for m in self.spot_markets)
            sp_vol = sp_buy + sp_sell
            tot = agg_vol + sp_vol
            spot_out = {
                "cvd_all": sp_all[-1]["value"] if sp_all else None,
                "cvd_daily": sp_daily[-1]["value"] if sp_daily else None,
                "buy_usd": sp_buy,
                "sell_usd": sp_sell,
                "delta_usd": sp_buy - sp_sell,
                "volume_usd": sp_vol,
                "spot_share_pct": round((sp_vol / tot) * 100.0, 4) if tot > 0 else None,
            }

        oi_out = None
        if self.oi_bars:
            lo = self.oi_bars[-1]
            px = pb["close"] if pb else 0.0
            oi_out = {**lo, "oi_usd": round(lo["c"] * px, 2) if px else None}

        return {
            "as_of_ms": int(time.time() * 1000),
            "last_update_ms": self.last_update_ms,
            "connected": self.connected,
            "tick_count": self.tick_count,
            "asset": self.asset,
            "timeframe": self.tf_label,
            "tf_ms": self.tf_ms,
            "unit": unit,
            "price": price_out,
            "aggregate_perp": {
                "cvd_all": agg_all[-1]["value"] if agg_all else None,
                "cvd_daily": agg_daily[-1]["value"] if agg_daily else None,
                "cvd_24h": agg_24h[-1]["value"] if agg_24h else None,
                "buy_usd": agg_buy,
                "sell_usd": agg_sell,
                "delta_usd": agg_buy - agg_sell,
                "volume_usd": agg_vol,
            },
            "aggregate_spot": spot_out,
            "venues": venues_out,
            "open_interest": oi_out,
            "footprint_latest": self.footprint_latest,
            "zones_day": self.zones_day,
            "liq_tensor": self.liq_tensor_summary,
            "recent_liquidations": self.recent_liquidations[-25:],
        }

    def _subscription_keys(self) -> List[str]:
        keys = [f"{m.key}:{self.tf_label}" for m in self.markets]
        if self.include_oi:
            keys.append(f"oi:{self.primary_market.key}")
        if self.include_footprint:
            keys.append(f"fp:{self.primary_market.key}:{self.tf_label}")
        return keys

    async def _candles_ws_loop(self) -> None:
        import websockets

        attempt = 0
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            try:
                ticket = await loop.run_in_executor(None, self.client.ws_ticket)
                url = f"wss://candles.bitcoinsensus.com/ws?ticket={urllib.parse.quote(ticket)}"
                async with websockets.connect(
                    url,
                    additional_headers={"Origin": ORIGIN, "User-Agent": UA},
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    attempt = 0
                    self.connected = True
                    await ws.send(json.dumps({"type": "subscribe", "keys": self._subscription_keys()}))
                    self._broadcast({"type": "status", "connected": True, "as_of_ms": int(time.time() * 1000)})

                    while not self._stop.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            continue
                        msg = json.loads(raw)
                        mtype = msg.get("type")
                        key = msg.get("key") or ""

                        if mtype in ("candle.tick", "candle.closed"):
                            mkt_key = ":".join(key.split(":")[:2]).lower()
                            spec = MARKETS_BY_KEY.get(mkt_key)
                            if spec and "candle" in msg:
                                bar = self._apply_candle_tick(spec, msg["candle"])
                                self._broadcast(
                                    {
                                        "type": mtype,
                                        "market_id": spec.id,
                                        "market_key": spec.key,
                                        "candle": bar,
                                        "closed": mtype == "candle.closed",
                                    }
                                )
                        elif mtype in ("oi.tick", "oi.closed") and "bar" in msg:
                            oi = self._apply_oi_tick(msg["bar"])
                            self._broadcast({"type": mtype, "key": key, "bar": oi})
                        elif mtype in ("footprint.tick", "footprint.closed") and "bar" in msg:
                            self.footprint_latest = msg["bar"]
                            self._broadcast({"type": mtype, "key": key, "bar": msg["bar"]})
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.connected = False
                attempt += 1
                delay = (1.0 + random.random() * 3.0) if attempt <= 1 else random.random() * min(30.0, 2.0**attempt)
                self._broadcast({"type": "status", "connected": False, "error": str(exc), "retry_in_s": round(delay, 2)})
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
        self.connected = False

    async def _periodic_refresh_loop(self) -> None:
        loop = asyncio.get_running_loop()
        last_zones = time.time()
        last_liq = time.time()
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=15.0)
                break
            except asyncio.TimeoutError:
                pass
            now = time.time()
            if self.include_zones and now - last_zones >= 60.0:
                last_zones = now
                try:
                    self.zones_day = await loop.run_in_executor(None, self.client.zones, self.primary_market, "day")
                    self._broadcast({"type": "zones.update", "zones_day": self.zones_day})
                except Exception:
                    pass
            if self.include_liq_tensor and now - last_liq >= 300.0:
                last_liq = now
                try:
                    self.liq_tensor_summary = await loop.run_in_executor(
                        None, self.client.liq_tensor, f"{self.asset.upper()}USDT", self.liq_interval, "aggregated", True
                    )
                    self._broadcast({"type": "liq_tensor.update", "liq_tensor": self.liq_tensor_summary})
                except Exception:
                    pass

    async def _binance_liq_loop(self, url: str, market_id: str, coinm_mult: Optional[float] = None) -> None:
        import websockets

        attempt = 0
        while not self._stop.is_set():
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    attempt = 0
                    while not self._stop.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            continue
                        d = json.loads(raw)
                        if d.get("e") != "forceOrder" or not d.get("o"):
                            continue
                        o = d["o"]
                        q = float(o.get("q") or 0.0)
                        ap = float(o.get("ap") or 0.0)
                        if q <= 0 or ap <= 0:
                            continue
                        ev = {
                            "type": "liquidation",
                            "market_id": market_id,
                            "time": int(o.get("T") or time.time() * 1000),
                            "price": ap,
                            "usd_size": (q * coinm_mult) if coinm_mult else (q * ap),
                            "side": "long" if o.get("S") == "SELL" else "short",
                        }
                        self.recent_liquidations.append(ev)
                        if len(self.recent_liquidations) > 200:
                            del self.recent_liquidations[0]
                        self._broadcast(ev)
            except asyncio.CancelledError:
                break
            except Exception:
                attempt += 1
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=min(30.0, 2.0**attempt))
                except asyncio.TimeoutError:
                    pass

    async def _bybit_liq_loop(self, url: str, topic: str, market_id: str, is_inverse: bool) -> None:
        import websockets

        attempt = 0
        while not self._stop.is_set():
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    attempt = 0
                    await ws.send(json.dumps({"op": "subscribe", "args": [topic]}))
                    while not self._stop.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            continue
                        d = json.loads(raw)
                        if d.get("topic") != topic or not d.get("data"):
                            continue
                        rows = d["data"] if isinstance(d["data"], list) else [d["data"]]
                        for item in rows:
                            sz = float(item.get("v") or item.get("size") or 0.0)
                            px = float(item.get("p") or item.get("price") or 0.0)
                            if sz <= 0 or px <= 0:
                                continue
                            side_raw = str(item.get("S") or item.get("side") or "")
                            ev = {
                                "type": "liquidation",
                                "market_id": market_id,
                                "time": int(item.get("T") or item.get("updatedTime") or d.get("ts") or time.time() * 1000),
                                "price": px,
                                "usd_size": sz if is_inverse else (sz * px),
                                "side": "long" if side_raw.lower() == "sell" else "short",
                            }
                            self.recent_liquidations.append(ev)
                            if len(self.recent_liquidations) > 200:
                                del self.recent_liquidations[0]
                            self._broadcast(ev)
            except asyncio.CancelledError:
                break
            except Exception:
                attempt += 1
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=min(30.0, 2.0**attempt))
                except asyncio.TimeoutError:
                    pass

    async def start(self) -> None:
        """Bootstrap history and launch background real-time WebSocket tasks."""
        self._stop.clear()
        await self.bootstrap()
        self._tasks = [
            asyncio.create_task(self._candles_ws_loop(), name="mp-candles-ws"),
            asyncio.create_task(self._periodic_refresh_loop(), name="mp-periodic-refresh"),
        ]
        if self.include_liquidations and self.asset in FORCE_ORDER_CONFIG:
            fc = FORCE_ORDER_CONFIG[self.asset]
            self._tasks.extend(
                [
                    asyncio.create_task(self._binance_liq_loop(fc["binance_linear"], "binance"), name="mp-bn-lin-liq"),
                    asyncio.create_task(
                        self._binance_liq_loop(fc["binance_coinm"], "binance_coinm", fc["coinm_contract_usd"]),
                        name="mp-bn-cm-liq",
                    ),
                    asyncio.create_task(
                        self._bybit_liq_loop("wss://stream.bybit.com/v5/public/linear", fc["bybit_linear_topic"], "bybit", False),
                        name="mp-bb-lin-liq",
                    ),
                    asyncio.create_task(
                        self._bybit_liq_loop("wss://stream.bybit.com/v5/public/inverse", fc["bybit_inverse_topic"], "bybit_inverse", True),
                        name="mp-bb-inv-liq",
                    ),
                ]
            )

    async def stop(self) -> None:
        """Gracefully close all WebSocket streams and background tasks."""
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def __aenter__(self) -> "MindPillarLiveFeed":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    async def stream(self, max_queue: int = 512) -> AsyncIterator[Dict[str, Any]]:
        """Yield live events (candle.tick, candle.closed, oi.tick, footprint.tick, liquidation, etc.)."""
        q: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=max_queue)
        self._subscribers.append(q)
        try:
            while not self._stop.is_set():
                ev = await q.get()
                yield ev
        finally:
            if q in self._subscribers:
                self._subscribers.remove(q)


# ---------------------------------------------------------------------------
# CLI Quick Demo
# ---------------------------------------------------------------------------

def _fmt_compact(val: Optional[float]) -> str:
    if val is None:
        return "n/a"
    a = abs(val)
    s = "-" if val < 0 else ""
    if a >= 1e9:
        return f"{s}{a/1e9:.2f} B"
    if a >= 1e6:
        return f"{s}{a/1e6:.1f} M"
    if a >= 1e3:
        return f"{s}{a/1e3:.1f} K"
    return f"{s}{a:.0f}"


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="MindPillar /cvd/btc/ real-time numeric data CLI")
    ap.add_argument("--asset", default="btc", choices=["btc", "eth", "sol"])
    ap.add_argument("--tf", default="15m")
    ap.add_argument("--bars", type=int, default=1000)
    ap.add_argument("--stream", action="store_true", help="Stream live WebSocket updates")
    ap.add_argument("--json", action="store_true", help="Print raw JSON snapshot")
    args = ap.parse_args()

    if args.stream:
        async def _run_stream() -> None:
            async with MindPillarLiveFeed(asset=args.asset, tf=args.tf, bars=args.bars) as feed:
                print("Connected! Streaming live updates (Ctrl+C to stop)...")
                async for ev in feed.stream():
                    snap = feed.snapshot()
                    px = (snap["price"] or {}).get("close")
                    agg = snap["aggregate_perp"]
                    if args.json:
                        print(json.dumps({"event": ev, "snapshot": snap}))
                    else:
                        print(
                            f"[{ev['type']:16s}] {args.asset.upper()}={px:,.2f}  "
                            f"AggCVD={_fmt_compact(agg['cvd_all']):>10s}  "
                            f"Buy={_fmt_compact(agg['buy_usd']):>8s}  Sell={_fmt_compact(agg['sell_usd']):>8s}  "
                            f"OI={(snap['open_interest'] or {}).get('c')}"
                        )

        try:
            asyncio.run(_run_stream())
        except KeyboardInterrupt:
            pass
    else:
        c = MindPillarClient()
        snap = c.full_snapshot(asset=args.asset, tf=args.tf, bars=args.bars, include_series=False)
        if args.json:
            print(json.dumps(snap, indent=2))
        else:
            cvd = snap["cvd"]
            px = cvd["price"]
            print(f"{px['ticker']} ({cvd['timeframe']})  O={px['open']:,.2f} H={px['high']:,.2f} L={px['low']:,.2f} C={px['close']:,.2f} ({px['change_pct']:+.2f}%)")
            print(f"{'Venue':18s} {'CVD ∞':>12s} {'CVD 1D':>12s} {'CVD 24H':>12s} {'Buy':>10s} {'Sell':>10s}")
            agg = cvd["aggregate_perp"]
            print(f"{'Aggregate Perp':18s} {_fmt_compact(agg['cvd_all']):>12s} {_fmt_compact(agg['cvd_daily']):>12s} {_fmt_compact(agg['cvd_24h']):>12s} {_fmt_compact(agg['buy_usd']):>10s} {_fmt_compact(agg['sell_usd']):>10s}")
            for vid, v in cvd["venues"].items():
                print(f"{v['label']:18s} {_fmt_compact(v['cvd_all']):>12s} {_fmt_compact(v['cvd_daily']):>12s} {_fmt_compact(v['cvd_24h']):>12s} {_fmt_compact(v['buy_usd']):>10s} {_fmt_compact(v['sell_usd']):>10s}")
            if "open_interest_latest" in snap:
                oi = snap["open_interest_latest"]
                print(f"Open Interest: {oi['c']:,.2f} {args.asset.upper()} (${_fmt_compact(oi['oi_usd'])})")
            if isinstance(snap.get("zones_day"), dict) and snap["zones_day"].get("dev"):
                dev = snap["zones_day"]["dev"]
                print(f"Today Volume Profile: POC={dev['poc']}  VAH={dev['vah']}  VAL={dev['val']}  EQ={dev['eq']}")
            if isinstance(snap.get("liq_tensor"), dict) and "spot_price" in snap["liq_tensor"]:
                lt = snap["liq_tensor"]
                print(f"Liq Tensor (1d): Above=${_fmt_compact(lt['notional_above_usd'])}  Below=${_fmt_compact(lt['notional_below_usd'])}  Ratio={lt['above_to_below_ratio']}")
            divs = (cvd.get("divergences") or {}).get("divergences") or []
            print(f"Detected CVD Divergences: {len(divs)} (latest: {divs[0]['kind']} {divs[0]['status']} {divs[0]['projection']['divergingCount']}/{divs[0]['projection']['total']})" if divs else "Detected CVD Divergences: 0")
