# MindPillar CVD Real-Time API (`mindpillar_REA`)

Real-time Python SDK and local HTTP + WebSocket API server for all public numeric data behind `https://mindpillar.com/cvd/btc/` (plus `eth` and `sol`), reverse-engineered with **REA (`rea@6.10.1`)**.

All accessed upstream endpoints are **public and unauthenticated** — no API keys, accounts, or cookies are required or stored.

## What Data Is Included

- **Multi-Venue Candles & Exact CVD** (`1m` through `1M`, 17 timeframes):
  - 4 Perpetual Futures venues: `Binance USDT`, `ByBit USDT`, `Binance USD (COIN-M)`, `ByBit USD (Inverse)`
  - 2 Spot venues: `Binance Spot`, `ByBit Spot` + **Spot Share %**
  - Exact MindPillar CVD math: `CVD ∞` (continuous), `CVD 1D` (UTC midnight reset), `CVD 24H` (rolling), in `usd` or `base`, plus **Aggregate CVD**
- **Auto CVD Divergences**:
  - Faithful port of MindPillar's fractal & ATR zigzag pivot divergence detector with multi-venue `k/n` projection, `pending`/`confirmed`/`rejected` resolution, MFE/MAE, R:R, and SFP reclaim targets (`0.618`, `0.786`)
- **Open Interest**: Historical OHLC OI bars + live `oi.tick` / `oi.closed` WebSocket updates
- **Footprint / Orderflow Clusters**: Per-price-level bid (`lb`) / ask (`la`) volumes, delta (`d`), volume (`v`), trade count (`n`), POC, plus live `footprint.tick` / `footprint.closed`
- **Volume Profile Zones & Key Price Levels**:
  - Previous & developing session `POC`, `VAH`, `VAL`, `EQ`, `OHLC` (`day`, `week`, `month`, `year`)
  - Naked/tapped `nPOC`, `tvPOC`, `dLevels`, `wLevels`, `mLevels`, and multi-timeframe **Order Blocks** (`ob`)
  - Arbitrary time-range & per-session Volume Profiles (`/profile`, `/profiles`)
- **Liquidation Heatmap & Liquidation Map Tensor** (`6h`, `12h`, `1d`, `2d`, `3d`, `7d`, `14d`, `30d`, `90d`, `180d`):
  - 7-exchange aggregated or per-exchange summary (`spot_price`, `vwap_4h`, `notional_above_usd`, `notional_below_usd`, top price clusters, leverage breakdown, cohorts) or raw tensor
- **Live Forced Liquidations**: Direct public Binance (`@forceOrder`) & Bybit v5 (`allLiquidation.<SYM>`) streams
- **Feed Health & 24h Volume Stats**: Live feed latency (`lastTradeAgeMs`), 24h trade counts & USD notional

## Quickstart

### 1. Install optional streaming / server deps
`MindPillarClient` (REST) requires **zero third-party packages** (Python 3.10+ stdlib only). For live WebSocket streaming and the local HTTP/WS server:

```bash
pip install -r requirements.txt
```

### 2. Python SDK — Sync Snapshot
```python
import mp

c = mp.MindPillarClient()

# Everything from /cvd/btc/ in one parallel call:
snap = c.full_snapshot(asset="btc", tf="15m", bars=1000)

# Individual calls:
cvd = c.cvd_snapshot(asset="btc", tf="15m", bars=1000, include_spot=True, include_divergences=True)
oi = c.open_interest("binance-btc", tf="15m", bars=100)
fp = c.footprint("binance-btc", tf="15m", bars=20, step=25)
zones = c.zones("binance-btc", period="day")
npocs = c.levels("binance-btc", family="npoc", only_untapped=True)
obs = c.levels("binance-btc", family="ob", only_untapped=True)
liq = c.liq_tensor("BTCUSDT", interval="1d", summary_only=True)
```

### 3. Python SDK — Real-Time WebSocket Stream
```python
import asyncio
import mp

async def main():
    async with mp.MindPillarLiveFeed(asset="btc", tf="15m", bars=1000) as feed:
        async for ev in feed.stream():
            snap = feed.snapshot()
            print(ev["type"], snap["price"]["close"], snap["aggregate_perp"]["cvd_all"])

asyncio.run(main())
```

### 4. CLI Usage
```bash
python mp.py --asset btc --tf 15m           # formatted table
python mp.py --asset btc --tf 15m --json    # full JSON snapshot
python mp.py --asset btc --tf 15m --stream  # live WebSocket updates
```

### 5. Local HTTP + WebSocket API Server
```bash
python server.py --host 127.0.0.1 --port 8765 --asset btc --tf 15m
```

Endpoints (interactive docs at `http://127.0.0.1:8765/docs`):
- `GET /health`
- `GET /v1/live` — zero-latency snapshot from hot in-memory WS state
- `GET /v1/snapshot` — full on-demand numeric snapshot
- `GET /v1/cvd`, `/v1/candles`, `/v1/divergences`, `/v1/oi`, `/v1/footprint`, `/v1/zones`, `/v1/levels`, `/v1/profile`, `/v1/profiles`, `/v1/liq-tensor`, `/v1/stats`, `/v1/config`
- `WS  /ws/live` — real-time push stream

### 6. Live Self-Test
```bash
python selftest_live.py
```

## Exact MindPillar CVD Divergence Formula

`mp.detect_divergences()` and `mp.project_divergence()` implement MindPillar's exact client-side divergence engine:

1. **Absorption Rule (wick-to-wick)**:
   - **Bearish**: Price Lower High (`curr.high < prev.high`) + CVD Higher High (`curr.cvd > prev.cvd`).
   - **Bullish**: Price Higher Low (`curr.low > prev.low`) + CVD Lower Low (`curr.cvd < prev.cvd`).
2. **Pivot Modes**:
   - `preset="fractal"`: ±2-bar wick fractals (`windowSize=2`), `maxSpanBars=20`, plus live `forming` tail pivot.
   - `preset="zigzag"`: 14-bar ATR ZigZag (`2.0 * ATR`, `3.0 * ATR` on `3m`), `gap-else-prev` pairing (`minGapAtr=16`), filtered by `minSizeAtr >= 3.0`, `minRr >= 1.0`, `oneLinePerAnchor=True`, and `hidePlayedOut=True`.
   - `preset="raw"`: Unfiltered output with all enrichment fields.
3. **Structural Guards**: Up to 50 prior pivots (`engulfLen`), monotonic envelope occlusion, straight-line crossing tolerance (`0.5 * avg_range`), and inner-divergence suppression.
4. **Zero-Lookahead Entry & R:R**:
   - `entryPrice`: close of the lock/confirmation bar (`lockIndex`).
   - `targetPrice`: extreme opposite wick strictly between `prev` and `curr`.
   - `invalidationPrice`: `prev.price`; stop distance measured from `entryPrice` to `curr.price`.
   - Lifecycle `state`: `forming` → `armed` → `confirmed` or `rejected`.
5. **Multi-Venue `k/n` Projection & SFP Reclaim**:
   - Scores every venue with range-normalized strength `|Δcvd| / (max_cvd - min_cvd)` across `[prev..curr]` (`divergingCount / total`, `strengthAvg`).
   - On `rejected` divergences, scans 50 bars for a close reclaiming `prev.price` and computes `0.618` / `0.786` SFP Fib targets.

```python
import mp

c = mp.MindPillarClient()
snap = c.cvd_snapshot("btc", tf="15m", bars=1000, include_series=True)
candles = snap["series"]["candles"]["binance-btc"]
cvd = snap["series"]["cvd"]

# High-signal Swing/ZigZag preset (minSizeAtr>=3, minRr>=1, hidePlayedOut):
zz = mp.detect_divergences(candles, cvd, preset="zigzag", tf_ms=snap["tf_ms"])

# Fractal preset requiring >= 3/4 venues agreeing:
fr = mp.detect_divergences(candles, cvd, preset="fractal", min_lines=3, tf_ms=snap["tf_ms"])

# Manual 2-point measure-divergence projection:
proj = mp.project_divergence(cvd, t1_ms=candles[-30]["time"], price1=candles[-30]["low"],
                             t2_ms=candles[-5]["time"], price2=candles[-5]["low"])
```

HTTP equivalent: `GET http://127.0.0.1:8765/v1/divergences?asset=btc&tf=15m&preset=zigzag&min_lines=3`

## Documentation
See [`spec.md`](spec.md) for the full reverse-engineered protocol, endpoint table, WebSocket framing, CVD/divergence formulas, and known limitations.
