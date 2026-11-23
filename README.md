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

## Documentation
See [`spec.md`](spec.md) for the full reverse-engineered protocol, endpoint table, WebSocket framing, CVD/divergence formulas, and known limitations.
