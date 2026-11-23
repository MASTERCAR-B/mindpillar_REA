# MindPillar CVD (`/cvd/btc/`) Public Numeric API — Spec

Reverse-engineered via **REA (`rea@6.10.1`)** from `https://mindpillar.com/cvd/btc/` on `2026-11-23`.

Status legend:
- **VERIFIED** = observed in REA browser capture and/or called directly live with HTTP 200 / live WS frames confirmed.
- **CODE** = read directly from MindPillar's shipped JS bundles (`use-wake-catchup-DVsv7BE7.js`, `index-CotLkwxA.js`, `chart-page-boundary-C1k_BLj_.js`, `cvd-BCbaMrj-.js`).
- **LIMITATION** = observed caveat or boundary.

REA Evidence references from this investigation:
- Browser scenario capture #1: `ev_41adfc27209cab6c0d378eb9327e19e7a9d89295ce9653ef092931faf9087e8c`
- Browser scenario capture #2 (76/76 HTTP 2xx, 5 WS streams, 15 WS frames, live accessibility tree): `ev_f55dd80aa2300c3921847383351caabd511c1762b028717dd3a26dd4774d9863`
- Static JS application analysis (`cvd-CzS63RM2.js`, `cvd-BCbaMrj-.js`): `ev_669de0e2d0d187c386303794bf534e5245d4f32723269856c5705d3775280dd4`

---

## 1. Architecture (VERIFIED)

**Zero authentication is required for any numeric data endpoint.** All requests use `credentials: "omit"`. Recommended headers: `Origin: https://mindpillar.com`, `Referer: https://mindpillar.com/`.

| Layer | Base URL | Protocol | Data Served |
|---|---|---|---|
| Market & Orderflow Backend | `https://candles.bitcoinsensus.com` | REST JSON + WebSocket (`GET /ws/ticket` → `wss://.../ws?ticket=<uuid>`) | Historical & live OHLC + buy/sell volume (`vbuy`, `vsell`), Open Interest (`/oi`), Footprint clusters (`/footprint`), Volume Profile Zones (`/zones`), Key Price Levels & Order Blocks (`/levels`), Range/Session Volume Profiles (`/profile`, `/profiles`), feed health (`/stats`), limits (`/config`) |
| Charts / Liquidation CDN | `https://charts.prd.bitcoinsensus.com` | REST gzip JSON (ETag / 304) | Liquidation Heatmap & Map Tensor (`/v1/liq-tensor/{SYMBOL}_{INTERVAL}.json`) across 7 exchanges (`binance`, `bybit`, `okx`, `hyperliquid`, `bitget`, `gate`, `mexc`) |
| Live Forced Liquidations | `wss://fstream.binance.com`, `wss://dstream.binance.com`, `wss://stream.bybit.com` | Public Exchange WebSockets | Real-time forced liquidation fills (Binance USDT-M/COIN-M `@forceOrder`, Bybit Linear/Inverse `allLiquidation.<SYM>`) |

---

## 2. Markets & Units (VERIFIED + CODE)

Defined in `use-wake-catchup-DVsv7BE7.js` (`dg`, `Cg`):

| Market ID | Wire Key | Display Label | Kind | Native `vbuy`/`vsell` Unit |
|---|---|---|---|---|
| `binance-btc` | `BINANCE_FUTURES:btcusdt` | Binance USDT | perp | `base` (BTC → multiply by `close` for USD) |
| `bybit-btc` | `BYBIT:BTCUSDT` | ByBit USDT | perp | `base` (BTC → multiply by `close` for USD) |
| `binance_coinm-btc` | `BINANCE_FUTURES:btcusd_perp` | Binance USD | perp | `usd` (already USD notional) |
| `bybit_inverse-btc` | `BYBIT:BTCUSD` | ByBit USD | perp | `usd` (already USD notional) |
| `binance_spot-btc` | `BINANCE:btcusdt` | Binance Spot | spot | `base` (BTC) |
| `bybit_spot-btc` | `BYBIT_SPOT:BTCUSDT` | ByBit Spot | spot | `base` (BTC) |

Same 4 perp markets exist for `eth` (`ethusdt`, `ETHUSDT`, `ethusd_perp`, `ETHUSD`) and `sol` (`solusdt`, `SOLUSDT`, `solusd_perp`, `SOLUSD`), plus ETH spot (`BINANCE:ethusdt`, `BYBIT_SPOT:ETHUSDT`).

### Supported Timeframes (`Wh`, 17 total)
`1m` (60s), `3m` (180s), `5m` (300s), `15m` (900s), `30m` (1800s), `1h` (3600s), `2h`, `3h`, `4h`, `6h`, `8h`, `12h`, `1d` (86400s), `2D`, `3D`, `1w`, `1M`.

---

## 3. REST Endpoint Table (ALL VERIFIED HTTP 200)

| Endpoint | Path | Notes |
|---|---|---|
| Config | `GET https://candles.bitcoinsensus.com/config` | `schemaVersion: 2`, `candleWindow.maxBars` (40,000 per tf) |
| Stats | `GET https://candles.bitcoinsensus.com/stats` | Live feed latency (`lastTradeAgeMs`), 24h trade counts & USD notional |
| WS Ticket | `GET https://candles.bitcoinsensus.com/ws/ticket` | Returns `{"ticket": "<uuid>"}` |
| Historical Candles | `GET https://candles.bitcoinsensus.com/historical/{fromMs}/{toMs}/{tfMs}/{urlEncodedKey}` | Returns `{columns, results}` with `time, open, high, low, close, vbuy, vsell, market` (`cbuy/csell/lbuy/lsell` are `null`) |
| Open Interest | `GET https://candles.bitcoinsensus.com/oi/{fromMs}/{toMs}/{tfMs}/{urlEncodedKey}` | Returns `{bars: [{t, o, h, l, c, closed}]}` |
| Footprint Clusters | `GET https://candles.bitcoinsensus.com/footprint/{fromMs}/{toMs}/{tfMs}/{urlEncodedKey}?step={step}` | Returns `{step, bars: [{t, o, h, l, c, d, v, n, lp, lb, la, closed}]}` |
| Volume Profile Zones | `GET https://candles.bitcoinsensus.com/zones/{urlEncodedKey}?period={day\|week\|month\|year}` | Returns `{prev, dev}` with `open, high, low, close, eq, poc, vah, val` |
| Key Levels & Order Blocks | `GET https://candles.bitcoinsensus.com/levels/{urlEncodedKey}?family={npoc\|tvpoc\|dlevels\|wlevels\|mlevels\|ob}` | For `tvpoc`, add `&chartTf={tfMs}`; for weekly/monthly nPOC/tvPOC, add `&period={week\|month}` |
| Range Volume Profile | `GET https://candles.bitcoinsensus.com/profile/{urlEncodedKey}?from={ms}&to={ms}&step={step}&valueArea=0.7` | Returns `{poc, vah, val, hi, lo, peak, rows: [{p, vbuy, vsell, v}]}` |
| Session Volume Profiles | `GET https://candles.bitcoinsensus.com/profiles/{urlEncodedKey}?period={day\|week\|month}&from={ms}&to={ms}&step={step}` | Returns `{sessions: [...]}` |
| Liquidation Tensor | `GET https://charts.prd.bitcoinsensus.com/v1/liq-tensor/{SYMBOL}_{INTERVAL}.json` | Gzip JSON, ETag-cached; intervals: `6h, 12h, 1d, 2d, 3d, 7d, 14d, 30d, 90d, 180d` |

---

## 4. Real-Time WebSocket Protocol (VERIFIED LIVE)

1. `GET https://candles.bitcoinsensus.com/ws/ticket` → `{"ticket": "<uuid>"}`
2. Connect to `wss://candles.bitcoinsensus.com/ws?ticket=<uuid>`
3. Server greets with `{"type": "connected", "clientId": "<uuid>"}`
4. Client sends:
   ```json
   {
     "type": "subscribe",
     "keys": [
       "BINANCE_FUTURES:btcusdt:15m",
       "BINANCE_FUTURES:btcusd_perp:15m",
       "BYBIT:BTCUSDT:15m",
       "BYBIT:BTCUSD:15m",
       "BINANCE:btcusdt:15m",
       "oi:BINANCE_FUTURES:btcusdt",
       "fp:BINANCE_FUTURES:btcusdt:15m"
     ]
   }
   ```
5. Server streams live JSON messages:
   - `candle.tick` / `candle.closed`: `{"type":"candle.tick","key":"BINANCE_FUTURES:btcusdt:15m","candle":{"time":...,"open":...,"high":...,"low":...,"close":...,"vbuy":...,"vsell":...,"closed":false}}`
   - `oi.tick` / `oi.closed`: `{"type":"oi.tick","key":"oi:BINANCE_FUTURES:btcusdt","bar":{"t":...,"o":...,"h":...,"l":...,"c":...,"closed":false}}`
   - `footprint.tick` / `footprint.closed`: `{"type":"footprint.tick","key":"fp:BINANCE_FUTURES:btcusdt:15m","bar":{"t":...,"o":...,"h":...,"l":...,"c":...,"d":...,"v":...,"n":...,"lp":[...],"lb":[...],"la":[...],"closed":false}}`
6. Unsubscribe with `{"type": "unsubscribe", "keys": [...]}`.

---

## 5. Exact CVD & Divergence Formulas (VERIFIED TO LAST DECIMAL)

From `use-wake-catchup-DVsv7BE7.js` (`Yk`, `Xk`, `Zk`, `$k`, `eA`):
- Per-bar delta:
  - `raw_delta = vbuy - vsell`
  - If `unit == "usd"`: `delta = raw_delta * close` when `market.unit == "base"`, else `raw_delta`
  - If `unit == "base"`: `delta = raw_delta` when `market.unit == "base"`, else `raw_delta / close`
- Cumulative modes:
  - `all` (`CVD ∞`): running sum from first loaded bar (page default loads 1000 bars). Verified bit-for-bit against captured live UI (`-1.84 B`, `-422.9 M`, `-530.1 M`, `-88.3 M`).
  - `daily` (`CVD 1D`): running sum resetting to `0` at `00:00 UTC`.
  - `rolling` (`CVD 24H`): `prefix[i] - prefix[len - 1 - rolling_bars]`.
- Aggregate CVD: sums bar deltas across all selected markets aligned by `time`, then applies `compute_cvd_series`.
- **Exact MindPillar CVD Divergence Formula** (`chart-page-boundary-C1k_BLj_.js` `Mc`/`Iee`/`Ac`/`kc`/`XC`/`JC`/`YC`/`qC`/`Xc`/`Zc`/`Qc`/`tl` + `use-wake-catchup-DVsv7BE7.js` `hk`/`_k`):
  1. **Pivots (wick-only)**:
     - **Fractal (`windowSize=2`)**: High pivot `high[i] > max(high[i-w..i-1])` & `> max(high[i+1..i+w])`; Low pivot `low[i] < min(low[i-w..i-1])` & `< min(low[i+1..i+w])` (with `minDistance+1` on lows). Appends a live `pending` tail extreme if the right tail has no settled pivot. `lockIndex = barIndex + windowSize`.
     - **Swing / ATR ZigZag (`zigzagAtrPeriod=14`, `zigzagAtrMult=2.0`, or `3.0` on `3m`)**: Reversal triggers when `hi - low[i] >= mult * ATR14[i]` (locks high pivot at `lockBarIndex = i`) or `high[i] - lo >= mult * ATR14[i]` (locks low pivot).
  2. **Core Absorption Divergence Rule (`jc`)**:
     - **Bearish**: Price makes a **Lower High** (`curr.high < prev.high`) while CVD makes a **Higher High** (`curr.cvd > prev.cvd`).
     - **Bullish**: Price makes a **Higher Low** (`curr.low > prev.low`) while CVD makes a **Lower Low** (`curr.cvd < prev.cvd`).
     - Scanned back up to `engulfLen = 50` pivots with monotonic envelope occlusion + straight-line non-crossing tolerance `0.5 * mean(high - low over [prev..curr])`. ZigZag uses `pairRule="gap-else-prev"` (`minGapAtr = 16`, `keepAdjacent = true`).
     - *(Manual 2-point measure tool `_k` / `project_divergence` detects any opposite price vs CVD slope: `sign(Δprice) != sign(Δcvd)`.)*
  3. **Multi-Venue Fusion & Inner Suppression (`Iee`, `Pc`)**: Runs on every venue CVD independently, merges by `kind|prev.timeMs|curr.timeMs` (max `|cvdDelta|`), drops inner pairs nested inside an unresolved outer divergence.
  4. **Zero-Lookahead Entry, Target, Invalidation & R:R (`YC`, `JC`, `qC`, `Ac`)**:
     - `targetPrice`: extreme opposite wick strictly between `prev` and `curr` (`max(high)` for bullish, `min(low)` for bearish).
     - `invalidationPrice`: `prev.price`.
     - `entryPrice`: `candles[min(lockIndex, N-1)].close` (close of the confirmation/lock bar, zero lookahead).
     - `reward = |targetPrice - entryPrice|` (signed), `risk = |entryPrice - curr.price|` (stop at `curr` wick), `rr = reward / risk` if both `> 0`.
     - `structureAtr = |targetPrice - curr.price| / ATR14(curr.barIndex)`.
     - Causal resolution starts at `max(curr.barIndex, lockBarIndex) + 1`: `confirmed` if `targetPrice` hits first, `rejected` if `prev.price` breaks first, else `forming` (if pending) or `armed`.
  5. **Per-Venue `k/n` Projection & Strength (`hk`)**: For each venue `v`, `strength_v = clamp01(|cvd2 - cvd1| / (max_cvd[prev..curr] - min_cvd[prev..curr]))`, reported as `divergingCount / total` and `strengthAvg`.
  6. **Failed-Divergence SFP Reclaim (`kc`)**: On `rejected` divergences, scans up to `sfpReclaimLen = 50` bars for a close reclaiming back inside `prev.price`, then projects `0.618` and `0.786` Fib targets from `sfpExtreme` to `structureExtreme`.
  7. **Chart Filter Presets (`Yc` / `Jc`)**:
     - `fractal`: `maxSpanBars=20`, hides rejected (`statusFilter="confirmed"`), `showPending=True`.
     - `zigzag`: `minSizeAtr=3`, `minRr=1`, `oneLinePerAnchor=True`, `hidePlayedOut=True` (suppresses new pairs on an anchor once an earlier qualifying divergence from that anchor already confirmed before `lockIndex`).

---

## 6. Observed Limitations

- **Bybit liquidation topic bug in MindPillar frontend**: MindPillar's JS still subscribes to `liquidation.BTCUSDT` / `liquidation.BTCUSD`, which Bybit v5 rejects with `handler not found`. `mp.py` uses Bybit v5's active `allLiquidation.BTCUSDT` / `allLiquidation.BTCUSD` topics (verified `success: true`).
- **`cbuy`, `csell`, `lbuy`, `lsell` columns**: Present in `/historical` column headers, always `null` in live responses.
- **Public server fair-use**: `candles.bitcoinsensus.com` and `charts.prd.bitcoinsensus.com` are public unauthenticated services; `mp.py` uses WebSocket streaming + ETag caching (`If-None-Match`) rather than aggressive polling.
