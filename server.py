"""
server.py - Local real-time HTTP + WebSocket API server for MindPillar CVD (/cvd/btc/).

Run:
    python server.py --host 127.0.0.1 --port 8765 --asset btc --tf 15m

Endpoints:
    GET  /health                     Server + live WebSocket feed status
    GET  /v1/live                    Zero-latency real-time numeric snapshot from hot WS state
    GET  /v1/snapshot                Full on-demand numeric snapshot (CVD, OI, footprint, zones, levels, liq-tensor, divergences)
    GET  /v1/cvd                     Per-venue + aggregate CVD (all / daily / rolling, usd / base)
    GET  /v1/candles                 OHLC + vbuy/vsell + USD/base delta for any market & timeframe
    GET  /v1/divergences             Auto-detected multi-venue CVD divergences (fractal or zigzag)
    GET  /v1/oi                      Open Interest OHLC bars
    GET  /v1/footprint               Orderflow footprint price-level clusters (lp, lb, la, d, v, n, poc)
    GET  /v1/zones                   Daily / Weekly / Monthly / Yearly Volume Profile Zones (POC, VAH, VAL, EQ)
    GET  /v1/levels                  Key price levels (nPOC, tvPOC, dLevels, wLevels, mLevels, Order Blocks)
    GET  /v1/profile                 Arbitrary time-range Volume Profile
    GET  /v1/profiles                Per-session Volume Profiles
    GET  /v1/liq-tensor              Liquidation Heatmap & Map summary (or full raw tensor)
    GET  /v1/stats                   Bitcoinsensus feed latency & 24h trade/notional stats
    GET  /v1/config                  Upstream bar window config & supported timeframes
    WS   /ws/live                    Real-time stream of ticks + live CVD/OI/footprint/liquidation updates
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect

import mp

DEFAULT_ASSET = os.environ.get("MP_ASSET", "btc")
DEFAULT_TF = os.environ.get("MP_TF", "15m")
DEFAULT_BARS = int(os.environ.get("MP_BARS", "1000"))

client = mp.MindPillarClient()
live_feed: Optional[mp.MindPillarLiveFeed] = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global live_feed
    live_feed = mp.MindPillarLiveFeed(
        asset=DEFAULT_ASSET,
        tf=DEFAULT_TF,
        bars=DEFAULT_BARS,
        include_spot=True,
        include_oi=True,
        include_footprint=True,
        include_liquidations=True,
        include_liq_tensor=True,
        include_zones=True,
        client=client,
    )
    await live_feed.start()
    try:
        yield
    finally:
        await live_feed.stop()


app = FastAPI(
    title="MindPillar CVD Real-Time API",
    version="1.0.0",
    description="Real-time numeric data API for https://mindpillar.com/cvd/btc/ (CVD, candles, OI, footprint, volume profiles, levels, liquidation tensor, divergences, and live WebSockets).",
    lifespan=lifespan,
)


async def _run(fn, *args, **kwargs):
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except mp.MindPillarHTTPError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except mp.MindPillarError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "live_feed": {
            "asset": live_feed.asset if live_feed else None,
            "timeframe": live_feed.tf_label if live_feed else None,
            "connected": live_feed.connected if live_feed else False,
            "tick_count": live_feed.tick_count if live_feed else 0,
            "last_update_ms": live_feed.last_update_ms if live_feed else 0,
        },
    }


@app.get("/v1/live")
async def get_live(unit: str = Query("usd", pattern="^(usd|base)$")) -> Dict[str, Any]:
    """Zero-latency live numeric snapshot from the hot in-memory WebSocket feed."""
    if live_feed is None:
        raise HTTPException(status_code=503, detail="Live feed not initialized")
    return live_feed.snapshot(unit=unit)


@app.get("/v1/snapshot")
async def get_full_snapshot(
    asset: str = Query("btc", pattern="^(btc|eth|sol)$"),
    tf: str = Query("15m"),
    bars: int = Query(1000, ge=10, le=40000),
    mode: str = Query("all", pattern="^(all|daily|rolling)$"),
    unit: str = Query("usd", pattern="^(usd|base)$"),
    include_spot: bool = True,
    include_divergences: bool = True,
    include_oi: bool = True,
    include_footprint: bool = True,
    include_zones: bool = True,
    include_levels: bool = True,
    include_liq_tensor: bool = True,
    liq_interval: str = Query("1d"),
    include_series: bool = False,
) -> Dict[str, Any]:
    return await _run(
        client.full_snapshot,
        asset=asset,
        tf=tf,
        bars=bars,
        mode=mode,
        unit=unit,
        include_spot=include_spot,
        include_divergences=include_divergences,
        include_oi=include_oi,
        include_footprint=include_footprint,
        include_zones=include_zones,
        include_levels=include_levels,
        include_liq_tensor=include_liq_tensor,
        liq_interval=liq_interval,
        include_series=include_series,
    )


@app.get("/v1/cvd")
async def get_cvd(
    asset: str = Query("btc", pattern="^(btc|eth|sol)$"),
    tf: str = Query("15m"),
    bars: int = Query(1000, ge=10, le=40000),
    mode: str = Query("all", pattern="^(all|daily|rolling)$"),
    unit: str = Query("usd", pattern="^(usd|base)$"),
    rolling_hours: float = Query(24.0, gt=0),
    include_spot: bool = True,
    include_series: bool = True,
    include_divergences: bool = False,
    divergence_mode: str = Query("fractal", pattern="^(fractal|zigzag)$"),
) -> Dict[str, Any]:
    return await _run(
        client.cvd_snapshot,
        asset=asset,
        tf=tf,
        bars=bars,
        mode=mode,
        unit=unit,
        rolling_hours=rolling_hours,
        include_spot=include_spot,
        include_series=include_series,
        include_divergences=include_divergences,
        divergence_mode=divergence_mode,
    )


@app.get("/v1/candles")
async def get_candles(
    market: str = Query("binance-btc"),
    tf: str = Query("15m"),
    bars: int = Query(500, ge=1, le=40000),
    from_ms: Optional[int] = None,
    to_ms: Optional[int] = None,
) -> Dict[str, Any]:
    spec = mp.resolve_market(market)
    tf_label, tf_ms = mp.resolve_tf(tf)
    rows = await _run(client.candles, spec, tf_ms, bars, from_ms, to_ms)
    return {"market": spec.id, "key": spec.key, "timeframe": tf_label, "tf_ms": tf_ms, "count": len(rows), "candles": rows}


@app.get("/v1/divergences")
async def get_divergences(
    asset: str = Query("btc", pattern="^(btc|eth|sol)$"),
    tf: str = Query("15m"),
    bars: int = Query(1000, ge=50, le=10000),
    mode: str = Query("all", pattern="^(all|daily|rolling)$"),
    unit: str = Query("usd", pattern="^(usd|base)$"),
    pivot_mode: str = Query("fractal", pattern="^(fractal|zigzag)$"),
    window_size: int = Query(2, ge=1, le=30),
    min_lines: int = Query(1, ge=1, le=4),
    status_filter: str = Query("all", pattern="^(all|pending|confirmed|rejected)$"),
) -> Dict[str, Any]:
    snap = await _run(
        client.cvd_snapshot,
        asset=asset,
        tf=tf,
        bars=bars,
        mode=mode,
        unit=unit,
        include_spot=False,
        include_series=True,
        include_divergences=False,
    )
    primary_id = mp.asset_markets(asset, include_spot=False)[0].id
    candles = snap["series"]["candles"][primary_id]
    cvd_map = snap["series"]["cvd"]
    divs = mp.detect_divergences(
        candles,
        cvd_map,
        pivot_mode=pivot_mode,
        window_size=window_size,
        min_lines=min_lines,
        status_filter=status_filter,
    )
    return {
        "asset": asset,
        "timeframe": snap["timeframe"],
        "bars": len(candles),
        "pivot_mode": pivot_mode,
        "count": len(divs["divergences"]),
        **divs,
    }


@app.get("/v1/oi")
async def get_oi(
    market: str = Query("binance-btc"),
    tf: str = Query("15m"),
    bars: int = Query(200, ge=1, le=5000),
    from_ms: Optional[int] = None,
    to_ms: Optional[int] = None,
) -> Dict[str, Any]:
    spec = mp.resolve_market(market)
    tf_label, tf_ms = mp.resolve_tf(tf)
    rows = await _run(client.open_interest, spec, tf_ms, bars, from_ms, to_ms)
    return {"market": spec.id, "key": spec.key, "timeframe": tf_label, "count": len(rows), "bars": rows}


@app.get("/v1/footprint")
async def get_footprint(
    market: str = Query("binance-btc"),
    tf: str = Query("15m"),
    bars: int = Query(50, ge=1, le=500),
    step: Optional[float] = None,
    from_ms: Optional[int] = None,
    to_ms: Optional[int] = None,
) -> Dict[str, Any]:
    return await _run(client.footprint, market, tf, bars, step, from_ms, to_ms)


@app.get("/v1/zones")
async def get_zones(
    market: str = Query("binance-btc"),
    period: str = Query("day", pattern="^(day|week|month|year)$"),
    method: str = Query("trades", pattern="^(trades|candles)$"),
    chart_tf_ms: Optional[int] = None,
) -> Dict[str, Any]:
    return await _run(client.zones, market, period, method, chart_tf_ms)


@app.get("/v1/levels")
async def get_levels(
    market: str = Query("binance-btc"),
    family: str = Query("npoc", pattern="^(npoc|tvpoc|dlevels|wlevels|mlevels|ob)$"),
    period: str = Query("day", pattern="^(day|week|month)$"),
    chart_tf: str = Query("15m"),
    only_untapped: bool = True,
) -> Dict[str, Any]:
    return await _run(client.levels, market, family, period, chart_tf, only_untapped)


@app.get("/v1/profile")
async def get_range_profile(
    market: str = Query("binance-btc"),
    from_ms: Optional[int] = None,
    to_ms: Optional[int] = None,
    lookback_hours: float = Query(24.0, gt=0),
    step: Optional[float] = None,
    value_area: float = Query(0.7, gt=0.1, lt=1.0),
) -> Dict[str, Any]:
    return await _run(client.range_profile, market, from_ms, to_ms, lookback_hours, step, value_area)


@app.get("/v1/profiles")
async def get_session_profiles(
    market: str = Query("binance-btc"),
    period: str = Query("day", pattern="^(day|week|month)$"),
    days: int = Query(3, ge=1, le=30),
    from_ms: Optional[int] = None,
    to_ms: Optional[int] = None,
    step: Optional[float] = None,
) -> Dict[str, Any]:
    return await _run(client.session_profiles, market, period, days, from_ms, to_ms, step)


@app.get("/v1/liq-tensor")
async def get_liq_tensor(
    symbol: str = Query("BTCUSDT", pattern=r"^[A-Za-z0-9]{2,20}$"),
    interval: str = Query("1d", pattern=r"^(6h|12h|1d|2d|3d|7d|14d|30d|90d|180d)$"),
    exchange: str = Query("aggregated", pattern=r"^[a-z0-9_]{2,32}$"),
    summary_only: bool = True,
    top_levels: int = Query(25, ge=1, le=200),
) -> Dict[str, Any]:
    return await _run(client.liq_tensor, symbol, interval, exchange, summary_only, top_levels)


@app.get("/v1/stats")
async def get_stats() -> Dict[str, Any]:
    return await _run(client.stats)


@app.get("/v1/config")
async def get_config() -> Dict[str, Any]:
    return await _run(client.config)


@app.websocket("/ws/live")
async def ws_live(
    websocket: WebSocket,
    unit: str = Query("usd", pattern=r"^(usd|base)$"),
    include_snapshot: bool = Query(True),
) -> None:
    """Real-time WebSocket push stream of every live CVD, candle, OI, footprint, and liquidation update."""
    if live_feed is None:
        await websocket.close(code=1013)
        return
    if len(live_feed._subscribers) >= 100:
        await websocket.close(code=1013)
        return
    await websocket.accept()
    try:
        # Send immediate initial full live snapshot
        await websocket.send_json({"type": "snapshot", "snapshot": live_feed.snapshot(unit=unit)})
        async for ev in live_feed.stream():
            payload: Dict[str, Any] = {"event": ev}
            if include_snapshot:
                snap = live_feed.snapshot(unit=unit)
                payload["price"] = snap["price"]
                payload["aggregate_perp"] = snap["aggregate_perp"]
                payload["aggregate_spot"] = snap["aggregate_spot"]
                payload["venues"] = snap["venues"]
                payload["open_interest"] = snap["open_interest"]
            await websocket.send_json(payload)
    except WebSocketDisconnect:
        pass


if __name__ == "__main__":
    import argparse
    import uvicorn

    ap = argparse.ArgumentParser(description="Run local MindPillar CVD real-time API server")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--asset", default=DEFAULT_ASSET)
    ap.add_argument("--tf", default=DEFAULT_TF)
    ap.add_argument("--bars", type=int, default=DEFAULT_BARS)
    args = ap.parse_args()

    DEFAULT_ASSET = args.asset
    DEFAULT_TF = args.tf
    DEFAULT_BARS = args.bars
    uvicorn.run(app, host=args.host, port=args.port)
