"""
selftest_live.py - End-to-end live read-only self test for mp.py and server.py.

Verifies:
  1. All public REST endpoints (config, stats, ws_ticket, historical candles,
     CVD snapshot + Aggregate CVD + Spot share, auto-divergences, Open Interest,
     Footprint clusters, Volume Profile Zones, Price Levels & Order Blocks,
     Range/Session Volume Profiles, Liquidation Tensor summary).
  2. Real-time WebSocket streaming via MindPillarLiveFeed (receives live candle.tick,
     oi.tick / footprint.tick updates and updates CVD in real time).
  3. Local FastAPI REST + WebSocket API server (`server.py`).

Usage:
    python selftest_live.py
"""

from __future__ import annotations

import asyncio
import sys
import time

import mp


def _fmt(v):
    return mp._fmt_compact(v)


def test_rest() -> None:
    print("=== [1/3] TESTING REST CLIENT (mp.MindPillarClient) ===")
    c = mp.MindPillarClient()

    cfg = c.config()
    assert cfg["schemaVersion"] == 2 and "15m" in cfg["candleWindow"]["maxBars"]
    print(f"  config: ok ({len(cfg['candleWindow']['maxBars'])} timeframes, maxBars={cfg['candleWindow']['maxBars']['15m']})")

    st = c.stats()
    assert st["feedsReporting"] >= 1
    print(f"  stats: ok (feeds={st['feedsReporting']}/{st['structure']['feeds']}, 24h_notional=${_fmt(st['notional24h']['value'])})")

    ticket = c.ws_ticket()
    assert len(ticket) >= 16
    print(f"  ws_ticket: ok ({ticket[:8]}...)")

    snap = c.cvd_snapshot(
        asset="btc",
        tf="15m",
        bars=1000,
        include_spot=True,
        include_series=False,
        include_divergences=True,
    )
    px = snap["price"]
    assert px and px["close"] > 0
    assert len(snap["venues"]) == 6  # 4 perp + 2 spot
    agg = snap["aggregate_perp"]
    sp = snap["aggregate_spot"]
    print(f"  price: {px['ticker']} O={px['open']:,.2f} H={px['high']:,.2f} L={px['low']:,.2f} C={px['close']:,.2f}")
    print(f"  aggregate_perp: CVD_inf={_fmt(agg['cvd_all'])}  CVD_1D={_fmt(agg['cvd_daily'])}  CVD_24H={_fmt(agg['cvd_24h'])}  Buy={_fmt(agg['buy_usd'])}  Sell={_fmt(agg['sell_usd'])}")
    print(f"  aggregate_spot: CVD_inf={_fmt(sp['cvd'])}  spot_share={sp['spot_share_pct']}%")
    for vid, v in snap["venues"].items():
        print(f"    {v['label']:16s} bars={v['bars']:4d}  CVD_inf={_fmt(v['cvd_all']):>10s}  Buy={_fmt(v['buy_usd']):>8s}  Sell={_fmt(v['sell_usd']):>8s}")

    divs = snap["divergences"]["divergences"]
    print(f"  divergences (fractal): {len(divs)} detected" + (f" (latest: {divs[0]['kind']} {divs[0]['status']} {divs[0]['projection']['divergingCount']}/{divs[0]['projection']['total']} rr={divs[0]['rr']})" if divs else ""))

    oi = c.open_interest("binance-btc", tf="15m", bars=20)
    assert len(oi) > 0 and oi[-1]["c"] > 0
    print(f"  open_interest: {len(oi)} bars, latest={oi[-1]['c']:,.2f} BTC (${_fmt(oi[-1]['c'] * px['close'])})")

    fp = c.footprint("binance-btc", tf="15m", bars=5)
    assert len(fp["bars"]) > 0 and len(fp["bars"][-1]["lp"]) > 0
    last_fp = fp["bars"][-1]
    print(f"  footprint: step={fp['step']} bars={len(fp['bars'])} latest_poc={last_fp.get('poc_price')} levels={len(last_fp['lp'])} trades={last_fp['n']}")

    z = c.zones("binance-btc", period="day")
    assert z.get("dev") and z["dev"]["poc"] > 0
    print(f"  zones (day): prev_poc={z['prev']['poc']} dev_poc={z['dev']['poc']} dev_vah={z['dev']['vah']} dev_val={z['dev']['val']}")

    npoc = c.levels("binance-btc", family="npoc", only_untapped=True)
    ob = c.levels("binance-btc", family="ob", only_untapped=True)
    print(f"  levels: untapped_nPOC={len(npoc['levels'])}, live_OB={len(ob['levels'])}")

    prof = c.range_profile("binance-btc", lookback_hours=24)
    assert prof["poc"] > 0 and len(prof["rows"]) > 0
    print(f"  range_profile (24h): poc={prof['poc']} vah={prof['vah']} val={prof['val']} rows={len(prof['rows'])}")

    lt = c.liq_tensor("BTCUSDT", interval="1d", summary_only=True)
    assert lt["spot_price"] > 0 and lt["cell_count"] > 0
    print(f"  liq_tensor (1d): spot={lt['spot_price']} vwap_4h={lt['vwap_4h']:.2f} above=${_fmt(lt['notional_above_usd'])} below=${_fmt(lt['notional_below_usd'])} cells={lt['cell_count']} heatmap_cells={lt['heatmap_cell_count']}")


async def test_live_stream() -> None:
    print("\n=== [2/3] TESTING REAL-TIME WEBSOCKET ENGINE (mp.MindPillarLiveFeed) ===")
    feed = mp.MindPillarLiveFeed(
        asset="btc",
        tf="1m",
        bars=200,
        include_spot=True,
        include_oi=True,
        include_footprint=True,
        include_liquidations=True,
        include_liq_tensor=False,
        include_zones=True,
    )
    async with feed:
        seen: dict[str, int] = {}
        t0 = time.time()
        async for ev in feed.stream():
            et = ev.get("type", "unknown")
            seen[et] = seen.get(et, 0) + 1
            if sum(seen.values()) >= 8 or (time.time() - t0) >= 8.0:
                break
        snap = feed.snapshot()
        assert snap["connected"] is True
        assert seen.get("candle.tick", 0) + seen.get("candle.closed", 0) >= 1, f"No candle ticks received: {seen}"
        print(f"  live events received in {time.time() - t0:.2f}s: {seen}")
        print(f"  live snapshot: BTC={snap['price']['close']:,.2f}  AggCVD={_fmt(snap['aggregate_perp']['cvd_all'])}  OI={(snap['open_interest'] or {}).get('c')}")


def test_fastapi_server() -> None:
    print("\n=== [3/3] TESTING LOCAL HTTP + WS API SERVER (server.py) ===")
    from fastapi.testclient import TestClient
    import server

    server.DEFAULT_TF = "1m"
    server.DEFAULT_BARS = 100
    with TestClient(server.app) as tc:
        r = tc.get("/health")
        assert r.status_code == 200 and r.json()["ok"] is True
        print("  GET /health:", r.json())

        # Give background WS ~2s to connect and receive live ticks
        time.sleep(2.5)
        r2 = tc.get("/v1/live")
        assert r2.status_code == 200
        live = r2.json()
        assert live["price"]["close"] > 0
        print(f"  GET /v1/live: connected={live['connected']} ticks={live['tick_count']} BTC={live['price']['close']:,.2f} AggCVD={_fmt(live['aggregate_perp']['cvd_all'])}")

        r3 = tc.get("/v1/zones?period=day")
        assert r3.status_code == 200 and r3.json()["dev"]["poc"] > 0
        print(f"  GET /v1/zones: dev_poc={r3.json()['dev']['poc']}")

        r4 = tc.get("/v1/divergences?tf=15m&bars=300")
        assert r4.status_code == 200 and "divergences" in r4.json()
        print(f"  GET /v1/divergences: count={r4.json()['count']}")

        with tc.websocket_connect("/ws/live?include_snapshot=true") as ws:
            first = ws.receive_json()
            assert first["type"] == "snapshot" and first["snapshot"]["price"]["close"] > 0
            nxt = ws.receive_json()
            assert "event" in nxt and "aggregate_perp" in nxt
            print(f"  WS /ws/live: initial snapshot ok + live event type={nxt['event']['type']}")


def main() -> int:
    test_rest()
    asyncio.run(test_live_stream())
    test_fastapi_server()
    print("\nALL LIVE SELF-TESTS PASSED!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
