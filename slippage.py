"""
Variational 슬리피지 분석.

처리 흐름
---------
1. export-trades.csv 를 읽어 코인별 체결 내역 수집
2. 각 코인에 대해 Binance Futures(USDT-M) 1분봉 kline 다운로드 → klines/<COIN>USDT.csv 캐시
   - Futures 에 없으면 Spot 으로 폴백
   - 둘 다 없으면 unsupported 로 표시
3. 각 체결 시각을 포함하는 1분봉 구간에서 open->next_open 선형보간으로 mark price 추정
4. 슬리피지 비용 계산
     signed_slip = (executed - mark) * side_sign     (buy=+1, sell=-1)
     slip_cost_usdc = signed_slip * qty              (양수면 손실, 음수면 이득)
5. 코인별로 raw 슬리피지 + basis-adjusted 슬리피지(코인별 중앙값 basis 제거) 집계
6. 결과 CSV 저장
     slippage_per_coin.csv  (요약)
     slippage_per_trade.csv (체결별 상세)

캐시
----
klines/ 디렉터리에 코인별 분봉이 저장됩니다. 재실행 시 캐시 사용.
새로 받으려면 해당 파일 삭제.

사용
----
$ python slippage.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
TRADES_CSV = ROOT / "export-trades.csv"
KLINE_DIR = ROOT / "klines"
KLINE_DIR.mkdir(exist_ok=True)

OUT_PER_COIN = ROOT / "slippage_per_coin.csv"
OUT_PER_TRADE = ROOT / "slippage_per_trade.csv"

# Binance API base URLs
FAPI = "https://fapi.binance.com/fapi/v1/klines"   # USDT-M perpetual
SAPI = "https://api.binance.com/api/v3/klines"     # spot

USER_AGENT = "Mozilla/5.0 slippage-analysis"
KLINE_LIMIT = 1500          # per request
INTERVAL = "1m"
INTERVAL_MS = 60_000


# ----------------------------------------------------------------------
# Binance fetch helpers
# ----------------------------------------------------------------------

def _http_get_json(url: str, params: dict, retries: int = 5) -> list:
    qs = urllib.parse.urlencode(params)
    full = f"{url}?{qs}"
    last_err = None
    for i in range(retries):
        try:
            req = urllib.request.Request(full, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2 ** i)
    raise RuntimeError(f"GET failed: {full} -> {last_err}")


def _symbol_exists(base_url: str, symbol: str) -> bool:
    """Single-bar probe to verify symbol is listed on this market."""
    try:
        out = _http_get_json(base_url, {"symbol": symbol, "interval": INTERVAL, "limit": 1}, retries=2)
        return bool(out)
    except Exception:
        return False


def fetch_klines(symbol: str, start_ms: int, end_ms: int, base_url: str) -> pd.DataFrame:
    """Paginated fetch of 1m klines.  Returns df with columns [open_time, open, high, low, close, volume]."""
    rows: list = []
    cursor = start_ms
    while cursor < end_ms:
        params = {
            "symbol": symbol,
            "interval": INTERVAL,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": KLINE_LIMIT,
        }
        batch = _http_get_json(base_url, params)
        if not batch:
            break
        rows.extend(batch)
        last_open = batch[-1][0]
        next_cursor = last_open + INTERVAL_MS
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(batch) < KLINE_LIMIT:
            break
    df = pd.DataFrame(
        rows,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "qav", "trades", "tbbav", "tbqav", "ignore",
        ],
    )
    if df.empty:
        return df
    df = df[["open_time", "open", "high", "low", "close"]].copy()
    for c in ("open", "high", "low", "close"):
        df[c] = df[c].astype(float)
    df["open_time"] = df["open_time"].astype("int64")
    df = df.drop_duplicates("open_time").sort_values("open_time").reset_index(drop=True)
    return df


def get_klines_cached(coin: str, start_ms: int, end_ms: int) -> tuple[pd.DataFrame, str]:
    """Try Futures, then Spot. Cache to disk."""
    cache_path = KLINE_DIR / f"{coin}.csv"
    if cache_path.exists():
        df = pd.read_csv(cache_path)
        market = df.attrs.get("market", "cached")
        # quick sanity check on coverage
        if not df.empty and df["open_time"].min() <= start_ms and df["open_time"].max() >= end_ms - INTERVAL_MS:
            return df, market

    sym = f"{coin}USDT"
    market = None
    base = None
    if _symbol_exists(FAPI, sym):
        base = FAPI
        market = "futures"
    elif _symbol_exists(SAPI, sym):
        base = SAPI
        market = "spot"
    else:
        return pd.DataFrame(), "unsupported"

    print(f"  fetching {sym} on {market} ({(end_ms-start_ms)/86_400_000:.1f}d) ...")
    df = fetch_klines(sym, start_ms, end_ms, base)
    if df.empty:
        return df, "unsupported"
    df.to_csv(cache_path, index=False)
    print(f"  cached {len(df):,} bars to {cache_path}")
    return df, market


# ----------------------------------------------------------------------
# Slippage computation
# ----------------------------------------------------------------------

def estimate_mark(klines: pd.DataFrame, ts_ms: pd.Series) -> pd.Series:
    """
    Linear interpolation of bar.open -> next_bar.open at the trade timestamp.
    `klines` must be sorted by open_time and have unique 1m bars.
    Returns a series of the same length as ts_ms.
    """
    if klines.empty:
        return pd.Series([float("nan")] * len(ts_ms), index=ts_ms.index)

    open_times = klines["open_time"].values
    opens = klines["open"].values
    closes = klines["close"].values

    # bar index containing each ts (largest open_time <= ts)
    idx = pd.Series(open_times).searchsorted(ts_ms.values, side="right") - 1
    idx = idx.clip(0, len(open_times) - 1)

    bar_open = opens[idx]
    bar_open_t = open_times[idx]
    next_idx = (idx + 1).clip(max=len(open_times) - 1)
    next_open = opens[next_idx]
    # When the trade is in the very last bar, fall back to that bar's close
    same = (idx == next_idx)
    next_open = pd.Series(next_open).where(~pd.Series(same), pd.Series(closes[idx])).values

    elapsed = ts_ms.values - bar_open_t
    frac = (elapsed / INTERVAL_MS).clip(0, 1)
    mark = bar_open + (next_open - bar_open) * frac
    return pd.Series(mark, index=ts_ms.index)


def main() -> int:
    if not TRADES_CSV.exists():
        print(f"missing: {TRADES_CSV}")
        return 1

    trades = pd.read_csv(TRADES_CSV)
    trades = trades[trades["status"] == "confirmed"].copy()
    trades["created_at"] = pd.to_datetime(trades["created_at"], utc=True, format="ISO8601")
    trades["ts_ms"] = (trades["created_at"].astype("int64") // 1_000_000).astype("int64")
    trades["price"] = trades["price"].astype(float)
    trades["qty"] = trades["qty"].astype(float)
    trades["side_sign"] = trades["side"].map({"buy": 1, "sell": -1})
    trades["notional"] = trades["price"] * trades["qty"]

    # Date range with 5-minute padding for safety
    pad_ms = 5 * INTERVAL_MS
    range_start = int(trades["ts_ms"].min()) - pad_ms
    range_end = int(trades["ts_ms"].max()) + pad_ms

    coins = sorted(trades["underlying"].dropna().unique())
    print(f"trades: {len(trades):,}  coins: {coins}")
    print(f"range: {datetime.fromtimestamp(range_start/1000, tz=timezone.utc)} -> "
          f"{datetime.fromtimestamp(range_end/1000, tz=timezone.utc)}")

    enriched_blocks = []
    market_per_coin: dict[str, str] = {}

    for coin in coins:
        sub = trades[trades["underlying"] == coin].copy()
        c_start = int(sub["ts_ms"].min()) - pad_ms
        c_end = int(sub["ts_ms"].max()) + pad_ms
        print(f"\n[{coin}] trades={len(sub):,}")
        kl, market = get_klines_cached(coin, c_start, c_end)
        market_per_coin[coin] = market
        if kl.empty:
            sub["mark"] = float("nan")
        else:
            sub["mark"] = estimate_mark(kl, sub["ts_ms"]).values
        enriched_blocks.append(sub)

    enriched = pd.concat(enriched_blocks, ignore_index=True)

    # Per-fill computations
    enriched["basis"] = enriched["mark"] - enriched["price"]            # >0 means binance higher
    enriched["basis_pct"] = enriched["basis"] / enriched["mark"]
    enriched["raw_slip_per_unit"] = (enriched["price"] - enriched["mark"]) * enriched["side_sign"]
    enriched["raw_slip_usdc"] = enriched["raw_slip_per_unit"] * enriched["qty"]

    # Basis-adjusted: shift Binance mark down by the per-coin median basis,
    # so structural premium/discount cancels out.
    median_basis = enriched.groupby("underlying")["basis"].transform("median")
    enriched["adj_mark"] = enriched["mark"] - median_basis
    enriched["adj_slip_per_unit"] = (enriched["price"] - enriched["adj_mark"]) * enriched["side_sign"]
    enriched["adj_slip_usdc"] = enriched["adj_slip_per_unit"] * enriched["qty"]

    # Per-coin summary
    grp = enriched.groupby("underlying")
    summary = pd.DataFrame({
        "n_trades": grp.size(),
        "n_buy": grp.apply(lambda d: (d["side"] == "buy").sum()),
        "n_sell": grp.apply(lambda d: (d["side"] == "sell").sum()),
        "total_notional_usdc": grp["notional"].sum(),
        "median_basis": grp["basis"].median(),
        "median_basis_pct": grp["basis_pct"].median(),
        "raw_slip_usdc": grp["raw_slip_usdc"].sum(),
        "adj_slip_usdc": grp["adj_slip_usdc"].sum(),
    })
    summary["raw_slip_pct"] = summary["raw_slip_usdc"] / summary["total_notional_usdc"]
    summary["adj_slip_pct"] = summary["adj_slip_usdc"] / summary["total_notional_usdc"]
    summary["market"] = summary.index.map(market_per_coin)
    summary = summary.sort_values("total_notional_usdc", ascending=False)

    # Write outputs
    cols_out = [
        "id", "created_at", "underlying", "side", "qty", "price", "mark",
        "basis", "basis_pct", "notional",
        "raw_slip_per_unit", "raw_slip_usdc",
        "adj_mark", "adj_slip_per_unit", "adj_slip_usdc",
    ]
    enriched[cols_out].to_csv(OUT_PER_TRADE, index=False)
    summary.to_csv(OUT_PER_COIN)

    # Console report
    print("\n=== Per-coin summary ===")
    show = summary.copy()
    show["raw_slip_pct"] = (show["raw_slip_pct"] * 100).round(4).astype(str) + "%"
    show["adj_slip_pct"] = (show["adj_slip_pct"] * 100).round(4).astype(str) + "%"
    show["median_basis_pct"] = (show["median_basis_pct"] * 100).round(4).astype(str) + "%"
    show["total_notional_usdc"] = show["total_notional_usdc"].round(2)
    show["raw_slip_usdc"] = show["raw_slip_usdc"].round(2)
    show["adj_slip_usdc"] = show["adj_slip_usdc"].round(2)
    print(show.to_string())
    print(f"\nwrote: {OUT_PER_COIN}")
    print(f"wrote: {OUT_PER_TRADE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
