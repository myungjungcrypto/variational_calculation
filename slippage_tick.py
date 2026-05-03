"""
Variational 틱-레벨 슬리피지 분석.

methodology mirrors the audit in Variationalca.xlsx (Tick Methodology sheet):
  1. Master price feed = Binance USDT-Perp aggTrades archive on data.binance.vision
     (every aggregated trade, ms-resolution transact_time)
  2. For each user fill, binary-search the nearest aggTrade by transact_time.
     If gap > MAX_GAP_MS, mark as unmatched.
  3. Signed slippage in bps:
       sign      = +1 for buy, -1 for sell
       slip_bps  = sign * (fill_price - reference) / reference * 10_000
       slip_usd  = slip_bps / 10_000 * notional
     Positive = adverse to user (paid above mark on buy, received below on sell)
  4. Aggregate weighted bps + buy/sell asymmetry + size buckets.

Why this is more accurate than the 1m-kline approach in slippage.py
-------------------------------------------------------------------
1m kline interpolation has ~5-10 bps of noise for BTC.  Symmetric noise cancels
in the *signed* aggregate but inflates *absolute* deviations.  Tick matching
removes the noise, exposing the small signed cost (= what the user actually paid).

Cache
-----
data.binance.vision archives are downloaded into ticks/<SYMBOL>/<SYMBOL>-aggTrades-YYYY-MM-DD.zip
and re-used on subsequent runs.

Outputs
-------
slippage_tick_per_coin.csv     - per-coin summary
slippage_tick_per_trade.csv    - per-fill detail
slippage_tick_asymmetry.csv    - buy vs sell breakdown per coin
slippage_tick_size_buckets.csv - notional bucket breakdown for sells

Run
---
$ python slippage_tick.py
"""

from __future__ import annotations

import io
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
TRADES_CSV = ROOT / "export-trades.csv"
TICK_DIR = ROOT / "ticks"
TICK_DIR.mkdir(exist_ok=True)

OUT_PER_COIN = ROOT / "slippage_tick_per_coin.csv"
OUT_PER_TRADE = ROOT / "slippage_tick_per_trade.csv"
OUT_ASYM = ROOT / "slippage_tick_asymmetry.csv"
OUT_SIZE = ROOT / "slippage_tick_size_buckets.csv"

ARCHIVE_BASE = "https://data.binance.vision/data/futures/um/daily/aggTrades"
USER_AGENT = "Mozilla/5.0 slippage-tick-analysis"
MAX_GAP_MS = 1_000      # mark fills as unmatched if no tick within 1s
BIG_SELL_USDC = 200_000 # bucket threshold for "big sells"


# ---------------------------------------------------------------------------
# Download / cache helpers
# ---------------------------------------------------------------------------

def _download(url: str, dest: Path, retries: int = 5) -> bool:
    """Download a file, returning True on success and False on 404."""
    last_err = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=60) as r:
                data = r.read()
            dest.write_bytes(data)
            return True
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
            last_err = e
            time.sleep(2 ** i)
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2 ** i)
    raise RuntimeError(f"download failed: {url} -> {last_err}")


def fetch_day(symbol: str, day: datetime) -> pd.DataFrame | None:
    """Return tick DataFrame [transact_time, price] for a single (symbol, day).
    Returns None if the daily archive doesn't exist."""
    tag = day.strftime("%Y-%m-%d")
    sym_dir = TICK_DIR / symbol
    sym_dir.mkdir(exist_ok=True)
    cache = sym_dir / f"{symbol}-aggTrades-{tag}.parquet"
    if cache.exists():
        return pd.read_parquet(cache)

    url = f"{ARCHIVE_BASE}/{symbol}/{symbol}-aggTrades-{tag}.zip"
    zip_path = sym_dir / f"{symbol}-aggTrades-{tag}.zip"
    ok = _download(url, zip_path)
    if not ok:
        return None
    try:
        with zipfile.ZipFile(zip_path) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as fh:
                df = pd.read_csv(
                    fh,
                    header=None,
                    names=["agg_id", "price", "qty", "first_id", "last_id",
                           "transact_time", "is_buyer_maker"],
                    usecols=["price", "transact_time"],
                    dtype={"price": "float64", "transact_time": "int64"},
                )
    finally:
        zip_path.unlink(missing_ok=True)
    df = df.sort_values("transact_time").reset_index(drop=True)
    df.to_parquet(cache, index=False)
    return df


def load_ticks(symbol: str, day_start: datetime, day_end: datetime) -> pd.DataFrame:
    """Concatenate daily tick archives covering [day_start, day_end]."""
    parts = []
    cur = datetime(day_start.year, day_start.month, day_start.day, tzinfo=timezone.utc)
    end = datetime(day_end.year, day_end.month, day_end.day, tzinfo=timezone.utc)
    while cur <= end:
        df = fetch_day(symbol, cur)
        if df is not None and not df.empty:
            parts.append(df)
        cur += timedelta(days=1)
    if not parts:
        return pd.DataFrame(columns=["transact_time", "price"])
    out = pd.concat(parts, ignore_index=True)
    return out.sort_values("transact_time").reset_index(drop=True)


def symbol_for(coin: str) -> str:
    return f"{coin}USDT"


# ---------------------------------------------------------------------------
# Tick matching + slippage
# ---------------------------------------------------------------------------

def match_nearest(fill_ts: np.ndarray, tick_ts: np.ndarray, tick_px: np.ndarray):
    """Binary-search nearest tick for each fill timestamp.
    Returns (ref_price, gap_ms).  ref_price is NaN where gap > MAX_GAP_MS."""
    if len(tick_ts) == 0:
        return np.full(len(fill_ts), np.nan), np.full(len(fill_ts), np.nan)
    idx = np.searchsorted(tick_ts, fill_ts)
    left = np.clip(idx - 1, 0, len(tick_ts) - 1)
    right = np.clip(idx, 0, len(tick_ts) - 1)
    d_left = np.abs(fill_ts - tick_ts[left])
    d_right = np.abs(tick_ts[right] - fill_ts)
    use_right = d_right < d_left
    nearest = np.where(use_right, right, left)
    gap = np.minimum(d_left, d_right)
    ref = tick_px[nearest]
    ref = np.where(gap <= MAX_GAP_MS, ref, np.nan)
    return ref, gap


def compute(trades: pd.DataFrame) -> pd.DataFrame:
    """Pull ticks per coin and attach reference price + slippage."""
    enriched_blocks = []
    for coin, sub in trades.groupby("underlying"):
        sym = symbol_for(coin)
        first = sub["created_at"].min()
        last = sub["created_at"].max()
        print(f"[{coin}] fills={len(sub):,}  range={first.date()}..{last.date()}")
        ticks = load_ticks(sym, first, last)
        if ticks.empty:
            print(f"  no archive for {sym} - skipping")
            sub = sub.copy()
            sub["reference"] = np.nan
            sub["gap_ms"] = np.nan
            sub["matched"] = False
            enriched_blocks.append(sub)
            continue
        ref, gap = match_nearest(
            sub["ts_ms"].to_numpy(),
            ticks["transact_time"].to_numpy(),
            ticks["price"].to_numpy(),
        )
        sub = sub.copy()
        sub["reference"] = ref
        sub["gap_ms"] = gap
        sub["matched"] = ~np.isnan(ref)
        matched_pct = 100.0 * sub["matched"].mean()
        print(f"  matched {sub['matched'].sum():,}/{len(sub):,} ({matched_pct:.1f}%)")
        enriched_blocks.append(sub)
    return pd.concat(enriched_blocks, ignore_index=True)


def add_slippage(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["sign"] = np.where(df["side"] == "buy", 1, -1)
    df["slip_bps"] = df["sign"] * (df["price"] - df["reference"]) / df["reference"] * 10_000
    df["slip_usd"] = df["slip_bps"] / 10_000 * df["notional"]
    return df


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------

def weighted_bps(s_bps: pd.Series, weight: pd.Series) -> float:
    w = weight.where(s_bps.notna(), 0.0)
    if w.sum() == 0:
        return float("nan")
    return float((s_bps.fillna(0.0) * w).sum() / w.sum())


def per_coin_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for coin, sub in df.groupby("underlying"):
        m = sub[sub["matched"]]
        if len(m) == 0:
            rows.append({"underlying": coin, "n_fills": len(sub), "matched": 0,
                         "notional_usd": float(sub["notional"].sum()),
                         "weighted_bps": float("nan"), "net_usd": float("nan")})
            continue
        rows.append({
            "underlying": coin,
            "n_fills": len(sub),
            "matched": int(m["matched"].sum()),
            "notional_usd": float(m["notional"].sum()),
            "weighted_bps": weighted_bps(m["slip_bps"], m["notional"]),
            "net_usd": float(m["slip_usd"].sum()),
            "buy_bps": weighted_bps(m.loc[m["side"] == "buy", "slip_bps"],
                                    m.loc[m["side"] == "buy", "notional"]),
            "sell_bps": weighted_bps(m.loc[m["side"] == "sell", "slip_bps"],
                                     m.loc[m["side"] == "sell", "notional"]),
            "sells_adv_pct": 100.0 * (m.loc[m["side"] == "sell", "slip_bps"] > 0).mean()
                              if (m["side"] == "sell").any() else float("nan"),
        })
    out = pd.DataFrame(rows).sort_values("notional_usd", ascending=False)
    return out


def asymmetry_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for coin, sub in df.groupby("underlying"):
        m = sub[sub["matched"]]
        for side in ("buy", "sell"):
            s = m[m["side"] == side]
            if len(s) == 0:
                continue
            rows.append({
                "underlying": coin,
                "side": side,
                "n_fills": len(s),
                "notional_usd": float(s["notional"].sum()),
                "weighted_bps": weighted_bps(s["slip_bps"], s["notional"]),
                "net_usd": float(s["slip_usd"].sum()),
                "adverse_pct": 100.0 * (s["slip_bps"] > 0).mean(),
            })
    return pd.DataFrame(rows)


def size_buckets(df: pd.DataFrame) -> pd.DataFrame:
    """Per-coin buy/sell breakdown into <BIG, >=BIG buckets."""
    rows = []
    for coin, sub in df.groupby("underlying"):
        m = sub[sub["matched"]]
        for side in ("buy", "sell"):
            s = m[m["side"] == side]
            if len(s) == 0:
                continue
            for label, mask in (
                ("small", s["notional"] < BIG_SELL_USDC),
                ("big",   s["notional"] >= BIG_SELL_USDC),
            ):
                bucket = s[mask]
                if len(bucket) == 0:
                    continue
                rows.append({
                    "underlying": coin,
                    "side": side,
                    "bucket": label,
                    "n_fills": len(bucket),
                    "notional_usd": float(bucket["notional"].sum()),
                    "weighted_bps": weighted_bps(bucket["slip_bps"], bucket["notional"]),
                    "net_usd": float(bucket["slip_usd"].sum()),
                    "adverse_pct": 100.0 * (bucket["slip_bps"] > 0).mean(),
                })
    return pd.DataFrame(rows)


def overall_line(df: pd.DataFrame) -> str:
    m = df[df["matched"]]
    bps = weighted_bps(m["slip_bps"], m["notional"])
    return (f"OVERALL  fills={len(m):,}  notional=${m['notional'].sum():,.0f}  "
            f"weighted={bps:+.2f} bps  net=${m['slip_usd'].sum():,.0f}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

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
    trades["notional"] = trades["price"] * trades["qty"]

    # focus on coins likely on Binance Futures
    skip = {"LIGHTER", "OVERTAKE", "MON"}  # not on Binance perps
    trades = trades[~trades["underlying"].isin(skip)].reset_index(drop=True)
    print(f"trades: {len(trades):,}  coins: {sorted(trades['underlying'].unique())}")

    enriched = compute(trades)
    enriched = add_slippage(enriched)

    per_coin = per_coin_summary(enriched)
    asym = asymmetry_table(enriched)
    sizes = size_buckets(enriched)

    enriched.to_csv(OUT_PER_TRADE, index=False)
    per_coin.to_csv(OUT_PER_COIN, index=False)
    asym.to_csv(OUT_ASYM, index=False)
    sizes.to_csv(OUT_SIZE, index=False)

    print("\n=== Per coin ===")
    show = per_coin.copy()
    for c in ("notional_usd", "net_usd"):
        if c in show:
            show[c] = show[c].round(2)
    for c in ("weighted_bps", "buy_bps", "sell_bps", "sells_adv_pct"):
        if c in show:
            show[c] = show[c].round(3)
    print(show.to_string(index=False))

    print("\n=== Asymmetry (buy vs sell) ===")
    a = asym.copy()
    a["notional_usd"] = a["notional_usd"].round(2)
    a["net_usd"] = a["net_usd"].round(2)
    for c in ("weighted_bps", "adverse_pct"):
        a[c] = a[c].round(3)
    print(a.to_string(index=False))

    print("\n=== Size buckets ===")
    b = sizes.copy()
    b["notional_usd"] = b["notional_usd"].round(2)
    b["net_usd"] = b["net_usd"].round(2)
    for c in ("weighted_bps", "adverse_pct"):
        b[c] = b[c].round(3)
    print(b.to_string(index=False))

    print()
    print(overall_line(enriched))
    print(f"\nwrote: {OUT_PER_COIN}\nwrote: {OUT_PER_TRADE}\nwrote: {OUT_ASYM}\nwrote: {OUT_SIZE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
