"""
Lighter SPY/QQQ 체결 vs 실제 미국 ETF 가격(yfinance 1분봉) 슬리피지.

SPY/QQQ perp 는 Binance 아카이브가 없으므로, 실제 ETF 의 1분봉을
reference 로 사용한다 (정규장 + 프리/애프터 커버, 그 외 시간 체결은 제외).

  reference = 체결 분의 1m open -> 다음 분 open 선형보간
  slip_bps  = sign * (fill - reference) / reference * 10000  (buy=+1, sell=-1)

사용
----
$ pip install yfinance pandas
$ python lighter_spy_ref.py <lighter-export.csv>
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

TICKERS = {"SPY": "SPY", "QQQ": "QQQ"}
BUY_SIDES = {"Open Long", "Close Short"}


def load_ref(ticker: str, start, end) -> pd.Series:
    import yfinance as yf
    # yfinance 1m: max ~7-8 days per request -> chunk
    frames = []
    cur = start
    while cur < end:
        nxt = min(cur + pd.Timedelta(days=6), end)
        h = yf.download(ticker, start=cur.strftime("%Y-%m-%d"),
                        end=(nxt + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                        interval="1m", prepost=True, progress=False,
                        auto_adjust=False)
        if len(h):
            frames.append(h["Open"])
        cur = nxt
    if not frames:
        return pd.Series(dtype=float)
    s = pd.concat(frames)
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    s = s[~s.index.duplicated()].sort_index()
    s.index = s.index.tz_convert("UTC")
    return s


def interp_ref(refs: pd.Series, ts: pd.Series) -> np.ndarray:
    """open->next open linear interpolation at each fill timestamp.
    NaN where the fill's minute (or the next bar) is missing / >5min gap.
    Both time axes are forced to int64 nanoseconds (UTC) so mixed datetime
    resolutions (pandas 2/3, ns vs us) can't silently break the search."""
    idx_times = refs.index.tz_convert("UTC").to_numpy(dtype="datetime64[ns]").astype("int64")
    vals = refs.to_numpy(dtype=float)
    out = np.full(len(ts), np.nan)
    t_ns = ts.dt.tz_convert("UTC").to_numpy(dtype="datetime64[ns]").astype("int64")
    pos = np.searchsorted(idx_times, t_ns, side="right") - 1
    for i, p in enumerate(pos):
        if p < 0 or p + 1 >= len(idx_times):
            continue
        t0, t1 = idx_times[p], idx_times[p + 1]
        if t1 - t0 > 5 * 60 * 1_000_000_000:   # gap (market closed)
            continue
        if t_ns[i] - t0 > 2 * 60 * 1_000_000_000:
            continue
        frac = (t_ns[i] - t0) / (t1 - t0)
        out[i] = vals[p] + (vals[p + 1] - vals[p]) * frac
    return out


def wavg(b, w):
    w = np.asarray(w, dtype=float); b = np.asarray(b, dtype=float)
    m = ~np.isnan(b)
    if w[m].sum() == 0:
        return float("nan")
    return float((b[m] * w[m]).sum() / w[m].sum())


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: python lighter_spy_ref.py <lighter-export.csv>")
        return 1
    t = pd.read_csv(Path(sys.argv[1]))
    t["Date"] = pd.to_datetime(t["Date"], utc=True)
    t = t[t["Market"].isin(TICKERS)].copy()
    t["sign"] = np.where(t["Side"].isin(BUY_SIDES), 1, -1)
    print(f"stock-perp fills: {len(t):,}  range {t['Date'].min()} .. {t['Date'].max()}")

    rows = []
    detail = []
    for mkt, tk in TICKERS.items():
        sub = t[t["Market"] == mkt].copy()
        if sub.empty:
            continue
        refs = load_ref(tk, sub["Date"].min().normalize(),
                        sub["Date"].max().normalize() + pd.Timedelta(days=1))
        print(f"{mkt}: {len(refs):,} 1m bars loaded")
        sub["ref"] = interp_ref(refs, sub["Date"])
        sub["slip_bps"] = sub["sign"] * (sub["Price"] - sub["ref"]) / sub["ref"] * 1e4
        sub["slip_usd"] = sub["slip_bps"] / 1e4 * sub["Trade Value"]
        cov = sub["ref"].notna().mean() * 100
        m = sub[sub["ref"].notna()]
        buys = m[m["sign"] == 1]; sells = m[m["sign"] == -1]
        b = wavg(buys["slip_bps"], buys["Trade Value"])
        s = wavg(sells["slip_bps"], sells["Trade Value"])
        rows.append({
            "market": mkt, "fills": len(sub), "matched_pct": round(cov, 1),
            "notional_usd": round(float(m["Trade Value"].sum()), 0),
            "buy_bps": round(b, 3) if not np.isnan(b) else None,
            "sell_bps": round(s, 3) if not np.isnan(s) else None,
            "weighted_bps": round(wavg(m["slip_bps"], m["Trade Value"]), 3),
            "net_usd": round(float(m["slip_usd"].sum()), 2),
        })
        detail.append(sub)

    print("\n=== SPY/QQQ vs real ETF 1m reference ===")
    print(pd.DataFrame(rows).to_string(index=False))
    out = Path(__file__).resolve().parent / "lighter_stock_slippage.csv"
    pd.concat(detail).to_csv(out, index=False)
    print(f"\nwrote: {out}")
    print("note: 1m-bar reference => 개별 체결에는 분봉 노이즈가 섞임. "
          "부호 있는 가중평균(집계)만 신뢰할 것.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
