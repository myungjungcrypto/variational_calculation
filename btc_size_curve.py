"""
BTC 사이즈별 슬리피지 커브.

slippage_tick_per_trade<suffix>.csv 를 읽어 (이미 계산된 per-fill 결과 재사용,
새 다운로드 없음) BTC 체결을 notional 구간별로 묶고, 구간마다:

  buy_bps   - 매수 가중 signed bps (음수 = Binance mid 보다 싸게 삼)
  sell_bps  - 매도 가중 signed bps (양수 = Binance mid 보다 싸게 팜)
  mid_bps   - (buy_bps + sell_bps) / 2
              = 구조적 베이시스를 제거한 '진짜 라운드트립 편도 비용'
  raw_bps   - 방향 합산 가중 bps (참고용)

를 출력한다. mid_bps 가 사이즈에 따라 어떻게 변하는지가 핵심.

사용
----
$ python btc_size_curve.py                      # slippage_tick_per_trade.csv
$ python btc_size_curve.py _btc_260803          # slippage_tick_per_trade_btc_260803.csv
$ python btc_size_curve.py _btc_260803 ETH      # 다른 코인
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent

# notional bucket edges in USD
EDGES = [0, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, float("inf")]
LABELS = ["<5K", "5-10K", "10-25K", "25-50K", "50-100K", "100-250K", ">250K"]


def wavg(bps: pd.Series, w: pd.Series) -> float:
    if w.sum() == 0:
        return float("nan")
    return float((bps * w).sum() / w.sum())


def main() -> int:
    suffix = sys.argv[1] if len(sys.argv) > 1 else ""
    coin = sys.argv[2] if len(sys.argv) > 2 else "BTC"
    src = ROOT / f"slippage_tick_per_trade{suffix}.csv"
    if not src.exists():
        print(f"missing: {src}  (run slippage_tick.py first)")
        return 1

    df = pd.read_csv(src)
    df = df[(df["underlying"] == coin) & df["matched"] & df["slip_bps"].notna()].copy()
    if df.empty:
        print(f"no matched {coin} fills in {src.name}")
        return 1

    df["bucket"] = pd.cut(df["notional"], bins=EDGES, labels=LABELS, right=False)

    print(f"file: {src.name}  coin: {coin}  fills: {len(df):,}  "
          f"notional: ${df['notional'].sum():,.0f}")
    print(f"period: {df['created_at'].min()[:10]} .. {df['created_at'].max()[:10]}")

    rows = []
    for label in LABELS:
        b = df[df["bucket"] == label]
        if b.empty:
            continue
        buys = b[b["side"] == "buy"]
        sells = b[b["side"] == "sell"]
        buy_bps = wavg(buys["slip_bps"], buys["notional"]) if len(buys) else float("nan")
        sell_bps = wavg(sells["slip_bps"], sells["notional"]) if len(sells) else float("nan")
        mid = (buy_bps + sell_bps) / 2 if not (np.isnan(buy_bps) or np.isnan(sell_bps)) else float("nan")
        rows.append({
            "bucket": label,
            "n_fills": len(b),
            "n_buy": len(buys),
            "n_sell": len(sells),
            "notional_usd": round(float(b["notional"].sum()), 0),
            "avg_clip_usd": round(float(b["notional"].mean()), 0),
            "buy_bps": round(buy_bps, 3),
            "sell_bps": round(sell_bps, 3),
            "mid_bps": round(mid, 3) if not np.isnan(mid) else float("nan"),
            "raw_bps": round(wavg(b["slip_bps"], b["notional"]), 3),
        })
    out = pd.DataFrame(rows)
    print("\n=== size buckets (mid_bps = true one-way cost, basis removed) ===")
    print(out.to_string(index=False))

    buys = df[df["side"] == "buy"]
    sells = df[df["side"] == "sell"]
    ob = wavg(buys["slip_bps"], buys["notional"])
    os_ = wavg(sells["slip_bps"], sells["notional"])
    print(f"\nOVERALL {coin}: buy {ob:+.3f} bps | sell {os_:+.3f} bps | "
          f"midpoint {((ob+os_)/2):+.3f} bps | raw {wavg(df['slip_bps'], df['notional']):+.3f} bps")
    return 0


if __name__ == "__main__":
    sys.exit(main())
