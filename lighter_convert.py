"""
Lighter trade export -> Variational schema 변환.

lighter export 의 side 를 buy/sell 로 매핑:
  Open Long / Close Short -> buy
  Open Short / Close Long -> sell

변환 후 기존 slippage_tick.py 를 그대로 사용:
  $ python lighter_convert.py <lighter-export.csv>
  $ python slippage_tick.py export-trades_lighter.csv

주의: lighter 타임스탬프는 1초 단위라 틱 매칭 오차 ±0.5초의 가격 노이즈가
개별 체결에 섞임 (집계에서는 상쇄).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "export-trades_lighter.csv"

BUY_SIDES = {"Open Long", "Close Short"}
SELL_SIDES = {"Open Short", "Close Long"}


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: python lighter_convert.py <lighter-export.csv>")
        return 1
    src = Path(sys.argv[1])
    t = pd.read_csv(src)
    t["Date"] = pd.to_datetime(t["Date"], utc=True)

    unknown = set(t["Side"].unique()) - BUY_SIDES - SELL_SIDES
    if unknown:
        print(f"[warn] unknown side values skipped: {unknown}")
        t = t[t["Side"].isin(BUY_SIDES | SELL_SIDES)]

    out = pd.DataFrame({
        "id": t["Trade ID"].astype(str),
        "created_at": t["Date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "side": t["Side"].map(lambda s: "buy" if s in BUY_SIDES else "sell"),
        "instrument_type": "perpetual_future",
        "underlying": t["Market"],
        "price": t["Price"],
        "qty": t["Size"],
        "trade_type": "trade",
        "status": "confirmed",
        "liquidation_trigger_price": "",
    })
    out.to_csv(OUT, index=False)
    print(f"wrote {len(out):,} rows -> {OUT}")
    print("markets:", out.groupby("underlying").size().to_dict())
    return 0


if __name__ == "__main__":
    sys.exit(main())
