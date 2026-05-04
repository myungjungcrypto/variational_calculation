"""
체결별 손익(P&L) + 슬리피지 결합 분석.

각 체결마다 다음을 계산:
  position_before / position_after  - 누적 포지션 (signed)
  opening_qty / closing_qty         - 이 체결에서 포지션을 늘린 수량 / 줄인 수량
  avg_entry_price                   - 닫는 부분의 평균 진입가 (FIFO)
  computed_pnl_usdc                 - FIFO 로 우리가 계산한 P&L (close 부분에서만 발생)
  platform_pnl_usdc                 - export-pnl.csv 의 realized_pnl 매칭 (시간/코인 기준)
  platform_pnl_gap_ms               - 매칭된 PnL 기록과의 시간차

슬리피지 결과(slippage_tick_per_trade.csv)가 있으면 자동으로 join해서
각 체결의 슬리피지 코스트와 P&L 을 한 줄에 같이 보여줌.

P&L 모델
---------
기본 가정: 각 코인 포지션은 시간순 FIFO 로 관리됨.
  - 포지션 0 또는 같은 방향으로 사이즈 키우면 -> 'opening' (queue 에 push)
  - 반대 방향으로 부분 정리 -> 'closing' (queue 에서 pop, P&L 발생)
  - 영을 가로지르면 (long -> short 등) -> 일부 close, 일부 open

플랫폼 PnL 매칭
---------------
'closing' 체결과 export-pnl.csv 의 realized_pnl 레코드를 시간 + 코인으로 매칭.
같은 시각에 여러 체결이 있을 때는 시간순으로 1:1 페어링.

출력
----
trade_pnl.csv          - 체결별 P&L + 슬리피지
trade_pnl_summary.csv  - 코인별 누적 P&L, 누적 슬리피지, win rate

사용
----
$ python trade_pnl.py
"""

from __future__ import annotations

import sys
from collections import deque
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
TRADES_CSV = ROOT / "export-trades.csv"
PNL_CSV = ROOT / "export-pnl.csv"

PNL_MATCH_WINDOW_MS = 60_000  # match closing fills to realized_pnl within 60s


def _suffix_from(path: Path) -> str:
    stem = path.stem
    if stem.startswith("export-trades"):
        return stem[len("export-trades"):]
    return f"_{stem}"


def fifo_pnl(trades: pd.DataFrame) -> pd.DataFrame:
    """Walk trades in time order per coin, classify open/close, compute FIFO P&L."""
    cols = [
        "position_before", "position_after",
        "opening_qty", "closing_qty",
        "avg_entry_price", "computed_pnl_usdc",
    ]
    out = {c: np.full(len(trades), np.nan, dtype="float64") for c in cols}

    for coin, sub in trades.groupby("underlying"):
        idxs = sub.sort_values("ts_ms").index.to_list()
        # signed inventory queue: deque of [signed_qty_remaining, price]
        # signed positive = long, negative = short
        lots: deque[list[float]] = deque()
        position = 0.0
        for i in idxs:
            qty = float(trades.at[i, "qty"])
            price = float(trades.at[i, "price"])
            side_sign = 1 if trades.at[i, "side"] == "buy" else -1
            signed_qty = qty * side_sign
            prev_pos = position
            new_pos = prev_pos + signed_qty

            opening = 0.0
            closing = 0.0
            entry_sum = 0.0  # sum of entry_price * matched_qty (for avg)
            pnl = 0.0
            remaining = qty

            # If signed_qty has opposite sign to existing position, we're closing some
            if prev_pos != 0 and (prev_pos > 0) != (signed_qty > 0):
                close_capacity = abs(prev_pos)
                close_amt = min(remaining, close_capacity)
                # pop FIFO until close_amt is consumed
                left = close_amt
                while left > 1e-12 and lots:
                    lot_qty, lot_px = lots[0]
                    lot_abs = abs(lot_qty)
                    take = min(left, lot_abs)
                    # P&L: long lot exited -> (current_price - entry_price) * take
                    # short lot exited -> (entry_price - current_price) * take
                    if lot_qty > 0:  # long lot, this fill is sell
                        pnl += (price - lot_px) * take
                    else:  # short lot, this fill is buy
                        pnl += (lot_px - price) * take
                    entry_sum += lot_px * take
                    if take >= lot_abs - 1e-12:
                        lots.popleft()
                    else:
                        sign = 1 if lot_qty > 0 else -1
                        lots[0][0] = sign * (lot_abs - take)
                    left -= take
                closing = close_amt
                remaining -= close_amt

            if remaining > 1e-12:
                # remainder opens (or extends) position
                lots.append([side_sign * remaining, price])
                opening = remaining

            avg_entry = entry_sum / closing if closing > 0 else float("nan")

            out["position_before"][i] = prev_pos
            out["position_after"][i] = new_pos
            out["opening_qty"][i] = opening
            out["closing_qty"][i] = closing
            out["avg_entry_price"][i] = avg_entry
            out["computed_pnl_usdc"][i] = pnl if closing > 0 else 0.0

            position = new_pos

    return pd.DataFrame(out, index=trades.index)


def match_platform_pnl(trades: pd.DataFrame, pnl: pd.DataFrame) -> pd.DataFrame:
    """For each closing fill, match the nearest unused realized_pnl record (same coin)."""
    matched_pnl = np.full(len(trades), np.nan, dtype="float64")
    matched_gap = np.full(len(trades), np.nan, dtype="float64")
    matched_id = np.full(len(trades), "", dtype=object)

    for coin, ptab in pnl.groupby("underlying"):
        ptab = ptab.sort_values("ts_ms").reset_index(drop=True)
        used = np.zeros(len(ptab), dtype=bool)
        ptimes = ptab["ts_ms"].to_numpy()

        sub = trades[(trades["underlying"] == coin) & (trades["closing_qty"] > 0)]
        sub = sub.sort_values("ts_ms")
        for i in sub.index:
            ts = trades.at[i, "ts_ms"]
            # find nearest unused record within window
            cand = np.argsort(np.abs(ptimes - ts))
            for j in cand:
                if used[j]:
                    continue
                gap = abs(int(ptimes[j]) - int(ts))
                if gap > PNL_MATCH_WINDOW_MS:
                    break
                used[j] = True
                matched_pnl[i] = float(ptab.at[j, "qty"])
                matched_gap[i] = gap
                matched_id[i] = str(ptab.at[j, "id"])
                break

    return pd.DataFrame(
        {"platform_pnl_usdc": matched_pnl,
         "platform_pnl_gap_ms": matched_gap,
         "platform_pnl_id": matched_id},
        index=trades.index,
    )


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    trades_csv = ROOT / args[0] if args else TRADES_CSV
    if not trades_csv.exists():
        print(f"missing: {trades_csv}")
        return 1
    suffix = _suffix_from(trades_csv)
    out_per_trade = ROOT / f"trade_pnl{suffix}.csv"
    out_summary = ROOT / f"trade_pnl_summary{suffix}.csv"
    slip_tick_csv = ROOT / f"slippage_tick_per_trade{suffix}.csv"
    print(f"input: {trades_csv.name}  output suffix: '{suffix}'")

    trades = pd.read_csv(trades_csv)
    trades = trades[trades["status"] == "confirmed"].copy()
    trades["created_at"] = pd.to_datetime(trades["created_at"], utc=True, format="ISO8601")
    trades["ts_ms"] = (trades["created_at"].astype("int64") // 1_000_000).astype("int64")
    trades["price"] = trades["price"].astype(float)
    trades["qty"] = trades["qty"].astype(float)
    trades["notional"] = trades["price"] * trades["qty"]
    trades = trades.reset_index(drop=True)

    fifo = fifo_pnl(trades)
    trades = pd.concat([trades, fifo], axis=1)

    if PNL_CSV.exists():
        pnl = pd.read_csv(PNL_CSV)
        pnl = pnl[pnl["transfer_type"] == "realized_pnl"].copy()
        pnl["created_at"] = pd.to_datetime(pnl["created_at"], utc=True, format="ISO8601")
        pnl["ts_ms"] = (pnl["created_at"].astype("int64") // 1_000_000).astype("int64")
        pnl["qty"] = pnl["qty"].astype(float)
        print(f"trades: {len(trades):,}  pnl_records: {len(pnl):,}")
        plat = match_platform_pnl(trades, pnl)
        trades = pd.concat([trades, plat], axis=1)
        matched = trades["platform_pnl_usdc"].notna().sum()
        closing_fills = (trades["closing_qty"] > 0).sum()
        print(f"closing fills: {closing_fills:,}  matched to platform pnl: {matched:,} "
              f"({100.0*matched/max(closing_fills,1):.1f}%)")
    else:
        print(f"trades: {len(trades):,}  (no PNL_CSV - skipping platform reconciliation)")
        trades["platform_pnl_usdc"] = float("nan")
        trades["platform_pnl_gap_ms"] = float("nan")
        trades["platform_pnl_id"] = ""

    if slip_tick_csv.exists():
        slip = pd.read_csv(slip_tick_csv, usecols=["id", "reference", "gap_ms",
                                                   "slip_bps", "slip_usd"])
        trades = trades.merge(slip, on="id", how="left")
        print(f"merged tick slippage from {slip_tick_csv.name}")

    out_cols = ["id", "created_at", "underlying", "side", "qty", "price", "notional",
                "position_before", "position_after",
                "opening_qty", "closing_qty",
                "avg_entry_price", "computed_pnl_usdc",
                "platform_pnl_usdc", "platform_pnl_gap_ms", "platform_pnl_id"]
    if "slip_bps" in trades.columns:
        out_cols += ["reference", "gap_ms", "slip_bps", "slip_usd"]
    trades[out_cols].to_csv(out_per_trade, index=False)

    # Summary per coin
    rows = []
    for coin, sub in trades.groupby("underlying"):
        closing = sub[sub["closing_qty"] > 0]
        wins = (closing["computed_pnl_usdc"] > 0).sum()
        losses = (closing["computed_pnl_usdc"] < 0).sum()
        slip_total = sub["slip_usd"].sum() if "slip_usd" in sub.columns else float("nan")
        rows.append({
            "underlying": coin,
            "n_fills": len(sub),
            "n_closing": len(closing),
            "wins": int(wins),
            "losses": int(losses),
            "win_rate_pct": 100.0 * wins / max(len(closing), 1),
            "computed_pnl_total": float(closing["computed_pnl_usdc"].sum()),
            "platform_pnl_total": float(sub["platform_pnl_usdc"].sum(skipna=True)),
            "best_trade": float(closing["computed_pnl_usdc"].max()) if len(closing) else float("nan"),
            "worst_trade": float(closing["computed_pnl_usdc"].min()) if len(closing) else float("nan"),
            "slip_total_usd": slip_total,
        })
    summary = pd.DataFrame(rows).sort_values("computed_pnl_total")
    summary.to_csv(out_summary, index=False)

    show = summary.copy()
    for c in ("computed_pnl_total", "platform_pnl_total", "best_trade",
              "worst_trade", "slip_total_usd"):
        show[c] = show[c].round(2)
    show["win_rate_pct"] = show["win_rate_pct"].round(1)
    print("\n=== Per coin P&L ===")
    print(show.to_string(index=False))

    print("\n=== Totals ===")
    closing_all = trades[trades["closing_qty"] > 0]
    print(f"closing fills:        {len(closing_all):,}")
    print(f"wins:                 {int((closing_all['computed_pnl_usdc']>0).sum()):,}"
          f"  ({100.0*(closing_all['computed_pnl_usdc']>0).mean():.1f}%)")
    print(f"computed P&L total:   ${closing_all['computed_pnl_usdc'].sum():,.2f}")
    print(f"platform P&L total:   ${trades['platform_pnl_usdc'].sum(skipna=True):,.2f}")
    if "slip_usd" in trades.columns:
        print(f"slippage total:       ${trades['slip_usd'].sum(skipna=True):,.2f}")
    print(f"\nwrote: {out_per_trade}\nwrote: {out_summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
