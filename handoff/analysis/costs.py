#!/usr/bin/env python3
"""Audit reported fees and execution-cost sensitivities from the handoff CSVs.

Run: python handoff/analysis/costs.py
No market quotes are present: spread scenarios are assumptions, not estimates.
Fee and notional aggregation uses integer units at each source column's precision.
All windows use [start, end); source timestamps are UTC, at one-second resolution.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
DATA = HERE.parent / "data" / "lighter"
FEE_SCALE = 10_000
VALUE_SCALE = 1_000_000
SPREAD_SCENARIOS_BP = [-0.5, 0.0, 0.2, 0.5, 1.0]
ALIGNED_WINDOWS = {
    "A1": ("2026-09-19T02:35:00Z", "2026-09-27T15:00:00Z"),
    "A2": ("2026-09-19T02:42:00Z", "2026-09-27T15:00:00Z"),
    "A3": ("2026-09-19T03:59:00Z", "2026-09-26T10:40:00Z"),
}
EXPORT_ENDS = {
    "A1": "2026-09-27T15:00:00Z",
    "A2": "2026-09-27T15:04:00Z",
    "A3": "2026-09-27T10:40:00Z",
}


def scaled_integer(values: pd.Series, places: int) -> pd.Series:
    """Convert the nonnegative, fixed precision source decimal strings exactly."""
    parts = values.str.split(".", n=1, expand=True)
    whole = parts[0].astype("int64")
    fraction = parts[1].fillna("") if parts.shape[1] == 2 else pd.Series("", index=values.index)
    if fraction.str.len().max() > places:
        raise ValueError(f"Source precision exceeds {places} decimal places")
    return whole * (10 ** places) + fraction.str.pad(places, side="right", fillchar="0").astype("int64")


def load_trades(account: str, data_dir: Path = DATA) -> pd.DataFrame:
    paths = list(data_dir.glob(f"{account.lower()}_lighter_export_*.csv*"))
    if len(paths) != 1:
        raise ValueError(f"Expected one export for {account}: {paths}")
    d = pd.read_csv(paths[0], dtype=str)
    if d["Trade ID"].duplicated().any():
        raise ValueError(f"Duplicate Trade IDs within {account}")
    d["timestamp"] = pd.to_datetime(d["Date"], utc=True)
    d["fee_units"] = scaled_integer(d["Fee"], 4)
    d["value_units"] = scaled_integer(d["Trade Value"], 6)
    d["Fee"] = d["fee_units"] / FEE_SCALE
    d["Trade Value"] = d["value_units"] / VALUE_SCALE
    d["Price"] = pd.to_numeric(d["Price"])
    d["Size"] = pd.to_numeric(d["Size"])
    d["direction"] = d["Side"].map({"Open Long": 1, "Close Short": 1, "Open Short": -1, "Close Long": -1})
    if d["direction"].isna().any() or not d["Type"].eq("trade").all():
        raise ValueError("Unexpected side or transaction type")
    return d.sort_values(["timestamp", "Trade ID"], kind="stable").reset_index(drop=True)


def select_window(d: pd.DataFrame, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    keep = pd.Series(True, index=d.index)
    if start is not None:
        keep &= d["timestamp"] >= pd.Timestamp(start)
    if end is not None:
        keep &= d["timestamp"] < pd.Timestamp(end)
    return d.loc[keep]


def summary(d: pd.DataFrame) -> dict:
    volume = int(d["value_units"].sum()) / VALUE_SCALE
    fee = int(d["fee_units"].sum()) / FEE_SCALE
    maker = d.loc[d["Role"].eq("Maker")]
    maker_volume = int(maker["value_units"].sum()) / VALUE_SCALE
    return {
        "fills": len(d),
        "volume_usd": volume,
        "fees_usd": fee,
        "effective_fee_bp": fee / volume * 10_000 if volume else None,
        "maker_fills": len(maker),
        "maker_volume_usd": maker_volume,
        "maker_fill_fraction": len(maker) / len(d) if len(d) else None,
        "maker_volume_fraction": maker_volume / volume if volume else None,
        "taker_volume_usd": volume - maker_volume,
        "first_fill_utc": d["timestamp"].min().isoformat() if len(d) else None,
        "last_fill_utc": d["timestamp"].max().isoformat() if len(d) else None,
    }


def cost_per_point(fees_usd: float, volume_usd: float, own_points: float, spread_bp: float = 0.0) -> float:
    """Own points must exclude referrals and cover the same observation window."""
    if own_points <= 0:
        raise ValueError("Own points must be positive")
    return (fees_usd + volume_usd * spread_bp / 10_000) / own_points


def same_second_gaps(all_trades: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Observable opposite-fill price gaps; NOT a spread or slippage estimator.

    Aggregate same-account/second/market/direction VWAP; inner-join A1 and A3.
    Compare opposite sides only. Quotes, subsecond ordering, order types, and
    trade-through movement remain unknown. Includes Maker and Taker fills.
    """
    groups = {}
    for a in ("A1", "A3"):
        d = all_trades[a]
        g = d.groupby(["timestamp", "Market", "direction"], as_index=False).agg(
            notional=("Trade Value", "sum"), size=("Size", "sum"), fills=("Size", "size")
        )
        g["vwap"] = g["notional"] / g["size"]
        groups[a] = g
    both = groups["A1"].merge(groups["A3"], on=["timestamp", "Market"], suffixes=("_a1", "_a3"))
    both = both.loc[both["direction_a1"] == -both["direction_a3"]].copy()
    both["buy_price"] = np.where(both["direction_a1"] == 1, both["vwap_a1"], both["vwap_a3"])
    both["sell_price"] = np.where(both["direction_a1"] == -1, both["vwap_a1"], both["vwap_a3"])
    both["gap_bp"] = (both["buy_price"] - both["sell_price"]) / ((both["buy_price"] + both["sell_price"]) / 2) * 10_000
    rows = []
    for market, g in both.groupby("Market"):
        rows.append({
            "market": market, "opposite_fill_second_pairs": len(g),
            "median_buy_minus_sell_gap_bp": g["gap_bp"].median(),
            "p05_gap_bp": g["gap_bp"].quantile(.05),
            "p95_gap_bp": g["gap_bp"].quantile(.95),
            "negative_gap_fraction": g["gap_bp"].lt(-1e-8).mean(),
            "a1_notional_in_pairs_usd": g["notional_a1"].sum(),
            "a3_notional_in_pairs_usd": g["notional_a3"].sum(),
        })
    return pd.DataFrame(rows)


def main() -> None:
    HERE.mkdir(exist_ok=True)
    snapshots = pd.read_csv(DATA / "points_snapshots.csv")
    snapshots["timestamp_utc"] = pd.to_datetime(snapshots["timestamp_utc"], utc=True)
    all_trades = {a: load_trades(a) for a in ALIGNED_WINDOWS}
    periods, by_role, by_market, fee_regimes, daily, sensitivity, transitions = [], [], [], [], [], [], []
    for a, d in all_trades.items():
        first_paid = d.loc[d["fee_units"] > 0, "timestamp"].min()
        zero_pre = d[d["timestamp"] < first_paid]
        paid_phase = d[d["timestamp"] >= first_paid]
        for label, frame in [("pre_first_positive_fee", zero_pre), ("from_first_positive_fee", paid_phase)]:
            fee_regimes.append({"account": a, "regime": label,
                               "classification": "Standard proxy" if a in ("A1", "A2") and label.startswith("pre_") else "Premium proxy" if label.startswith("from_") else "initial zero-fee fills; type not known",
                               **summary(frame)})
        transitions.append({
            "account": a, "first_positive_fee_utc": first_paid.isoformat(),
            "last_zero_fee_fill_before_utc": zero_pre["timestamp"].max().isoformat(),
            "later_zero_fee_fills": int(paid_phase["fee_units"].eq(0).sum()),
            "later_zero_fee_volume_usd": int(paid_phase.loc[paid_phase["fee_units"].eq(0), "value_units"].sum()) / VALUE_SCALE,
        })
        start, end = ALIGNED_WINDOWS[a]
        windows = [("lifetime", None, EXPORT_ENDS[a]), ("aligned_points_window", start, end),
                   ("same_start_through_export", start, EXPORT_ENDS[a]), ("after_aligned_points_until_export", end, EXPORT_ENDS[a])]
        s = snapshots.loc[snapshots["account"].eq(a)].sort_values("timestamp_utc")
        for left, right in zip(s.iloc[:-1].itertuples(index=False), s.iloc[1:].itertuples(index=False)):
            if right.timestamp_utc <= pd.Timestamp(EXPORT_ENDS[a]):
                windows.append(("snapshot_interval", left.timestamp_utc.isoformat(), right.timestamp_utc.isoformat()))
        for window, begin, finish in windows:
            w = select_window(d, begin, finish)
            meta = {"account": a, "window": window, "start_inclusive_utc": begin, "end_exclusive_utc": finish}
            row = {**meta, **summary(w)}
            periods.append(row)
            for role, r in w.groupby("Role"):
                by_role.append({**meta, "role": role, **summary(r)})
            for market, r in w.groupby("Market"):
                by_market.append({**meta, "market": market, **summary(r)})
            if window == "aligned_points_window":
                for bp in SPREAD_SCENARIOS_BP:
                    spread = row["volume_usd"] * bp / 10_000
                    sensitivity.append({**meta, "assumed_signed_execution_cost_bp": bp,
                                        "fees_usd": row["fees_usd"], "volume_usd": row["volume_usd"],
                                        "assumed_spread_usd": spread, "fee_plus_assumed_spread_usd": row["fees_usd"] + spread})
        for day, r in d.groupby(d["timestamp"].dt.strftime("%Y-%m-%d")):
            daily.append({"account": a, "date_utc": day, **summary(r)})
    outputs = {"windows": periods, "by_role": by_role, "by_market": by_market, "fee_regimes": fee_regimes,
               "fee_transitions": transitions, "daily": daily, "spread_scenarios": sensitivity}
    for name, rows in outputs.items():
        pd.DataFrame(rows).to_csv(HERE / f"costs_{name}.csv", index=False, float_format="%.6f")
    same_second_gaps(all_trades).to_csv(HERE / "costs_same_second_gaps.csv", index=False, float_format="%.6f")
    (HERE / "costs_summary.json").write_text(json.dumps({
        "window_convention": "[start, end), UTC",
        "fee_precision_usd": .0001, "notional_precision_usd": .000001,
        "spread_identified": False, "spread_scenarios_are_assumptions": True,
        "window_summaries": periods, "fee_regime_transitions": transitions,
    }, indent=2) + "\n")
    write_findings(periods, transitions, snapshots)
    print(pd.DataFrame(periods).query("window == 'aligned_points_window'")[["account", "volume_usd", "fees_usd", "effective_fee_bp", "maker_volume_fraction"]].to_string(index=False))


def write_findings(periods: list[dict], transitions: list[dict], snapshots: pd.DataFrame) -> None:
    get = lambda a, w: next(r for r in periods if r["account"] == a and r["window"] == w)
    point_deltas = {}
    for a, (start, end) in ALIGNED_WINDOWS.items():
        pts = snapshots.loc[snapshots["account"].eq(a)].set_index("timestamp_utc")["points"]
        point_deltas[a] = float(pts.loc[pd.Timestamp(end)] - pts.loc[pd.Timestamp(start)])
    lines = ["# Fee and spread audit", "", "The Fee column supplies explicit transaction fees. Fee sums are exact to the CSV's $0.0001 precision. Reported Trade Value sums are exact to $0.000001. All intervals use [start, end), UTC.", "",
             "| Account | Lifetime volume | Lifetime fees | Aligned-window volume | Aligned-window fees | Aligned-window fee bp | Maker share of window volume |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for a in ALIGNED_WINDOWS:
        l, w = get(a, "lifetime"), get(a, "aligned_points_window")
        lines.append(f"| {a} | ${l['volume_usd']:,.6f} | ${l['fees_usd']:,.4f} | ${w['volume_usd']:,.6f} | ${w['fees_usd']:,.4f} | {w['effective_fee_bp']:.4f} | {w['maker_volume_fraction']:.4%} |")
    lines += ["", "Aligned windows:"]
    for a, (start, end) in ALIGNED_WINDOWS.items():
        lines.append(f"- {a}: {start} through {end}.")
    tail = get("A3", "after_aligned_points_until_export")
    lines += ["", f"A3's export is dated 24 hours after its points snapshot. Its additional ${tail['volume_usd']:,.6f} volume and ${tail['fees_usd']:,.4f} fees after September 26 10:40 UTC must not enter the numerator for the September 26 points denominator. The matched September 19–26 Fee-only cost divided by the observed {point_deltas['A3']:.6f}-point increase is ${get('A3', 'aligned_points_window')['fees_usd']/point_deltas['A3']:.6f}/point. A2's matched fee-only cost divided by {point_deltas['A2']:.6f} points is ${get('A2', 'aligned_points_window')['fees_usd']/point_deltas['A2']:.6f}/point. A1 requires a referral subtraction before a fee-per-own-point value can be identified.", "", "## Fee regimes", ""]
    for r in transitions:
        lines.append(f"- {r['account']}: first positive fee at {r['first_positive_fee_utc']}; last preceding zero-fee fill at {r['last_zero_fee_fill_before_utc']}. Later {r['later_zero_fee_fills']} zero-fee fills total only ${r['later_zero_fee_volume_usd']:.6f}, consistent with fee-display rounding.")
    lines += ["", "The pre/post-first-positive-fee split is an observed charging-regime proxy, not proof of exact account-upgrade times. It supports the Standard-to-Premium change for A1/A2. A3 has only a brief initial zero-fee sequence; its account type in that interval is not supplied. Tier changes, small-fill rounding, and maker/taker mix mean the realized fee rate should be read from the export rather than imposed as 3.41 bp for all accounts.", "", "## Spread identification", "",
              "The supplied package contains no quote, midpoint, arrival-price, or external reference tick history. Spread cost is therefore not identified. Closed PnL mixes execution costs with market moves and cannot replace a spread measurement. The earlier analyst's Binance-based +0.2/-0.6 bp claims cannot be reproduced from these inputs.", "",
              "costs_same_second_gaps.csv is only a diagnostic of A1/A3 opposite-direction same-second VWAP gaps. Its samples have unknown subsecond ordering and changing prices and are selected by both accounts' execution behavior. They cannot establish a midpoint, each account's cost, or the cost of non-overlapping fills, so they are not inserted in cost-per-point estimates.", "",
              "For a clearly labeled scenario, use cost/own-point = (Fee + volume × s / 10,000) / own_points, where s is signed execution shortfall in basis points per dollar of reported one-way notional. Negative s represents favorable execution/maker spread capture. The [-0.5, 0, 0.2, 0.5, 1] bp rows are sensitivity cases, not a data-driven confidence range. Each extra 1 bp costs $100 per $1M traded. Funding and price/hedge PnL are outside this transaction-cost definition.", "",
              "Future fees to close existing positions are not in realized fees. Observed-window cost-per-point also compares cash fees with point deliveries in that period: the weekly drop may reward trading before the first snapshot. It is a realized window ratio, not a marginal causal farming cost.", ""]
    (HERE / "costs_findings.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
