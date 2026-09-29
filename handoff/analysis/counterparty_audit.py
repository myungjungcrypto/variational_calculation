#!/usr/bin/env python3
"""Local A1/A3 execution-overlap audit; does not infer intent or penalties.

Run from any directory with Python 3 + pandas. Only bundled raw exports are read.
Shared execution fields and opposite maker/taker roles are evidence about fills,
not evidence of beneficial ownership, coordinated orders, or point eligibility.
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
DATA = HERE.parent / "data" / "lighter"
SIDE = {"Open Long": 1, "Close Short": 1, "Open Short": -1, "Close Long": -1}
MATCH_FIELDS = ["Date", "Market", "Price", "Size", "Trade Value"]


def total(values):
    return sum((Decimal(value) for value in values), Decimal(0))


def run():
    paths = {a: next(DATA.glob(f"{a.lower()}_lighter_export_*.csv*")) for a in ("A1", "A3")}
    frames = {a: pd.read_csv(path, dtype=str) for a, path in paths.items()}
    for a, d in frames.items():
        assert not d["Trade ID"].duplicated().any(), f"Duplicate Trade IDs inside {a}"
        d["direction"] = d.Side.map(SIDE)
        assert d.direction.notna().all()
    a1, a3 = frames["A1"], frames["A3"]

    shared = a1.merge(a3, on="Trade ID", suffixes=("_a1", "_a3"), validate="one_to_one")
    shared["all_execution_fields_equal"] = True
    for field in MATCH_FIELDS:
        shared["all_execution_fields_equal"] &= shared[f"{field}_a1"].eq(shared[f"{field}_a3"])
    shared["opposite_directions"] = shared.direction_a1.eq(-shared.direction_a3)
    shared["maker_taker_compatible"] = (
        shared.Role_a1.eq("Maker") & shared.Role_a3.eq("Taker")
    ) | (shared.Role_a1.eq("Taker") & shared.Role_a3.eq("Maker"))
    shared = shared.sort_values(["Date_a1", "Trade ID"])
    shared.to_csv(HERE / "counterparty_shared_ids.csv", index=False)

    # An independent join checks whether exact matching executions appear with
    # different IDs. Compare canonical fixed-precision strings as supplied.
    exact = a1.merge(a3, on=MATCH_FIELDS, suffixes=("_a1", "_a3"))
    exact = exact.loc[exact.direction_a1.eq(-exact.direction_a3)].copy()
    exact["trade_id_equal"] = exact["Trade ID_a1"].eq(exact["Trade ID_a3"])
    exact.to_csv(HERE / "counterparty_exact_field_matches.csv", index=False)

    # These are account-side/role groups sharing a second and market, not
    # one-to-one execution matches. Do not infer counterparties from this join.
    grouped = {}
    for a, d in frames.items():
        grouped[a] = d.groupby(["Date", "Market", "direction", "Role"], as_index=False).size()
    overlap = grouped["A1"].merge(grouped["A3"], on=["Date", "Market"], suffixes=("_a1", "_a3"))
    overlap = overlap.loc[overlap.direction_a1.eq(-overlap.direction_a3)]
    role_counts = overlap.groupby(["Market", "Role_a1", "Role_a3"], as_index=False).size()
    role_counts = role_counts.rename(columns={"size": "same_second_opposite_side_groups"})
    role_counts.to_csv(HERE / "counterparty_same_second_roles.csv", index=False)

    compatible = shared.loc[shared.all_execution_fields_equal & shared.opposite_directions & shared.maker_taker_compatible]
    notional = total(compatible["Trade Value_a1"])
    lifetime = {a: total(d["Trade Value"]) for a, d in frames.items()}
    result = {
        "inputs": {a: {"path": str(path.relative_to(HERE.parent)),
                       "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for a, path in paths.items()},
        "shared_trade_id_count": len(shared),
        "shared_ids_matching_all_execution_fields_and_roles": len(compatible),
        "exact_opposite_field_match_count": len(exact),
        "exact_opposite_field_matches_with_different_ids": int((~exact.trade_id_equal).sum()),
        "single_sided_matched_notional_usd": str(notional),
        "two_export_sum_of_matched_notional_usd": str(2 * notional),
        "matched_notional_share_of_lifetime_pct": {a: float(notional / v * 100) for a, v in lifetime.items()},
        "lifetime_notional_usd": {a: str(v) for a, v in lifetime.items()},
        "role_fill_counts": {a: d.Role.value_counts().to_dict() for a, d in frames.items()},
        "same_second_group_role_counts": role_counts.to_dict(orient="records"),
        "interpretation": [
            "Six shared IDs also match UTC second, market, price, size and notional, with opposite directions and complementary maker/taker roles.",
            "This strongly supports opposite sides of the same six executions, assuming the exports use common execution IDs; ID semantics were not independently verified here.",
            "The combined value counts each execution twice across the two exports; single-sided matched notional is the appropriate match-volume figure.",
            "The SPY/QQQ same-second opposite groups are Taker/Taker and share no IDs; normal maker/taker semantics do not make them opposite sides of one trade.",
            "The export contains no counterparty address field, order IDs, order submissions, IP/control information, point eligibility flags, or penalty ledger.",
            "Intent, common beneficial ownership, coordination, rule violation, netting/deductions and future enforcement cannot be inferred from these matches.",
            "A point-model residual is not a penalty measurement: the fitted reward rule and referral allocation are independently uncertain.",
        ],
    }
    (HERE / "counterparty_summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    rows = []
    for r in compatible.to_dict(orient="records"):
        rows.append(f"| {r['Trade ID']} | {r['Date_a1']} | {r['Market_a1']} | {r['Price_a1']} | {r['Size_a1']} | {r['Trade Value_a1']} | {r['Side_a1']} / {r['Role_a1']} | {r['Side_a3']} / {r['Role_a3']} |")
    text = [
        "# A1/A3 execution-overlap audit", "",
        "This is a local raw-export audit. It does not determine intent, common ownership, rule violations or point penalties.", "",
        f"There are {len(shared)} shared Trade IDs. All also match exact UTC second, market, price, size and Trade Value, with opposite trade directions and A1 Maker / A3 Taker roles. This strongly supports the two exports describing opposite sides of the same six executions. Exchange-wide Trade ID semantics were not independently verified; the exports do not contain counterparty addresses.", "",
        f"Single-sided matched volume is ${notional:,.6f}, or {result['matched_notional_share_of_lifetime_pct']['A1']:.6f}% of A1 lifetime volume and {result['matched_notional_share_of_lifetime_pct']['A3']:.6f}% of A3 lifetime volume. Adding both exports gives ${2*notional:,.6f}, which double-counts these executions. All six are BTC/ETH/ZEC on September 22–23. There are no shared IDs in SPY/QQQ.", "",
        "| Trade ID | UTC | Market | Price | Size | USD notional | A1 | A3 |",
        "|---|---|---|---:|---:|---:|---|---|", *rows, "",
        "An independent exact (timestamp, market, price, size, notional) opposite-side join finds precisely these six pairs and no differently numbered pairs.", "",
        "The same-second opposite-side/role aggregation finds 844 SPY and 538 QQQ groups. All are Taker/Taker, unlike the six complementary Maker/Taker records. Under ordinary matching semantics both takers are taking different resting orders, so synchronized opposite flow alone does not show that these accounts traded with each other. Group counts are not one-to-one transaction counts.", "",
        "A small number of direct matches is compatible with incidental matching in a shared public order book. The data does not distinguish incidental matching from intentional coordination. Nor does it verify the owners' identity/IP statements; those are supplied account facts. Different people/IPs do not reveal what the exchange's eligibility or enforcement system decided.", "",
        "No point-eligibility flags, penalty notices, counterparty addresses, order submission history or exact reward formula are present. Existing model residuals can arise from market/account weights, unknown referral receipts, mark approximation, timing, eligibility or a changing reward rule. They cannot independently establish a penalty or cross-account OI netting.", "",
        "Reproduce with `python3 handoff/analysis/counterparty_audit.py`. Matching CSVs preserve the original execution fields for review.", "",
    ]
    (HERE / "counterparty_findings.md").write_text("\n".join(text))
    print(json.dumps({k: v for k, v in result.items() if k not in ("inputs", "interpretation", "same_second_group_role_counts")}, indent=2))
    return result


if __name__ == "__main__":
    run()
