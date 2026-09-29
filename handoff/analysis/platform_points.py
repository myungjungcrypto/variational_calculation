#!/usr/bin/env python3
"""Sparse leaderboard bounds and explicit rank-tail sensitivities.

This analysis uses only the two bundled CSVs. It never treats a tail assumption
as a measured platform total. Run directly, or call run(data_dir, output_dir).
All dependencies are from the Python standard library.
"""
from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def read_observations(path, source):
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    return [dict(row, rank=int(row["rank"]), points=float(row["points"]),
                 source=source, time=timestamp(row["timestamp_utc"])) for row in rows]


def write_csv(path, rows):
    if not rows:
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def lower_bound(observations, cutoff):
    """At later times each earlier observed rank is a lower constraint.

    If rank r had p points, at least r accounts then had at least p points.
    Nondecreasing cumulative points preserves that lower count at every later
    time, even if ranks and identities change. L(k)=max_{r>=k} p is therefore a
    valid lower bound on the k-th order statistic at cutoff.
    """
    valid = [row for row in observations if row["time"] <= timestamp(cutoff)]
    largest_rank = max(row["rank"] for row in valid)
    values = []
    segments = []
    for rank in range(1, largest_rank + 1):
        row = max((row for row in valid if row["rank"] >= rank),
                  key=lambda item: item["points"])
        values.append(row["points"])
        if segments and segments[-1]["points_per_rank_lower"] == row["points"]:
            segments[-1]["rank_end"] = rank
        else:
            segments.append({"as_of_utc": cutoff, "rank_start": rank,
                             "rank_end": rank,
                             "points_per_rank_lower": row["points"],
                             "source_timestamp_utc": row["timestamp_utc"],
                             "source_rank": row["rank"], "source": row["source"]})
    for segment in segments:
        segment["segment_points_lower"] = (
            segment["rank_end"] - segment["rank_start"] + 1
        ) * segment["points_per_rank_lower"]
    return {"as_of_utc": cutoff, "total_points_lower": math.fsum(values),
            "total_points_upper": "unbounded", "minimum_observed_account_count": largest_rank}, segments


def log_interpolate(anchors, rank):
    for left, right in zip(anchors[:-1], anchors[1:]):
        if left["rank"] <= rank <= right["rank"]:
            fraction = math.log(rank / left["rank"]) / math.log(right["rank"] / left["rank"])
            return math.exp(math.log(left["points"]) + fraction * math.log(right["points"] / left["points"]))
    raise ValueError(f"Rank {rank} is not covered by anchors")


def run(data_dir=None, output_dir=None):
    here = Path(__file__).resolve().parent
    data_dir = Path(data_dir) if data_dir is not None else here.parent / "data" / "lighter"
    output_dir = Path(output_dir) if output_dir is not None else here
    output_dir.mkdir(parents=True, exist_ok=True)
    leaderboard = read_observations(data_dir / "leaderboard_snapshots.csv", "leaderboard_snapshots.csv")
    accounts = read_observations(data_dir / "points_snapshots.csv", "points_snapshots.csv")
    observations = leaderboard + accounts

    bounds, segments = [], []
    for cutoff in ("2026-09-19T03:59:00Z", "2026-09-28T13:00:00Z"):
        bound, detail = lower_bound(observations, cutoff)
        bounds.append(bound)
        segments.extend(detail)
    write_csv(output_dir / "platform_total_bounds.csv", bounds)
    write_csv(output_dir / "platform_bound_segments.csv", segments)

    # A deliberately labelled approximation: these ranks were observed over
    # 28 minutes, not in a single synchronized screenshot.
    anchor_specs = [(None, "2026-09-19T03:00:00Z", rank) for rank in range(1, 11)] + [
        ("A3", "2026-09-19T03:03:00Z", 29),
        ("A1", "2026-09-19T02:35:00Z", 115),
        ("A2", "2026-09-19T02:42:00Z", 1643),
    ]
    anchors = []
    for account, time, rank in anchor_specs:
        source = leaderboard if account is None else accounts
        row = next(row for row in source if row["timestamp_utc"] == time and row["rank"] == rank
                   and (account is None or row["account"] == account))
        anchors.append({"rank": rank, "points": row["points"], "timestamp_utc": time,
                        "source": row["source"], "account": account or "unknown"})
    write_csv(output_dir / "platform_model_anchors.csv", anchors)
    last_rank, last_points = anchors[-1]["rank"], anchors[-1]["points"]
    prefix_rows = []
    for rank in range(1, last_rank + 1):
        prefix_rows.append({"rank": rank,
                           "interpolated_points": log_interpolate(anchors, rank),
                           "synchronized_prefix_lower": max(row["points"] for row in anchors if row["rank"] >= rank),
                           "synchronized_prefix_upper": min(row["points"] for row in anchors if row["rank"] <= rank)})
    write_csv(output_dir / "platform_prefix_model.csv", prefix_rows)
    prefix = math.fsum(row["interpolated_points"] for row in prefix_rows)
    prefix_lower = math.fsum(row["synchronized_prefix_lower"] for row in prefix_rows)
    prefix_upper = math.fsum(row["synchronized_prefix_upper"] for row in prefix_rows)
    slope_rows = []
    for left, right in zip(anchors[9:-1], anchors[10:]):
        alpha = -math.log(right["points"] / left["points"]) / math.log(right["rank"] / left["rank"])
        slope_rows.append({"rank_start": left["rank"], "rank_end": right["rank"], "power_law_alpha": alpha})
    write_csv(output_dir / "platform_local_slopes.csv", slope_rows)
    observed_alpha = slope_rows[-1]["power_law_alpha"]

    model_rows = []
    for alpha in (0.8, 1.0, observed_alpha, 1.25, 1.5, 2.0):
        for population in (2270, 5000, 10000, 25000, 100000, "infinite"):
            if population == "infinite":
                if alpha <= 1:
                    model_rows.append({"tail_alpha": alpha, "population_N": population,
                                       "first_1643_model_points": prefix,
                                       "total_model_points": "divergent",
                                       "model_sum_lower": "divergent", "model_sum_upper": "divergent"})
                    continue
                # For the decreasing positive model, integral bounds enclose
                # the infinite discrete tail; width is under one anchor score.
                tail_upper = last_points * last_rank / (alpha - 1)
                tail_lower = tail_upper * (1 + 1 / last_rank) ** (1 - alpha)
                lower, upper = prefix + tail_lower, prefix + tail_upper
                total = (lower + upper) / 2
            else:
                tail = math.fsum(last_points * (rank / last_rank) ** (-alpha)
                                 for rank in range(last_rank + 1, population + 1))
                total = lower = upper = prefix + tail
            model_rows.append({"tail_alpha": alpha, "population_N": population,
                               "first_1643_model_points": prefix, "total_model_points": total,
                               "model_sum_lower": lower, "model_sum_upper": upper})
    write_csv(output_dir / "platform_tail_models.csv", model_rows)

    start, end = "2026-09-19T03:59:00Z", "2026-09-28T03:42:00Z"
    elapsed_days = (timestamp(end) - timestamp(start)).total_seconds() / 86400
    top_before = {row["rank"]: row["points"] for row in leaderboard if row["timestamp_utc"] == start}
    top_after = {row["rank"]: row["points"] for row in leaderboard if row["timestamp_utc"] == end}
    growth_rows = []
    for label, ranks in (("rank_1_order_statistic", [1]), ("rank_2_order_statistic", [2]), ("top_2_sum", [1, 2])):
        initial, final = sum(top_before[r] for r in ranks), sum(top_after[r] for r in ranks)
        growth_rows.append({"series": label, "start_utc": start, "end_utc": end,
                            "elapsed_days": elapsed_days, "points_start": initial, "points_end": final,
                            "points_change": final - initial, "relative_change": final / initial - 1,
                            "average_change_per_7_days": (final - initial) * 7 / elapsed_days,
                            "compound_change_per_7_days": (final / initial) ** (7 / elapsed_days) - 1})
    write_csv(output_dir / "platform_observed_growth.csv", growth_rows)
    a3_before = next(row["points"] for row in accounts if row["account"] == "A3" and row["timestamp_utc"] == start)
    a3_after = next(row["points"] for row in accounts if row["account"] == "A3" and row["timestamp_utc"] == end)
    # Final top-two identities are not supplied, but their starting sum cannot
    # exceed the initial top-two sum. Their gains are at least the top-two sum
    # change. A3 is a distinct account at both exact endpoints (ranks 29/22), so
    # its positive gain may be added without double counting.
    platform_growth_lower = growth_rows[-1]["points_change"] + (a3_after - a3_before)
    summary = {
        "inputs": ["data/lighter/leaderboard_snapshots.csv", "data/lighter/points_snapshots.csv"],
        "assumptions_for_bounds": ["Cumulative points for each account do not decrease or disappear.",
                                   "Rank r means at least r accounts have that many points.",
                                   "CSV measurements are accepted as supplied; screenshot timing is approximate."],
        "total_bounds": bounds,
        "near_synchronous_model": {
            "anchor_window_utc": ["2026-09-19T02:35:00Z", "2026-09-19T03:03:00Z"],
            "top_10_points": math.fsum(row["points"] for row in anchors[:10]),
            "prefix_1643_loglog_interpolation": prefix,
            "prefix_1643_monotone_lower_if_synchronized": prefix_lower,
            "prefix_1643_monotone_upper_if_synchronized": prefix_upper,
            "tail_alpha_from_ranks_115_1643": observed_alpha,
            "finite_N_scenarios_at_observed_alpha": [row for row in model_rows if row["tail_alpha"] == observed_alpha],
            "warning": "These are structural model scenarios, not confidence intervals or measured totals."},
        "growth": {"start_utc": start, "end_utc": end, "elapsed_days": elapsed_days,
                   "top_two": growth_rows[-1], "a3_change": a3_after - a3_before,
                   "platform_absolute_change_lower": platform_growth_lower,
                   "platform_average_weekly_change_lower": platform_growth_lower * 7 / elapsed_days,
                   "platform_absolute_change_upper": "unbounded", "platform_percentage_change": "not identified",
                   "warning": "Differences of total lower bounds are not lower bounds on total growth."},
        "prior_assessment": {"500000_current_total": "plausible model scenario, not identified",
                             "100000_to_150000_weekly_growth": "not identified by these snapshots"},
    }
    (output_dir / "platform_summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")

    lines = [
        "# Platform points: bounds and tail sensitivity", "",
        "Only the bundled leaderboard and account point CSVs are used. Bounds assume per-account cumulative points never decrease or disappear and standard rank semantics; approximate screenshot times are accepted as supplied.", "",
        f"- By 2026-09-19 03:59 UTC: at least {bounds[0]['total_points_lower']:,.3f} points.",
        f"- By 2026-09-28 13:00 UTC: at least {bounds[1]['total_points_lower']:,.3f} points.",
        "- Neither date has a finite total upper bound: account count and the unobserved tail are not supplied.", "",
        "Each earlier (rank r, score p) reading implies at least r accounts still have at least p points at the later cutoff. Sum the envelope L(k)=max p over all observations with r>=k and time<=cutoff. Rank identities may change. The calculation uses pre-drop observations too; these are persistent lower constraints, not a synchronized cross-section. See platform_bound_segments.csv.", "",
        f"For a structural model, treat the 2026-09-19 02:35–03:03 readings as approximately synchronous. Top 10 sum={summary['near_synchronous_model']['top_10_points']:,.3f}. Piecewise straight lines in log(rank), log(points) through the top 10, rank 29, rank 115 and rank 1643 sum to {prefix:,.3f} for ranks 1–1643. The monotone-only prefix range under that synchronization assumption is {prefix_lower:,.3f}–{prefix_upper:,.3f}; it is not a confidence interval. Local slopes are in platform_local_slopes.csv.", "",
        f"Extrapolate beyond rank 1643 as p(r)=29.653493*(r/1643)^(-alpha). The rank 115–1643 fitted alpha is {observed_alpha:.6f}. There were already at least 2,270 ranked accounts, so no finite-population scenario below 2,270 is used.", "",
        "| Total ranked accounts N | Total points at fitted tail slope |",
        "|---:|---:|",
    ]
    for row in summary["near_synchronous_model"]["finite_N_scenarios_at_observed_alpha"]:
        if row["population_N"] == "infinite":
            lines.append(f"| Infinite | {row['model_sum_lower']:,.0f}–{row['model_sum_upper']:,.0f} |")
        else:
            lines.append(f"| {row['population_N']:,} | {row['total_model_points']:,.0f} |")
    lines += ["", "The infinite-tail interval bounds numerical summation of that specific model only. It is not an empirical uncertainty interval. platform_tail_models.csv varies alpha over 0.8, 1.0, 1.110446, 1.25, 1.5, 2.0; alpha<=1 has a divergent infinite tail. An approximately 0.5M total is compatible with some assumptions, but is not inferred uniquely from the snapshots.", "",
              f"From {start} to {end} ({elapsed_days:.6f} days), top-two total rose by {growth_rows[-1]['points_change']:,.3f}, or {100*growth_rows[-1]['relative_change']:.3f}% ({100*growth_rows[-1]['compound_change_per_7_days']:.3f}% per 7 days compounded). These are rank slots; account identities are absent. A3 rose by {a3_after-a3_before:,.3f} in the same exact-endpoint interval and is distinct from both final leaders. Therefore platform growth is at least {platform_growth_lower:,.3f} over the interval, equivalent to {platform_growth_lower*7/elapsed_days:,.3f} per 7 days averaged over it. No finite growth upper bound or positive percentage lower bound follows. Do not subtract the two total lower bounds to estimate growth.", "",
              "The prior 100K–150K points/week extrapolation requires stable leaderboard share or a stable distribution, neither observed here. Program-end totals 1.5M/2.0M/3.6M/5.2M belong in reward-value scenarios, not in the measured-denominator estimate.", "",
              "Most useful next measurement: synchronized full leaderboard exports (including ranked-account count and tail) on two consecutive Fridays, with screenshots immediately before and after the drop. A reported platform total would directly settle the denominator."]
    (output_dir / "platform_notes.md").write_text("\n".join(lines) + "\n")
    return summary


if __name__ == "__main__":
    result = run()
    print(json.dumps({"total_bounds": result["total_bounds"],
                      "growth_lower_per_week": result["growth"]["platform_average_weekly_change_lower"]}, indent=2))
