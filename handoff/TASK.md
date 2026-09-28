# TASK: Reverse-engineer the Lighter (Robinhood Chain) points model and compute farming economics

You are an independent analyst. Everything you need is in this `handoff/` directory. Work only from the data here
plus public docs; do not assume prior results are correct — `prior_results.md` lists estimates from another analyst
that you should try to **reproduce, refute, or tighten**.

## Goal

Build a quantitative model of how Lighter on Robinhood Chain awards points, then compute the economics of
(a) holding open interest (OI) and (b) trading volume, for the three accounts in `data/lighter/`.

Concretely, estimate with uncertainty:

1. **Live points rate**: points per $1M of gross OI per week accrued in real time. Is it linear in OI? Does it differ
   by market (stock perps SPY/QQQ vs crypto BTC/ETH/HYPE/ZEC)? Is there a real-time credit per $ of volume at fill time?
2. **Friday weekly drop**: points per $1M of weekly volume, and whether the drop also contains an OI component
   (points per $1M-week of OI). Test market weighting and non-linearity in volume.
3. **Referral share**: verify the 10% rule and whether it applies to live points, drops, or both (A1 refers A3 and others).
4. **Cost per point** for each account: explicit fees (Fee column) + spread cost, divided by points earned by the
   account's own activity (exclude referral income).
5. **APR of OI farming** at 10x / 15x / 20x gross OI to equity, across LIT price {3,4,5} and program-end scenarios
   (total points at end = 1.5M / 2.0M / 3.6M / 5.2M, pool = 11,000,000 LIT), for two hedge structures:
   (A) both legs inside Lighter (both legs earn points, funding nets), (B) one leg on another venue (only one leg earns).
6. **Total outstanding points on the platform** (denominator for $/point) from the leaderboard snapshots, and its
   weekly growth rate.

## Data

- `data/lighter/a{1,2,3}_lighter_export_*.csv(.gz)` — full trade history per account since inception
  (columns: Market, Side, Date[UTC, 1-second], Trade Value, Size, Price, Closed PnL, Fee, Role, Type, Trade ID).
  Side mapping: Open Long / Close Short = buy; Open Short / Close Long = sell. Reconstruct positions and gross OI
  over time by cumulative signed size × price (accounts started flat; exports are complete).
- `data/lighter/points_snapshots.csv` — timestamped TOTAL POINTS readings (6 decimals) per account, with notes on
  which windows are trade-free and where the Friday drop fell.
- `data/lighter/positions_snapshots.csv` — mark-price OI, cumulative funding, liquidation prices at snapshot times.
- `data/lighter/leaderboard_snapshots.csv`, `referrals.csv`, `fee_tiers.csv`, `program_facts.md`.
- `data/variational/` — a separate exchange (Variational) whose points model was solved earlier
  (`points_observations.md` + trade exports). Use it only if you want to compare structures; not required.

## Method hints (not mandates)

- Use trade-free snapshot pairs to isolate the live rate; use snapshot pairs that straddle Friday ~15:30 UTC to
  isolate the drop. Subtract modelled live accrual from the drop window.
- A3 has no referees and huge OI (≈$28–36M) → best account for an OI-in-drop coefficient.
  A2 has tiny OI → best for the volume coefficient. A1 has referral income (10% of A3 and other referees) —
  model it explicitly; referee volumes are in `referrals.csv` but their intra-week timing is unknown.
- Week boundaries for the drop: Friday 15:00 UTC → Friday 15:00 UTC (assumed; verify if the data suggests otherwise).
- Check lifetime consistency: each account's cumulative points should ≈ Σ live + Σ real-time credits + Σ drops
  (+ referral for A1).
- Report every coefficient with a range, and state which observation drives it.

## Deliverables

1. `RESULTS.md`: the fitted model (equations + coefficients + ranges), a table of per-account decomposition
   (live / real-time credit / drop / referral) for each snapshot window, cost-per-point table, APR tables, total
   platform points estimate, and an explicit list of what could not be identified from this data.
2. `analysis/` with reproducible scripts (Python, pandas) that regenerate every number in RESULTS.md from `data/`.
3. A short list of the **next measurements** that would most reduce uncertainty (e.g., which account should stay
   trade-free over the next Friday drop).

Keep the write-up plain: state the number, the range, and the evidence. Where the prior analyst and you disagree,
say why.
