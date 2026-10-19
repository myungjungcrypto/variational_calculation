# Lighter (Robinhood Chain) and Variational points models — final (2026-10-19)

Both platforms were reverse-engineered from trade exports, position snapshots and points snapshots.
Two multi-agent rounds for Lighter (`RESULTS_round1.md`, `RESULTS_round2_sector.md`) with adversarial verification;
the Variational week model is in `../oct15/RESULTS.md`.

## Lighter — formula (window averages; drop rates vary week to week, 9/25 paid ~10-15% less than 9/18 and October)

    weekly points = 5.5 x OI_total                       (live, continuous)
                  + 4.6 x volume                         (credit at fill)
                  + Friday drop [ ~35-40 x volume_week + ~24 x OI_crypto_week + ~7 x OI_stock_week ]
                  + 0.10 x referees' points
    OI in $M of position value (long and short both count, as the Positions panel shows); volume one-way $M.

| all-in rate | central | range | which weeks |
|---|---|---|---|
| crypto OI, pt per $1M-week | 30 | 28-33.5 (October-only 34-37) | idle-ETH account 9/4-10/16 (7 drops) + A1/A2/A3 windows |
| stock-perp OI (SPY/QQQ/pre-IPO) | 12 | 10-13 (October-only 8-10) | 9/25 + Oct 2/9/16; pinned by A1 only |
| volume, pt per $1M | 40 | 35-44 | 9/18, 9/25 drops and A2 W1 |
| crypto / stock ratio | 2.4-2.5x | 2.0-3.2 (October-only 3.5-4.6) | |

Live 5.5 is measured directly on trade-free intervals (A1 5.4, A2 6.0, A3 5.1); the fits pin the all-in totals, not the live/drop split.
Maker and taker volume are credited equally (41:27 or any split is rejected; free fit 34:34). Market-independent OI is rejected
(idle-ETH account mispredicted by 31-50%, A2 October by 22-29%).

Not excluded: a sector-blind concave size rule (small accounts earn more per $, chi2 37 vs 40 for the sector rule). The two are
separated by one observation: swap A2's SPY/QQQ legs (~$50K each) to BTC/ETH for one trade-free drop week and bracket the Friday drop
(14:30Z / 16:00Z snapshots on A1, A2, the idle ETH account and leaderboard A3). Sector rule predicts A2's drop 6.7 -> 8.6 pt (+27%),
the size rule predicts no change. Alternative: an idle $100K SPY position for one drop week: 1.2 pt (sector) vs 2.3 pt (size).

Pipeline checks: four independent reconstructions agree to 3e-16 on 240 quantities; 9/19 -> 10/19 position continuity exact (A1 5e-10).
The friend's 35.5 pt per $1M-week is consistent with an October crypto book (34-37) or with A1's all-in 35 fitted on owner data only;
it is not evidence of a computation error. Two conventions also reproduce 35.5 from an A1-like book: a 2x Robinhood-Wallet account, or
counting only one leg of a pair as OI.

### Economics (LIT price must be read live; $3.5 used here, 11M LIT pool)
$/pt by end total: 1.5M $25.7, 2M $19.3, 3M $12.8, 5M $7.7. Platform stock ~0.9-1.2M on 10/19, issuance ~100-160K/week (flat; no dilution evidence).

OI APR on equity = rate x 52 x $/pt x leverage / 1e6 (both legs on Lighter, funding excluded), cells 10x/15x/20x:

| OI type | end 1.5M | 2M | 3M | 5M |
|---|---|---|---|---|
| crypto 30 | 40/60/80% | 30/45/60% | 20/30/40% | 12/18/24% |
| stock 11 | 15/22/29% | 11/17/22% | 7/11/15% | 4/7/9% |
| A1 mix (72% stock) ~17 | 23/34/45% | 17/25/34% | 11/17/23% | 7/10/13% |
October-only crypto 35.5: multiply the crypto row by 1.18. APR scales linearly with LIT price.

Volume cost per point at 40 pt/$M: taker 3.5bp $8.75, maker 1.2bp $3.00 (realized A1 October $1.22/pt incl. OI points, A2 $0.96).

Counterfactual: A1's 13.3 $M-wk of SPY/QQQ/OPENAI drop-window OI moved to BTC/ETH pairs at equal gross and volume: +220 pt (x1.54, central)
to +375 pt (x1.9, October-only rates) per three weeks; zero if the size rule is true.
Risks of the swap: funding (A1's stock pair earned +1.26% of gross/yr, crypto pair paid -0.29%: a -31%/yr swing on 20x equity, net of points
+7% at $19/pt central, +21% at October rates, negative above ~3.75M end total); liquidation distance for a two-leg pair 7.6% at 20x,
10.9% at 15x, 17.6% at 10x (ETH/BTC moved -8.8% between 9/19 and 10/19 marks, QQQ/SPY -1%); SPY/QQQ perps trade 24/7 on Lighter.

## Variational — formula (week 10/8-10/15; rates decline week over week: b was ~35 in August, 13.5-14.6 in October)

    weekly points = 0.055-0.06 x sum(fill notional x displayed half-spread)   ( = $16.5-18.4 of displayed spread per point )
                  + 14.2-14.6 x OI ($M-week, swap OI x2 per owner, unverifiable)
                  + 0 intercept; boost already applied

Market-independent (RWA and crypto earn the same per displayed-spread dollar); the candidate formula 0.90 x sum((s-0.34bp) x N - $0.032)/$20
is rejected (loses in 54/54 spread variants, needs b = 35 for account 3). Cost per point by market = $16.5-18.4 per $1 of displayed spread,
i.e. BTC 0.53bp -> ~$31-35K of volume per point, QQQ/US500 2.5bp -> ~$6.6-7.4K per point. OI APR at $20/pt, 20x: ~30%; at $40/pt ~59%.
Side finding: XAU swap vs XAU perp basis swings +-5bp intraday; entry/exit timing moves more money than the quoted spread.
