# Week 2026-10-08 -> 10-15: what the three exports say about the points rule

## Inputs
- Trades: `data/acct1.csv` (156 in-week fills, $7.49M), `data/acct2.csv` (3 fills, $150K), `data/acct3.csv` (12 fills, $675K).
- OI-time (swap OI x2, 15-min grid, snapshot backward walk): acct1 10.17 $M-wk, acct2 0.751, acct3 0.108 (`week.py`).
- Half-spreads: order-panel quotes read on 10/16 (`FACTS2.md`). Displayed-spread $ paid: acct1 $627, acct2 $7.5, acct3 $114.5.
- Points: acct1 181.44, acct2 11.62, acct3 7.90, acct4 ~1.67 (no export). Previous week acct3 had zero fills and 2.797 pt on $207.6K OI -> b = 13.5 anchor.

## Candidate formula  points = 0.90 x SUM((s - 0.34bp) x notional - $0.032) / $20
Back-solved OI coefficient b = (points - formula spread pts) / OI-time (`formula.py`):

| acct | formula spread pts | implied b (swap OI 2x) | implied b (swap OI 1x) |
|---|---|---|---|
| 1 | 16.5 | 16.2 | 17.3 |
| 2 | 0.10 | 15.3 | 16.6 |
| 3 | 4.10 | 35.0 | 35.5 |

acct3 cannot have b = 35 (same account measured 13.5 the week before with no trades). The formula under-credits
acct3's RWA-heavy volume by ~2 pt. Swap volume 2x changes every account by <= 0.14 pt (not identifiable).

## Linear model  points = a x displayed-spread $ + b x OI-time
Six account-weeks (10/1-10/8 and 10/8-10/15), independent multi-agent fit: a = 0.0605 ($16.5/pt), b = 14.2 +- 0.1,
intercept c = +0.07 +- 0.32 (c = 0.9 rejected, p = 0.08 / 0.004-0.04 under WLS). This-week-only fit: a = 0.0545 ($18.4/pt), b = 14.6.
Residuals all < 0.6 pt. Leave-one-out errors 1-6% vs 4-25% for the formula; the formula loses in 54/54 spread-stress variants.

acct4: held-only predicts 0.71 (+0.96 short); owner statement (3 fills per leg) predicts 1.38; with swap volume 2x 1.51 vs 1.67 actual.
Residual 0.16-0.29 pt is inside the model's scatter but acct4-specific. Needs acct4's export.

## Why the "acct3 really paid wider spreads" rescue of the formula fails
acct3's XAU swap/perp legs are seconds apart. Treating the swap fill as the mid, the perp close on 10/12 06:59 sits 5.5 bp away,
which would make the formula fit (b ~ 17). But acct1's XAU pairs on 10/7 and 10/9 show the perp SELL filling 4.8-6.4 bp ABOVE the swap BUY,
i.e. a negative "spread". The swap-perp gap is basis (perp premium/discount, OI skew), not spread. The displayed spread, which is what
the platform credits, stays ~1.66 bp. So the formula is not rescued; the linear model on displayed spread stands.

Side finding for the swap-long / perp-short structure: XAU perp vs swap basis swings +-5 bp intraday. Entering when the perp is at a
premium (acct1 10/9: +4.4 bp = +$44 on $100K) and exiting when it is still at a premium (acct3 10/12 close: -6 bp = -$59 on $96K) moves
more money than the quoted spread itself.

## Cost per point (linear model, displayed half-spread)
$16.5-18.4 per $1 of displayed spread for every market, so cheaper markets per notional are simply the lower-spread ones:
BTC 0.53 bp -> ~$0.33 of spread per $1K traded -> ~$31-35K of round-trip volume per point; QQQ/US500 2.5 bp -> ~$6.6-7.4K per point.
OI: 14.2-14.6 pt per $1M-week (swap counted 2x) -> at $20/pt and 20x leverage ~30% APR before funding; $40/pt ~59%.

## Open items that would settle the remaining ambiguity
1. Run `python slippage_tick.py oct15/data/acct1.csv` locally (Binance ticks) and re-run `formula.py`: if measured BTC/ETH midpoint spreads
   are ~1 bp (not 0.5), the formula regains ground on acct1 but still fails acct3 and the zero-fill anchor.
2. Cleanest experiment: one account-week with near-zero OI-time and >= $1M of BTC/ETH round trips, logging the displayed Spread per fill.
   Linear model predicts 3.0-3.2 pt per $1M, the formula 0.7-0.85 pt.
3. acct4 trade export (fill count decides 0.66 pt of the gap).
4. Friend's $40/pt: ask for spread-$ sum, OI-time and weekly points; under the linear model $40 is only possible if full bid-ask was logged
   or actual spreads were ~2.5x the displayed ones.
