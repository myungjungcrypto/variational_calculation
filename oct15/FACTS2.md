# Addendum (supersedes the bps table in FACTS.md) — actual Variational quoted half-spreads at real clip sizes, read from the order panel on 2026-10-16

| market | size shown | Buy / Sell | half-spread (bp) | note |
|---|---|---|---|---|
| BTC perp | 1 BTC (~$82K) | 82,076.31 / 82,067.66 | 0.53 | displayed "Spread 0.0053%" |
| ETH perp | 40 ETH (~$98K) | 2,443.07 / 2,442.83 | 0.49 | |
| XAU swap | $50K | 4,174.93 / 4,174.51 | 0.50 | swap funding (L) 0.0162% / (S) 0.0071%, countdown 15h33m (looks daily) |
| XAU perp (rwa) | $50K | 4,176.95 / 4,175.56 | 1.66 | 4h funding 0% |
| QQQ perp (rwa) | $100K | 730.000 / 729.640 | 2.47 | |
| US500 perp (rwa) | $100K | 773.772 / 773.373 | 2.58 | a $100K buy was blocked: "OI skew too large" |
| HYPE perp | at clip size | (owner) | ~3.0 | size-0 quote was 0.48 -> strong size dependence on thin markets |
| ENA perp | $5K | 0.2033 / 0.2030 | 7.4 | other alts assumed 5-10 |
Other assumptions: SOL 0.8, BNB 1.0, XAG 2.5, ZEC 3.0, NEAR 6, PONS 10, LIGHTER 5, WLD 5, ONDO 8, USELESS 8, OPENAI 10, CASHCAT 8.
The displayed "Spread" IS the half-spread (= what a market order pays vs mid, one side).

## Owner statements to test
1. Swap volume (spread paid on swap fills) is credited 2x, like swap OI.
2. acct4 did NOT simply hold: it opened both legs inside the week, closed everything mid-week and re-opened the same positions (so ~3 fills per leg: ~$51K swap volume, ~$51K perp volume).
3. A friend who logged every fill's displayed Spread on his own account measured a mining cost of ~$40 per point (mostly BTC/ETH trading).
4. Candidate exact formula (origin unknown): points = 0.90 x SUM_fills( [spread_paid_bp − 0.34bp] x notional − $0.032 ) / $20  (i.e. 90% of platform net spread revenue, converted at $20/pt). Under it the cost per point is 22.2 x s/(s−0.34): BTC(0.53bp) ~$62, XAU perp ~$28, QQQ ~$26, alts ~$23, floor $22.2.

## Updated weekly costs with the quoted spreads (my computation; verify)
acct1: ~$620 (BTC 188, ETH 126, HYPE 72, XAU 57 assuming half swap/half perp, index 25, alts ~90, others ~60); acct2: $7.5 (XAU swap $150K x 0.5bp); acct3: $114 (swap $194K x 0.5, perp $194K x 1.66, QQQ/US500 $288K x 2.5).
With a=0.06: b = 14.2 / 15.0 / ~10(noisy) for acct1/2/3. With a=0.025 (friend): b = 16.3 / 15.2 / 47 (acct3 breaks).

## Workflow-1 findings (prior run, old bps table)
- Small accounts systematically under-predicted by ~+1 pt (acct2 +1.0-1.3, acct3 +0.6-0.9, acct4 +0.7-1.0) -> an intercept c ~ 0.9 pt/account/week fits everything incl. acct4 for any swap weight.
- Swap OI weight and off-market accrual are unidentifiable from acct1-3 (swap share 1-8% of OI).
- a,b are strongly collinear (corr -0.995); 68% intervals a 0.03-0.08, b 10-16.
