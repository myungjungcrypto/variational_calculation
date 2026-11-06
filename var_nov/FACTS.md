# Variational Omni — accounts var1 (= acct1) and var2 (= acct2), one-month exports taken 2026-11-02, snapshot 2026-11-02T14:30Z

## Goal
For each weekly points period (Thu 00:00Z -> Thu 00:00Z, points distributed Friday), reconstruct gross OI-time and volume/displayed-spread cost
for both accounts and back out the weekly OI coefficient b (pt per $1M-week) and spread coefficient a (pt per $1 of displayed half-spread),
and show how b moved week to week (owner suspects it changes weekly).

## Files (absolute paths)
- var_nov/data/var1_export_month_2026-11-02.csv : var1 trades 2026-10-04T03:47Z .. 2026-11-02T06:49Z, 719 confirmed rows
- var_nov/data/var2_export_month_2026-11-02.csv : var2 trades 2026-10-07T02:51Z .. 2026-11-01T10:55Z, 75 rows
- Older exports (for 9/17 .. 10/4): /home/user/variational_calculation/oct15/data/acct1.csv (492 rows, 9/17..10/15T17:43Z) and acct2.csv (8 rows, 9/17..10/09). Union by id (271 / 6 overlapping ids).
- Columns: id,created_at(UTC),side,instrument_type (perpetual_future | perpetual_rwa_future | swap),underlying,price,qty,trade_type,status,liquidation_trigger_price. Use status=confirmed. buy=+qty, sell=-qty.
- Previous analysis of the 10/8-10/15 week: /home/user/variational_calculation/oct15/{week.py,formula.py,RESULTS.md,FACTS.md,FACTS2.md}.

## Points (Distribution History, no referral points on either account)
| period | var1 | var2 |
|---|---|---|
| 2026-10-01 to 10-08 | 163.030848 | 9.985448 |
| 2026-10-08 to 10-15 | 181.443115 | 11.620779 |
| 2026-10-15 to 10-22 | 185.810837 | 10.699622 |
| 2026-10-22 to 10-29 | 178.023552 | 33.478299 |
| 2026-10-29 to 11-05 | (pending, next drop in ~3d 9h from 11/02 14:30Z) | pending |
Earlier var1: 9/24-10/1 174.79, 9/17-9/24 217.71, 9/10-9/17 304.41, 9/3-9/10 356.48, 8/27-9/3 354.13, 8/20-8/27 521.70, 8/13-8/20 505.10, 8/6-8/13 619.72.
Earlier var2: 9/24-10/1 10.81, 9/17-9/24 12.34, 9/10-9/17 34.85, 9/3-9/10 57.71, 8/27-9/3 80.16, 8/20-8/27 46.43, 8/13-8/20 958.60, 8/6-8/13 157.71.
Totals 11/02: var1 12,111.38 (rank 79), var2 4,553.73 (rank 272). Boost is already included in displayed numbers (owner). Weekly period = [Thu 00:00Z, next Thu 00:00Z).

## Position snapshots 2026-11-02T14:30Z (signed qty, mark), from the Positions panels
var1 (16, portfolio $650,284): ETH +1657.61541 @2281.56 | BTC -41.972063 @77663.73 | QQQ(rwa) -1460.3132 @695.978 | XAU swap +127.3682 @3947.90 | XAU perp(rwa) -63.86928 @3951.34 |
  US500(rwa) +198.4545 @760.175 | SOL -700 @93.7177 | GOOGL(rwa) +177.0021 @282.711 | CRWV(rwa) +508.383 @55.7612 | ZEC +18.40849 @1259.87 | ENA +83333.3 @0.1761 |
  CASHCAT +114862.8 @0.1020 | OPENAI(rwa) +6.50818 @1384.01 | ANTHROPIC(rwa) +5.22195 @1657.88 | PONS +31839.1 @0.2340 | USELESS +55968.5 @0.1258. Gross ~$9.19M (swap x1).
var2 (6, portfolio $136,479): ETH -312.66472 @2279.80 | BTC +8.627308 @77626.17 | US500 -594.5698 @760.045 | QQQ +640.8645 @695.579 | XAU swap +68.36556 @3948.02 | XAU perp -62.97051 @3951.49. Gross ~$2.80M.
Earlier snapshot 2026-10-16T05:00Z (oct15/FACTS.md): var1 BTC -27.045073, US500 -2555.7934, QQQ +2652.7277, ETH +771.05239, XAU swap +156.68251, XAU perp -119.82864, SOL +1996.9025, HYPE +1500.604, BNB +70.0015, ZEC +30.34587, CASHCAT +114862.8, PONS +31839.1, OPENAI +6.50818, USELESS +55968.5;
var2 BTC -1.977422, QQQ +211.6094, US500 -197.756, ETH +62.48028, XAU swap +12.09861, XAU perp -4.27614.
CONTINUITY VERIFIED: backward walk from the 11/02 snapshot through the month export lands exactly (1e-5) on the 10/16 snapshot for every market of both accounts. The exports are complete.

## Volume by week (one-way notional, $)
var1: 10/1-8 524K | 10/8-15 7.49M | 10/15-22 11.0M (incl. a BTC round trip 10/21 17:07-18:59Z: 47 BTC bought and 47 sold, $7.93M, net 0) | 10/22-29 8.84M | 10/29-11/2 4.56M.
var2: 10/1-8 118K | 10/8-15 150K | 10/15-22 147K (10/18 ETH sell 20; 10/21 QQQ sell 69.1 / US500 buy 64.8) | 10/22-29 2.12M (all on 10/27) | 10/29-11/1 1.72M.

## Spread-cost convention (displayed half-spread, bp, applied to fill notional) — quotes read 10/16; re-read 11/02: ANTHROPIC spread 0.024% (=1.2bp half? panel shows "Spread 0.024%"), QQQ 0.019%
BTC 0.53, ETH 0.49, XAU swap 0.50, XAU perp 1.66, QQQ 2.47 (11/02 panel 0.019% -> 1.9bp), US500 2.58, HYPE 3.0, ENA 7.4, SOL 0.8, BNB 1.0, XAG 2.5, ZEC 3.0, NEAR 6, PONS 10, LIGHTER 5, WLD 5, ONDO 8, USELESS 8, OPENAI 10, CASHCAT 8, SUI 3, CRWV 5, GOOGL 4, ANTHROPIC 2.4 (11/02 panel 0.024%); default 6.
Prior model (10/8-10/15 week, verified): points = a x displayed-spread$ + b x OI-time($M-wk), a = 0.055-0.06, b = 14.2-14.6 (swap OI x2 per owner, unverifiable; x1 variant too), intercept 0, market-independent.
History of b: Aug ~35 (boost-inclusive display), 10/1-10/8 13.5 (acct3 trade-free), 10/8-10/15 14.2-14.6. The 10/21 BTC round trip ($7.93M, net 0) added only ~+4 pt vs the prior week -> volume is credited by spread, not by notional.

## Addendum 2026-11-06 — fifth week (10/29 -> 11/05) and new snapshot
- Points for 2026-10-29 to 2026-11-05: var1 185.335, var2 50.827 (owner, distributed 11/06).
- New one-month exports (complete, verified by continuity): var_nov/data/var1_export_month_2026-11-06.csv (685 rows, 10/07T18:14Z .. 11/05T23:34Z) and
  var_nov/data/var2_export_month_2026-11-06.csv (72 rows, 10/09 .. 11/01T10:55Z; var2 made NO trades after 11/01). Union all exports by id (var*_month.csv, oct15 acct*.csv).
- Position snapshot 2026-11-06T02:40Z (11:39 KST):
  var1 (15, portfolio $627,796): ETH +1519.61541 @2239.03 | BTC -39.472063 @76037.58 | QQQ -1754.602 @657.454 | US500 +746.0067 @737.001 | XAU swap +122.48962 @4068.57 | XAU perp -63.86928 @4070.96 |
    SOL -700 @90.1483 | ZEC +33.95171 @1310.93 | CRWV +414.543 @45.3263 | ENA +83333.3 @0.1638 | CASHCAT +114862.8 @0.09321 | OPENAI +6.50818 @1333.73 | ANTHROPIC +5.22195 @1508.66 | USELESS +55968.5 @0.1287 | PONS +31839.1 @0.1858 (GOOGL closed).
  var2 (6, portfolio $154,807): ETH -312.66472 @2236.29 | BTC +8.627308 @75949.54 | US500 -594.5698 @736.404 | QQQ +640.8645 @656.778 | XAU swap +68.36556 @4066.93 | XAU perp -62.97051 @4069.60 (unchanged since 11/01).
  Backward walk 11/06 -> 11/02 snapshot matches exactly for both accounts.
- Quick pass (a = 0.055, 15-min grid, price = last own fill else 11/06 mark): week 10/29: var1 OI-time 9.02 (x2) / 8.55 (x1) $M-wk, volume $5.01M, displayed-spread $525, b = 17.4 (x2) / 18.3 (x1); var2 OI-time 2.50 / 2.28, volume $1.72M, cost $224, b = 15.4 / 16.9.
  The exact two-account solve for this week gives a = -0.007, b = 21.0 (inadmissible): at a = 0 both accounts give b = 20.3-20.6, at a = 0.055 they split 17.4 vs 15.4. Earlier weeks (a = 0.055, x2): var1 14.4 / 14.5 / 14.3 / 12.3, var2 15.5 / 15.4 / 13.4 / 26.7.
- Hypotheses to test: (H1) b varies week to week (pool-share: fixed weekly pool divided by platform activity) with a fixed ~0.055; (H2) the spread credit a changed (toward 0) and b rose this week; (H3) a one-off/boost on one account; (H4) the spread-cost table overstates var1's cost this week (RWA spreads tighter: QQQ panel 0.019%, XAU perp panel 0.015% on 11/06).
