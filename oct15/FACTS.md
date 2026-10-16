# Facts for week 2026-10-08 00:00 UTC -> 2026-10-15 00:00 UTC (Variational Omni points, distributed Fri 10/16 00:00 UTC)

Official rule: points distributed every Friday 00:00 UTC for activity through the previous Thursday 00:00 UTC.
So the "2026-10-08 to 2026-10-15" row covers [2026-10-08T00:00Z, 2026-10-15T00:00Z).

## Data files (raw Variational trade exports, columns: id,created_at(UTC),side,instrument_type,underlying,price,qty,trade_type,status,liquidation_trigger_price)
- data/acct1.csv : account 1, 492 fills, 2026-09-17 .. 2026-10-15T17:05Z  (does NOT start from flat; positions existed before 9/17)
- data/acct2.csv : account 2, 8 fills, 2026-09-17 .. 2026-10-09  (account was liquidated/reduced before 10/1 by non-trade events; use snapshot + backward walk)
- data/acct3.csv : account 3, 15 fills, 2026-09-17 .. 2026-10-12  (account WAS flat before 2026-09-17 -> forward cumsum from zero is valid; no snapshot given)
instrument_type values: perpetual_future (crypto perp), perpetual_rwa_future (stock-index / gold perp), swap (XAU swap).
Owner's claim to TEST: swap OI is weighted 2x in points.

## Points received for this week
acct1 181.443115 | acct2 11.620779 | acct3 7.900982 | acct4 1.670000 (approx, "1.67")
Previous week (10/1-10/8): acct1 163.030848 | acct2 9.985448 | acct3 2.797166 (acct3 had zero trades that week, OI ~ $207.6K -> implied OI rate 13.5 pt per $1M-week)

## Position snapshots (mark prices) taken ~2026-10-16T05:00Z (derived from the "Next Drop" countdown 6d19h01m before Fri 10/23 00:00Z)
acct1 (14 positions, portfolio $696,077):
  BTC perp -27.045073 @82011.01 | US500 rwa -2555.7934 @773.695 | QQQ rwa +2652.7277 @730.378 | ETH perp +771.05239 @2439.89
  XAU swap +156.68251 @4172.70 | XAU rwa -119.82864 @4174.81 | SOL +1996.9025 @105.427 | HYPE +1500.604 @80.8193
  BNB +70.0015 @728.510 | ZEC +30.34587 @1210.22 | CASHCAT +114862.8 @0.1201 | PONS +31839.1 @0.3356 | OPENAI rwa +6.50818 @1526.27 | USELESS +55968.5 @0.1744
acct2 (6 positions, portfolio $27,184):
  BTC perp -1.977422 @82024.69 | QQQ rwa +211.6094 @730.408 | US500 rwa -197.756 @773.705 | ETH perp +62.48028 @2440.56 | XAU swap +12.09861 @4172.95 | XAU rwa -4.27614 @4175.21
acct3: no snapshot. From export: QQQ +140.8958 / US500 -131.159 held from 9/17 until closed 2026-10-10T13:57Z; XAU swap+perp round trip 2026-10-12 04:40Z->06:59Z (~$97K each leg); QQQ +53.1091 / US500 -51.2312 opened 2026-10-12T07:02Z and still open.
acct4 (no export yet): XAU swap long 4.1225 @ entry 4080.99 (value $17,089.86 at mark ~4145) + XAU perp short (value $17,017.69). Owner says "held for a week". Chart shows buy marker near Oct 6-8 and a sell+buy pair near Oct 14-15 (possible small round trip). Received 1.67 points for the week.

## Spread-cost convention used so far (half-spread bps per market, applied to notional)
BTC 0.61, ETH 1.33, HYPE 3.4, XAU 1.5 (assumed), QQQ 1.5 (assumed), US500 1.5 (assumed), SOL 1.8, BNB 2.7, ZEC 6.0, ENA 8.1, NEAR 6.4, ONDO 8.7, PONS 12, USELESS 8.5, ZRO 7.8, CASHCAT 8, OPENAI 10, XAG/WLD/others 8 (assumed).
Measured values (BTC/HYPE/ZEC/ENA/NEAR/ONDO/PONS/USELESS/ZRO) come from a Binance-tick analysis of the 10/1-10/8 fills. Variational credits points on its OWN quoted spread; Binance-referenced midpoint ~= that in normal weeks.

## Prior model (to be tested, not trusted)
points = a * spread_cost($) + b * OI_time($M-weeks, swap x2), with b ~= 13.5 (10/1-10/8) and a ~= 0.077 (10/1-10/8, cost measured). Earlier (Aug) b ~= 35, a ~= 0.055-0.11.
Open questions: (1) is swap really 2x? (2) do RWA (index/gold) OI and crypto OI earn the same rate? (3) does swap OI accrue when the swap market is closed (XAU trades ~Sun 22:00Z - Fri 21:00Z, ~71% of the week)? (4) how do acct4's 1.67 points fit?

## Reference implementation
week.py (same dir) reproduces: acct1 weighted OI avg $10.167M (raw $9.559M), volume $7.488M, est cost $984; acct2 OI_w $0.751M, vol $150K, cost $22.5; acct3 OI_w $0.108M, vol $675K, cost $101.
Joint LS (3 accts): a=0.0586, b=12.18. With b fixed 13.5: a = 0.045/0.066/0.064 for acct1/2/3.
Run with: python3.13 -I week.py data
