# Lighter on Robinhood Chain — October data (accounts A1 and A2), gathered 2026-10-19

## Goal
Back out, from the trade exports + position snapshots + points snapshots, how many points the program currently pays
(a) per $1M of gross open interest held for one week and (b) per $1M of trading volume, and how that compares with the
September estimates (live 5.5 pt/$M-wk, immediate trade credit 4.6 pt/$M, Friday drop ~45-50 pt/$M volume + ~10 pt/$M-wk OI).

## Files (all paths absolute)
- New exports (web-app export, user-selected date range starting ~10/07):
  - A1: lighter_oct/data/a1_lighter_export_2026-10-19T06_06Z.csv
        1826 rows, 2026-10-07T04:02:37Z .. 2026-10-19T04:28:33Z, 3 rows have Type=liquidation, Role Maker 963 / Taker 863.
  - A2: lighter_oct/data/a2_lighter_export_2026-10-19T06_06Z.csv
        44 rows, 2026-10-08T14:05:20Z .. 2026-10-13T10:55:43Z, Maker 40 / Taker 4, markets HYPE (close long) and ZEC.
  Columns: Market,Side,Date(UTC, 1s),Trade Value(USD, one-way),Size,Price,Closed PnL (may be "-"),Fee,Role,Type,Trade ID.
  Side mapping: Open Long / Close Short = buy (+size); Open Short / Close Long = sell (-size).
- Old exports (through 9/27) in the repo: /home/user/variational_calculation/handoff/data/lighter/a1_lighter_export_2026-09-27T15_00Z.csv
  (67,539 rows, 9/02..9/27T08:11Z) and a2_lighter_export_2026-09-27T15_04Z.csv (1,349 rows, 9/04..9/23T11:39Z).
- September facts, snapshots and the prior model: /home/user/variational_calculation/handoff/data/lighter/{program_facts.md,points_snapshots.csv,positions_snapshots.csv,leaderboard_snapshots.csv,referrals.csv,fee_tiers.csv}
  and /home/user/variational_calculation/handoff/prior_results.md, /home/user/variational_calculation/handoff/RESULTS.md (independent Codex analysis, Korean),
  /home/user/variational_calculation/handoff/analysis/independent_audit.md.

## Points observations
| account | time (UTC) | total points | note |
|---|---|---|---|
| A1 | 2026-09-28T13:00Z | 831.977303 | from points_snapshots.csv |
| A1 | 2026-10-19T06:06Z (approx, same session as export) | 1622.996296 | rank #79 |
| A2 | 2026-09-28T13:00Z | 47.710838 | |
| A2 | 2026-10-19T06:06Z (approx) | 74.694137 | rank #1634 |
Weekly drops land ~Friday 15:30Z (observed 9/18). Drops inside the window: 10/02, 10/09, 10/16. The drop window is believed to be
Fri 15:00Z -> Fri 15:00Z (not proven; Codex sensitivity used 00:00/12:00/15:00/15:30 cutoffs).
Points accrue live (continuous OI accrual + immediate trade credit at fill) plus the Friday drop.

## A1 referral income (A1 receives 10% of referees' trading points; all-time referral page on 10/19)
REFERRAL VOLUME $119.90M, REFERRED 9, REFERRAL LIVE POINTS 145.478049, REFERRAL WEEKLY POINTS 535.735153 (sum 681.213 = all-time referral income).
Per referee (account, referred on, cumulative volume, A1's points all-time):
0xc2..9664 (= A3) 9/3  $95,840,426.77  620.968663 | 0x52..1040 9/21 $15,691,358.34 30.561969 | 0x88..1eaC 9/19 $1,830,420.96 14.392162 |
0x01..201A 9/25 $4,034,833.70 13.748202 | 0x35..3E36 9/24 $129,824.55 0.752557 | 0x42..E4FC 10/11 $2,277,358.16 0.38064 |
0xaF..71f0 9/5 $103,899.76 0.380944 | 0x0c..1b1c 9/19 $1,052.35 0.028062 | 0x10..4890 9/7 none.
Referral volumes on 9/27 (referrals.csv): A3 $35,823,804.21; 0x52 $5,224,966.56; 0x88 $1,830,420.96; 0xaF $103,899.76; 0x0c $1,052.35; 0x35 $506.99; 0x01 $399.03.
A3's own points: 2843.574565 at 2026-09-28T03:42Z; leaderboard rank #10 "0xc2..." shows 6210.2163 on 10/19 (and 620.968663 = 10.000% of it, confirming
A1's cut is 10% of A3's total points). So A3 gained 3366.64 pt and did $60.02M volume between 9/27-28 and 10/19. A3 equity was $2,868,729 on 9/28 (~12.5x gross).
A2 has no referees and no referral income (referral page shows 0).

## Position snapshots at export time (~2026-10-19T06:06Z), signed size, value, mark
A1 (8 positions, Positions panel): QQQ short 3086.9744 ($2,223,640.26, mark 720.33) | SPY long 2759.6577 ($2,123,556.60, 769.50) |
  BTC long 12.50748 ($1,034,918.92, 82,744.0) | ETH short 210.4154 ($511,593.48, 2,431.35) | HYPE long 2740.522 ($218,970.44, 79.901) |
  SOL long 200.034 ($21,144.19, 105.703) | SUI long 10101.1 ($10,201.30, 1.00992) | OPENAI long 2.0435 ($3,201.32, 1,566.59). Gross ~$6.147M.
A2 (6 positions, equity $12,175.93, cross leverage 19.6x): BTC long 0.63102 ($52,220.06, 82,755.0) | QQQ short 69.3924 ($49,986.81, 720.35) |
  SPY long 64.8055 ($49,871.72, 769.56) | ETH short 20.4222 ($49,667.40, 2,432.03) | ZEC long 23.4307 ($27,254.12, 1,163.18) | ANTHROPIC long 5.12698 ($10,256.52, 2,000.5). Gross ~$239.3K.
September snapshots (9/19) are in positions_snapshots.csv.

## Continuity check already done (backward walk from the 10/19 snapshot through the new export vs forward walk from the 9/19 snapshot through the old export)
A1 at new-export start (10/07 04:02Z) vs old-export end (9/27 08:11Z): BTC 9.30748 = 9.30748, ETH -255.2558 = -255.2558, QQQ -3000.661 = -3000.661,
SPY 2804.4795 = 2804.4795, SOL 500 = 500, OPENAI 2.0435 = 2.0435 (exact). Differences => trades in the 9/27-10/07 gap not in either export:
NEAR 3719.74 -> 3169.36 (sold 550.38 NEAR), CASHCAT 142,650.7 -> 0 (closed, ~$30K at ~$0.22), SUI 0 -> 10,443.5 (opened, ~$10K).
A2: BTC/QQQ/SPY/ETH/ANTHROPIC unchanged since 9/19; HYPE 368.034 (9/27) vs 369.572 (10/08 start): +1.538 HYPE in the gap; ZEC 0 (9/27) -> 10.6531 at 10/08 start (opened ~$13K in the gap).
So both accounts have small unrecorded gap volume (A1 roughly $40-60K, A2 roughly $13K) whose timing within 9/27-10/08 is unknown.

## Platform context
Leaderboard top-10 on 10/19: 45422.49, 45104.32, 29564.32, 14917.45, 13743.35, 12532.71, 10003.67, 7736.70, 6651.23, 6210.22 (rank 79 = 1623.0, rank 1634 = 74.69).
On 9/19 03:00Z: 27484.7, 23389.4, 19469.2, 10786.9, 7330.1, 4880.6, 4792.2, 4188.5, 3915.7, 3751.7 (rank 115 = 566, rank 1643 = 29.7). On 9/28: top-1 32571.0, top-2 31162.6.
Fees: Premium taker 3.5bp / maker 1.2bp displayed on 10/19 (A2 panel). LIT price assumption for $/pt: $3.5 (owner), pool 11M LIT.
