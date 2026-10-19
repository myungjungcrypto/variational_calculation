# Lighter on Robinhood Chain — accounts A1 and A2, one-month exports taken 2026-10-19T06:30Z

## Goal
Back out, from the trade exports + position snapshots + points snapshots, how many points the program pays
(a) per $1M of gross open interest held for one week and (b) per $1M of trading volume, for the October weeks, and compare
with the September estimates (live 5.5 pt/$M-wk accrued continuously, immediate trade credit 4.6 pt/$M at fill,
Friday drop ~45-50 pt/$M volume + ~10 pt/$M-wk OI; Codex pooled alternative 38.6 + 12.1).

## Files (absolute paths)
- ONE-MONTH exports (complete; every row of the earlier exports in range is present, verified by Trade ID):
  - A1: lighter_oct/data/a1_lighter_export_month_2026-10-19T06_30Z.csv
        5740 rows, 2026-09-20T05:58:47Z .. 2026-10-19T04:28:33Z, 3 rows Type=liquidation, Taker 4373 / Maker 1367, fees $1,044.
  - A2: lighter_oct/data/a2_lighter_export_month_2026-10-19T06_30Z.csv
        222 rows, 2026-09-19T18:31:11Z .. 2026-10-13T10:55:43Z, Maker 210 / Taker 12, fees $65.5, markets ZEC and HYPE only.
  Columns: Market,Side,Date(UTC, 1s),Trade Value(USD, one-way),Size,Price,Closed PnL (may be "-"),Fee,Role,Type,Trade ID.
  Side mapping: Open Long / Close Short = buy (+size); Open Short / Close Long = sell (-size). Liquidation rows reduce positions like fills.
- Older exports (9/02..9/27) for anything before 9/20: /home/user/variational_calculation/handoff/data/lighter/a1_lighter_export_2026-09-27T15_00Z.csv (67,539 rows)
  and a2_lighter_export_2026-09-27T15_04Z.csv (1,349 rows). A3's September export: a3_lighter_export_2026-09-27T10_40Z.csv.gz (same dir).
- September facts and snapshots: /home/user/variational_calculation/handoff/data/lighter/{program_facts.md,points_snapshots.csv,positions_snapshots.csv,leaderboard_snapshots.csv,referrals.csv,fee_tiers.csv};
  prior analyses: /home/user/variational_calculation/handoff/prior_results.md, /home/user/variational_calculation/handoff/RESULTS.md (Codex, Korean), /home/user/variational_calculation/handoff/analysis/independent_audit.md and the scripts in handoff/analysis/.

## Points observations (total points shown on the Points page; includes referral income for A1)
| account | time (UTC) | total points | note |
|---|---|---|---|
| A1 | 2026-09-19T02:35Z | 568.296016 | positions snapshot same time (positions_snapshots.csv) |
| A1 | 2026-09-27T15:00Z | 825.327811 | export time |
| A1 | 2026-09-28T13:00Z | 831.977303 | equity $255,654 |
| A1 | 2026-10-19T06:06Z (+-10 min) | 1622.996296 | rank #79; positions snapshot same session |
| A2 | 2026-09-19T02:42Z | 29.653493 | positions snapshot same time |
| A2 | 2026-09-27T15:00Z | 47.524591 | |
| A2 | 2026-09-28T13:00Z | 47.710838 | equity $11,494 |
| A2 | 2026-10-19T06:06Z (+-10 min) | 74.694137 | rank #1634; equity $12,175.93 |
Earlier A1/A2 snapshots around the 9/18 drop are in points_snapshots.csv.
Weekly drops land ~Friday 15:30Z (observed 9/18 between 14:31Z and 15:32Z). Drops inside 9/28->10/19: 10/02, 10/09, 10/16. Drops inside 9/19->9/28: 9/25.
The drop's activity window is believed to be Fri 15:00Z -> Fri 15:00Z (not proven; test 00:00Z too).
Points accrue live (continuous OI accrual + immediate trade credit at fill) plus the Friday drop.

## A1 referral income (10% of referees' points). All-time referral page on 10/19:
REFERRAL VOLUME $119.90M, REFERRED 9, REFERRAL LIVE POINTS 145.478049, REFERRAL WEEKLY POINTS 535.735153 (sum 681.213 = all-time referral income).
Per referee (joined, cumulative volume 10/19, A1's points all-time): 0xc2..9664 (= A3) 9/3 $95,840,426.77 620.968663 | 0x52..1040 9/21 $15,691,358.34 30.561969 |
0x88..1eaC 9/19 $1,830,420.96 14.392162 | 0x01..201A 9/25 $4,034,833.70 13.748202 | 0x35..3E36 9/24 $129,824.55 0.752557 | 0x42..E4FC 10/11 $2,277,358.16 0.38064 |
0xaF..71f0 9/5 $103,899.76 0.380944 | 0x0c..1b1c 9/19 $1,052.35 0.028062 | 0x10..4890 9/7 none.
Referee volumes on 9/27 (referrals.csv): A3 $35,823,804.21; 0x52 $5,224,966.56; 0x88 $1,830,420.96; 0xaF $103,899.76; 0x0c $1,052.35; 0x35 $506.99; 0x01 $399.03.
Referee volumes on 9/18: A3 $28,408,753.47; 0xaF $103,899.76.
A3's own points: 1967.074763 (9/19T03:03Z), 2797.108974 (9/26T10:40Z), 2843.574565 (9/28T03:42Z), 6210.2163 on 10/19 (leaderboard #10 "0xc2..."; 620.968663 = 10.000% of it,
confirming A1's cut is 10% of A3's total points). A3 equity $2,868,729.64 on 9/28 (~12.5x gross in September). A3 did $60.02M volume between 9/27 and 10/19.
A2 has no referees and no referral income.

## Position snapshots at the export session (~2026-10-19T06:06Z), signed size, value, mark
A1: QQQ short 3086.9744 ($2,223,640.26, 720.33) | SPY long 2759.6577 ($2,123,556.60, 769.50) | BTC long 12.50748 ($1,034,918.92, 82,744.0) |
    ETH short 210.4154 ($511,593.48, 2,431.35) | HYPE long 2740.522 ($218,970.44, 79.901) | SOL long 200.034 ($21,144.19, 105.703) |
    SUI long 10101.1 ($10,201.30, 1.00992) | OPENAI long 2.0435 ($3,201.32, 1,566.59). Gross ~$6.147M.
A2: BTC long 0.63102 ($52,220.06, 82,755.0) | QQQ short 69.3924 ($49,986.81, 720.35) | SPY long 64.8055 ($49,871.72, 769.56) | ETH short 20.4222 ($49,667.40, 2,432.03) |
    ZEC long 23.4307 ($27,254.12, 1,163.18) | ANTHROPIC long 5.12698 ($10,256.52, 2,000.5). Gross ~$239.3K.
September (9/19) snapshots are in positions_snapshots.csv. Continuity: the backward walk from the 10/19 snapshot through the month export must land on the forward walk
from the 9/19 snapshot (A1: BTC 9.30748, ETH -255.2558, QQQ -3000.661, SPY 2804.4795 at 9/27T08:11Z were already confirmed with the earlier partial export). Verify it.

## Previously observed anomaly (with the range-limited export; re-test with the full data)
Under the September rates, A1's own points in 9/28->10/19 were predicted within ~5% but A2's were under-predicted by ~60%
(A2 earned ~1.5-2x more per $ of OI and per $ of volume than A1). Hypotheses: (1) unrecorded gap trades — now resolved: the month export shows
A1 traded $218K and A2 $83K between 9/27 and 10/07; (2) sublinear (concave) scaling of the weekly drop in account size (the old Season-2 doc says "non-linear scaling");
(3) A1's referral income larger than assumed; (4) part of A1's activity ineligible (fills vs Standard accounts, or A1/A3 counterparty matches).

## Platform context
Leaderboard top-10 on 10/19: 45422.49, 45104.32, 29564.32, 14917.45, 13743.35, 12532.71, 10003.67, 7736.70, 6651.23, 6210.22 (rank 79 = 1623.0, rank 1634 = 74.69).
On 9/19 03:00Z: 27484.7, 23389.4, 19469.2, 10786.9, 7330.1, 4880.6, 4792.2, 4188.5, 3915.7, 3751.7 (rank 115 = 566, rank 1643 = 29.7). On 9/28: top-1 32571.0, top-2 31162.6, rank 91-92 = 825-832, rank 1466-1472 = 47.5-47.7.
Fees: Premium taker 3.5bp / maker 1.2bp displayed on 10/19. LIT price assumption for $/pt: $3.5 (owner), pool 11M LIT.

## Addendum 2026-10-19 08:10Z — market-weight hypothesis (to be tested)
- A friend of the owner reports earning ~35.5 pt per $1M of OI per week on Lighter (details of his book unknown; presumably crypto perps). Our market-independent fit gives 19.4 (pooled) or 15.9 (A1-only, September split). The gap must be explained or refuted.
- An adversarial verifier in the previous run found that a single MARKET-WEIGHTED linear rule fits all four A1/A2 account-windows within 2.6% (<= 9.9% in every grid cell): drop volume ~28.5 pt/$M, drop OI crypto ~33 pt/$M-wk, drop OI stock ~4 pt/$M-wk (live 5.5 common). Codex's September two-sector live fit was stock 4.76 vs other 6.54 pt/$M-wk.
- Sector shares of gross OI: A1 10/19 snapshot: stock (QQQ+SPY+OPENAI) $4.35M of $6.15M = 71%, crypto (BTC, ETH, HYPE, SOL, SUI) 29%; A1 9/19: stock 78%; A2 10/19: stock (QQQ+SPY+ANTHROPIC) $110K of $239K = 46%; A2 9/19: 48%.
- Stock-like markets on Lighter: SPY, QQQ, ANTHROPIC, OPENAI (pre-IPO); commodity: XAG; everything else crypto (BTC, ETH, HYPE, SOL, SUI, ZEC, NEAR, LIT, CASHCAT ...).
- Direct drop observations (points_snapshots.csv): 9/18 drop A1 +308.46 (14:31Z -> 15:32Z, includes 10% of A3's drop), A2 +15.80 (14:31Z -> 15:33Z). A3 September export (handoff/data/lighter/a3_lighter_export_2026-09-27T10_40Z.csv.gz) allows A3's OI by sector for W1 (A3 points 1967.07 at 9/19 03:03Z -> 2843.57 at 9/28 03:42Z; no referral income; A3's book: SPY short / QQQ long / BTC short / ETH long plus crypto taker flow).
- Owner confirmed (10/19): the 10/16 drop landed early 10/17 KST (points jumped), so W2 contains exactly three drops (10/02, 10/09, 10/16) and W1 one (9/25).

## Addendum 3 (10/19 08:20Z) — clean crypto-OI anchor: idle ETH-only account ("A5")
- Owner reports a separate account that opened ETH long 0.8181 at 9/2 18:00 (KST, i.e. 09:00Z; if UTC the difference is 9 h, immaterial) and did NOTHING else; its total points on 10/19 are 0.466881. No referrals, no other trades.
- ETH notional ~$1,950 at open (ETH ~2,389) and ~$1,940 at 10/19 (ETH ~2,370); ETH averaged ~$2,553 over the period (6-hourly VWAP of A1/A3 ETH fills, 53,347 fills 9/2..10/18). OI-time 9/2 09:00Z -> 10/19 08:15Z = 0.01402 $M-wk (6.71 weeks, avg notional ~$2,089). Seven Friday drops inside (9/4, 9/11, 9/18, 9/25, 10/2, 10/9, 10/16); drop activity OI-time up to the 10/16 15:00Z cutoff = 0.01325 $M-wk.
- Implied all-in crypto OI rate = 0.466881 / 0.01402 = 33.3 pt per $M-week (lifetime average over Sept+Oct). With live 5.5: drop-OI coefficient 29.4 if the opening $2K fill earned nothing, 24.6 if it earned 33 pt/$M -> crypto OI total 30-35 pt/$M-wk. This is independent of referral, sector mix and volume, and matches the friend's ~35.5 and the sector-weighted fits (crypto >> stock). It also argues against a small-account premium (this $2K account earns the same crypto rate as the sector fit gives large accounts).
- MODEL/VERIFY agents: use this as a hard anchor for the crypto OI rate (with the ETH price path uncertainty +-3%).
- Owner's standing caveat (10/19): the weekly drop rates may change from week to week. Treat every fitted rate as a window AVERAGE (W1 = one drop, W2 = three drops averaged, the idle ETH account = seven drops averaged) and say explicitly which weeks each number averages over; do not present a single rate as a constant of the program. Where observations conflict (e.g. the idle ETH account's 7-week average 33.3 vs A2's October), consider a September-vs-October rate change as a candidate explanation and quantify what per-week rates would reconcile them.
