# Lighter on Robinhood Chain — program facts (as gathered through 2026-09-28)

## Official / public
- Program live since 2026-08-10. Points accrue **in real time** ("live points") plus a **weekly drop every Friday**
  (first drop 2026-08-21). Observed drop time: ~15:30 UTC Friday (00:30 KST Saturday).
- Pool: **11,000,000 LIT** committed to the Robinhood community (token-denominated; press quoted $11M/$20M/$25M at
  different LIT prices). LIT ≈ $5 on 2026-09-19. Points convert to LIT "subject to Lighter's terms";
  **conversion ratio, weekly LIT budget, total points cap and end date are NOT published.**
- 2x points when trading via Robinhood Wallet app (isolated margin only); 1x via web app. **All three accounts here use the web app (1x).**
- Referral: referrer receives **10% of referees' trading points**.
- Formula is undisclosed. The main-platform Season-2 doc (ended) said: weekly fixed pool; categories = volume, OI,
  funding, liquidations, PnL; markets weighted differently; non-linear scaling; premium account may affect weights.
- T&C excludes wash trading, self-trading / commonly-controlled-account trading, artificial volume, coordinated
  manipulation. Sybil detection is applied; up to 10 accounts per user tolerated on main platform.
- Docs: https://docs.lighter.xyz/points-program/lighter-on-robinhood-chain-points ,
  https://docs.lighter.xyz/points-program/retail (old Season 2), https://docs.lighter.xyz/trading/trading-fees

## Account facts (from the account owners)
- A1, A2, A3 are operated by different people on different IPs. A3 was referred by A1 (A3 = referee 0xc2...9664).
  A2 joined with code YOURDICK (not A1's). A1 joined with code 21691AJN. **A3 has no referees. A2 has no referees.**
- All three are **Premium** accounts (fees apply). A1 and A2 were on Standard (zero fees) until ~2026-09-11 and then upgraded.
- Owner's understanding: fills where BOTH counterparties are Standard accounts do not earn points.
- Nobody uses Robinhood Wallet (no 2x).
- Points display shows 6 decimals and updates live; "TOTAL POINTS" on the leaderboard page is a plain cumulative number.
- Weekly drop of 2026-09-18 15:30Z landed between the 14:31Z and 15:32Z snapshots for A1 and A2.
- Between 2026-09-19 and 2026-09-27, A1 traded (was NOT a zero-volume control). A2 traded until 2026-09-23 only.
- A3 export timestamps are UTC, 1-second resolution, side labels Open Long / Close Long / Open Short / Close Short.

## Strategy facts
- A1: SPY long / QQQ short + BTC long / ETH short pairs, ~20x gross OI to equity, small 30-second clips.
- A3: mirror image of A1 (SPY short / QQQ long / BTC short / ETH long) plus ETH/NEAR/SOL/ZEC/LIT taker flow; ~12.5x.
- A2: HYPE / ZEC two-sided with ~20% maker fills; small.
