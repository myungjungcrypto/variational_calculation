# Prior results (from a previous analyst) — to verify, not to trust

All numbers below are estimates produced from the same data in `data/`. Treat as hypotheses.

## Lighter (Robinhood Chain) model
```
live      = 5.5 pt × gross OI ($M) / week          [3 accounts: 5.1 / 5.4–6.0 / 5.1; linear $0.23M–$36M; market-independent]
rt_credit = 4.6 pt × volume ($M)                    [booked at fill time; from A2's accidental $15K trade + A3 windows]
drop      = 45–50 pt × weekly volume ($M)           [A2 two weeks: 50.1, 45.0]
          + ~10 pt × weekly OI ($M-week)            [A3 only, range 8–11; A1 uninformative due to referral noise]
referral  = 10% of referees' (live + rt + drop)     [A1 live rate drops 8.2 → 5.4 after removing 10% of A3's live]
```
- OI per $1M held one week ≈ 15.5 pt total (5.5 live + ~10 in drop).
- Realized cost per point (fees ÷ own points, 9/19→9/27 window): A1 $3.5, A2 $2.35, A3 $2.64.
- Taker $1M volume: fee ~3.41bp + spread ~0.2–1bp ≈ $350–440 → ~55–58 pt → ~$6–8/pt (without OI).
- Spread vs Binance ticks (separate tick analysis): A1 majors ≈ +0.2 bp net, A2 ≈ −0.6 bp (maker fills), stock perps
  trade ~+10 bp premium to the ETF but the SPY-long/QQQ-short pair cancels it.
- Platform total points ≈ 0.5M on 2026-09-19 (power-law fit to leaderboard: top-10 = 110K, rank 115 = 566, rank 1643 = 29.7),
  growing ~100–150K/week (top-2 grew +18%/+33% in 9 days).
- $/point = 11M LIT × price ÷ total points at program end (end date unknown).

## APR table (OI 15.5 pt/$M-week, funding = 0, hedge inside Lighter; halve for one leg on another venue)
| leverage | end 1.5M pts (7.33 LIT/pt) | 2.0M (5.5) | 3.6M (3.1) | 5.2M (2.1) |   (LIT $5)
| 10x | 30% | 22% | 12% | 9% |
| 15x | 44% | 33% | 19% | 13% |
| 20x | 59% | 44% | 25% | 17% |

## Known weaknesses of the prior analysis
- Only 3 accounts; volume and OI are correlated across them, so the (volume, OI) split of the drop is ill-conditioned.
- A1's referral income in the 9/19–9/27 window is uncertain by ±40 pt (new referees' timing unknown).
- OI over time is reconstructed from fill prices (not mark prices); ±5–10%.
- A3's screenshot timing has ~±30 min uncertainty; A1/A2 first-day snapshot ±10 min.
