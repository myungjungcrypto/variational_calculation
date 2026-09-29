# Independent model audit

This companion audit uses only supplied exports. It integrates market positions
at each account's own most recent fill VWAP, independently of the canonical
analysis's cross-account price marks. Consequently small coefficient differences
are expected. All reconstructed position sizes match the known September 19
position snapshots to floating-point precision. No external market data is used.

## Most reliable local observations

- A2's accidental $15,000 maker fill implies immediate credit 4.574204 pt/$M;
  replacing fill-price OI with known September 19 marks gives 4.574214.
  This is strong evidence for a local credit, but one fill cannot establish a
  universal rate or exact booking time between snapshots.
- A2's next, genuinely trade-free interval implies live 6.104363 pt/$M-week
  using last-fill prices, or 5.961011 using the known mark prices.
- The 56-minute A3 clean interval gives 5.129215 pt/$M-week at recorded times.
- A1's two clean postdrop windows imply common live 5.369451 and 5.392169 after
  adding 10% of A3's OI exposure. This supports referral sharing of live accrual
  conditional on a common own/referee coefficient; it does not independently
  identify 10% for every points component.
- The first 19-minute A1/A2 observations are compatible with an approximately
  ten-minute common timestamp error and should not determine the rate.
- A3's final window includes known $0.635487M trading. Removing immediate credit
  gives live 4.970472, conditional on
  no trades after its export cutoff. A1/A2/A3 final points snapshots all extend
  beyond their respective trade exports: export silence cannot certify no later
  trading.

## Drop alternatives and limitations

At Friday 15:00 UTC, assumed live 5.5 and immediate 4.574204, the A2 drop
volume-only slopes are 50.077714 (September 18)
and 45.075865 (September 25), versus A3's
100.035509 (September 25). A common constant linear volume-only
coefficient is inconsistent with these observations. However, the following
models all fit the September 25 A2/A3 observations exactly:

- Volume + OI: 35.956819 pt/$M volume plus
  12.590959 pt/$M-week OI. With that OI rate, September 18 A2
  requires a volume rate 40.631894.
- Nonlinear volume with zero OI: 61.015280 * V^1.272278, with V in $M.
- Separate volume weights with zero OI: other markets 45.075865, stock
  markets 1676.596334 pt/$M; the stock multiplier is 37.194990.
  This large multiplier is a mathematical counterexample, not evidence for it.

The pooled three-row linear V+OI fit is about 38.57 + 12.08 and misses the two
A2 drops by approximately 0.77 points. In particular, the prior 45–50 volume
slopes were obtained while omitting A2's own OI contribution; adding a positive
OI coefficient requires lowering those volume slopes. Midnight/noon/15:00
boundary choices materially move fitted coefficients; see sensitivity CSV.
Market weighting, superlinearity, weekly coefficient changes and an OI term
are not separately identified by two accounts on one common observed drop.

As an out-of-fit check, the same pooled model predicts A1's September 19–27
window as own 186.012481 plus A3 referral 86.677693, versus observed
257.031795. The residual is -15.658379 points
before adding any other-referee income. Positive omitted referral income only
widens the discrepancy. Thus the common four-component coefficients fail to
reconcile A1, even though they approximately fit the selected A2/A3 windows.
This failure cannot by itself distinguish eligibility, market weights, weekly
rate changes, point-credit lags, inaccurate valuation or a narrower referral
definition. These coefficients should not be described as a solved global rule.

## Market-independence and uncertainty

A two-sector clean-live fit gives stock 4.763036 and other markets
6.536161 pt/$M-week, with maximum relative residual
8.33%. A material market difference is compatible
with the stated 5–10% mark-price approximation and account-level confounding.
Near-linear live OI accrual is supported over a wide scale range, but neither
exact linearity nor market independence is established.

The ranges in this audit are sensitivity ranges, not statistical confidence
intervals. Independently uncertain ±30-minute A3 timestamps cannot identify a
narrow rate from a nominal 56-minute interval: the possible duration reaches
zero. Accurate relative timing or a shared clock-offset assumption is needed.

## Next measurements

1. A2 and A3: synchronized snapshots immediately before/after a full Friday drop,
   zero trades for that entire reward week, continuous exposure/mark logs.
2. One isolated eligible trade, snapshots immediately before/after, no position
   changes or unrelated trades nearby, repeated across several markets and sizes.
3. A1 and every referee: synchronized component totals around the same events
   to distinguish a 10% share from unknown other-referee activity.
4. Export trades through each last points snapshot; retain actual timestamp
   evidence rather than applying broad ±30-minute uncertainty to short windows.
