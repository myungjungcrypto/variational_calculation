# Fee and spread audit

The Fee column supplies explicit transaction fees. Fee sums are exact to the CSV's $0.0001 precision. Reported Trade Value sums are exact to $0.000001. All intervals use [start, end), UTC.

| Account | Lifetime volume | Lifetime fees | Aligned-window volume | Aligned-window fees | Aligned-window fee bp | Maker share of window volume |
|---|---:|---:|---:|---:|---:|---:|
| A1 | $8,740,448.623934 | $1,959.7979 | $2,120,128.266643 | $593.8488 | 2.8010 | 27.1869% |
| A2 | $907,174.681769 | $96.0228 | $313,895.564711 | $42.0631 | 1.3400 | 93.9119% |
| A3 | $35,822,364.832859 | $11,784.0150 | $6,778,124.408248 | $2,186.1189 | 3.2253 | 0.0000% |

Aligned windows:
- A1: 2026-09-19T02:35:00Z through 2026-09-27T15:00:00Z.
- A2: 2026-09-19T02:42:00Z through 2026-09-27T15:00:00Z.
- A3: 2026-09-19T03:59:00Z through 2026-09-26T10:40:00Z.

A3's export is dated 24 hours after its points snapshot. Its additional $635,486.952573 volume and $200.1340 fees after September 26 10:40 UTC must not enter the numerator for the September 26 points denominator. The matched September 19–26 Fee-only cost divided by the observed 829.229098-point increase is $2.636327/point. A2's matched fee-only cost divided by 17.871098 points is $2.353694/point. A1 requires a referral subtraction before a fee-per-own-point value can be identified.

## Fee regimes

- A1: first positive fee at 2026-09-11T18:22:27+00:00; last preceding zero-fee fill at 2026-09-11T09:09:35+00:00. Later 44 zero-fee fills total only $3.728733, consistent with fee-display rounding.
- A2: first positive fee at 2026-09-12T03:11:33+00:00; last preceding zero-fee fill at 2026-09-12T03:11:03+00:00. Later 4 zero-fee fills total only $0.978348, consistent with fee-display rounding.
- A3: first positive fee at 2026-09-03T11:35:55+00:00; last preceding zero-fee fill at 2026-09-03T11:35:53+00:00. Later 134 zero-fee fills total only $13.591587, consistent with fee-display rounding.

The pre/post-first-positive-fee split is an observed charging-regime proxy, not proof of exact account-upgrade times. It supports the Standard-to-Premium change for A1/A2. A3 has only a brief initial zero-fee sequence; its account type in that interval is not supplied. Tier changes, small-fill rounding, and maker/taker mix mean the realized fee rate should be read from the export rather than imposed as 3.41 bp for all accounts.

## Spread identification

The supplied package contains no quote, midpoint, arrival-price, or external reference tick history. Spread cost is therefore not identified. Closed PnL mixes execution costs with market moves and cannot replace a spread measurement. The earlier analyst's Binance-based +0.2/-0.6 bp claims cannot be reproduced from these inputs.

costs_same_second_gaps.csv is only a diagnostic of A1/A3 opposite-direction same-second VWAP gaps. Its samples have unknown subsecond ordering and changing prices and are selected by both accounts' execution behavior. They cannot establish a midpoint, each account's cost, or the cost of non-overlapping fills, so they are not inserted in cost-per-point estimates.

For a clearly labeled scenario, use cost/own-point = (Fee + volume × s / 10,000) / own_points, where s is signed execution shortfall in basis points per dollar of reported one-way notional. Negative s represents favorable execution/maker spread capture. The [-0.5, 0, 0.2, 0.5, 1] bp rows are sensitivity cases, not a data-driven confidence range. Each extra 1 bp costs $100 per $1M traded. Funding and price/hedge PnL are outside this transaction-cost definition.

Future fees to close existing positions are not in realized fees. Observed-window cost-per-point also compares cash fees with point deliveries in that period: the weekly drop may reward trading before the first snapshot. It is a realized window ratio, not a marginal causal farming cost.
