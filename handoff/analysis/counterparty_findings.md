# A1/A3 execution-overlap audit

This is a local raw-export audit. It does not determine intent, common ownership, rule violations or point penalties.

There are 6 shared Trade IDs. All also match exact UTC second, market, price, size and Trade Value, with opposite trade directions and A1 Maker / A3 Taker roles. This strongly supports the two exports describing opposite sides of the same six executions. Exchange-wide Trade ID semantics were not independently verified; the exports do not contain counterparty addresses.

Single-sided matched volume is $2,378.261338, or 0.027210% of A1 lifetime volume and 0.006639% of A3 lifetime volume. Adding both exports gives $4,756.522676, which double-counts these executions. All six are BTC/ETH/ZEC on September 22–23. There are no shared IDs in SPY/QQQ.

| Trade ID | UTC | Market | Price | Size | USD notional | A1 | A3 |
|---|---|---|---:|---:|---:|---|---|
| 934542679 | 2026-09-22 22:40:50 | ETH | 2755.460000 | 0.336700 | 927.763382 | Open Short / Maker | Open Long / Taker |
| 934593561 | 2026-09-22 22:43:31 | BTC | 86197.900000 | 0.003120 | 268.937448 | Open Long / Maker | Open Short / Taker |
| 934843597 | 2026-09-22 22:55:20 | ETH | 2757.190000 | 0.336700 | 928.345873 | Open Short / Maker | Open Long / Taker |
| 934845974 | 2026-09-22 22:55:32 | BTC | 86137.100000 | 0.002030 | 174.858313 | Open Long / Maker | Open Short / Taker |
| 938379000 | 2026-09-23 01:20:48 | ZEC | 1612.270000 | 0.024300 | 39.178161 | Close Short / Maker | Open Short / Taker |
| 938382334 | 2026-09-23 01:20:56 | ZEC | 1612.270000 | 0.024300 | 39.178161 | Close Short / Maker | Open Short / Taker |

An independent exact (timestamp, market, price, size, notional) opposite-side join finds precisely these six pairs and no differently numbered pairs.

The same-second opposite-side/role aggregation finds 844 SPY and 538 QQQ groups. All are Taker/Taker, unlike the six complementary Maker/Taker records. Under ordinary matching semantics both takers are taking different resting orders, so synchronized opposite flow alone does not show that these accounts traded with each other. Group counts are not one-to-one transaction counts.

A small number of direct matches is compatible with incidental matching in a shared public order book. The data does not distinguish incidental matching from intentional coordination. Nor does it verify the owners' identity/IP statements; those are supplied account facts. Different people/IPs do not reveal what the exchange's eligibility or enforcement system decided.

No point-eligibility flags, penalty notices, counterparty addresses, order submission history or exact reward formula are present. Existing model residuals can arise from market/account weights, unknown referral receipts, mark approximation, timing, eligibility or a changing reward rule. They cannot independently establish a penalty or cross-account OI netting.

Reproduce with `python3 handoff/analysis/counterparty_audit.py`. Matching CSVs preserve the original execution fields for review.
