# Reproduce the handoff analysis

From the repository root:

```bash
python3 -m pip install -r handoff/analysis/requirements.txt
python3 handoff/analysis/run_all.py
```

The analysis itself uses no network, downloads, credentials, exchange APIs, or
files outside `handoff/data/lighter/`. Installation is unnecessary when pandas
and numpy are already available. Tested with Python 3.12.14, pandas 2.2.3 and
numpy 2.3.5. Scripts resolve paths relative to themselves, not the shell's cwd.

`run_all.py` rewrites the generated CSV/JSON/Markdown files and `../RESULTS.md`.
It preserves raw inputs and records their SHA-256 digests in `input_manifest.json`.

## Calculation flow

- `core.py`: normalized raw trades, same-second aggregation, signed positions,
  all-account last-observed-price proxies plus supplied marks, exact event-time
  integration. Continuous mark prices are not available.
- `model.py`: trade-free live evidence, the HYPE credit estimate, a conditional
  two-factor weekly-drop fit, sensitivity grid, all snapshot windows and lifetime
  reconciliation. Baseline live 5.5 and sensitivity bands are explicit modeling
  assumptions, not fitted confidence intervals. The base drop fit fails to fully
  explain A1 and lifetime totals; residuals remain visible.
- `costs.py`: raw Fee and Trade Value sums, role and fee-regime breakdowns,
  aligned snapshot windows and assumed spread numerators. The Fee-only ratios
  compare period cash fees with delivered points, not matched activity cohorts.
- `counterparty_audit.py`: cross-account Trade ID and exact execution-field
  checks. Matched executions do not establish intent, common control or any
  points penalty. Same-second opposite flow alone is not a counterparty match.
- `platform_points.py`: cumulative rank constraints, monotonic total lower
  bounds, separately labeled tail models and growth lower bounds.
- `independent_audit.py`: alternative, independently coded own-fill-price OI
  reconstruction and model-form checks. It intentionally uses a different
  price proxy from `core.py`, so coefficient estimates need not be identical.
- `economics.py`: explicitly assumed referral deductions, spread sensitivity,
  APR cases for both hedge structures and entry/exit cost illustrations.
- `tvl_sensitivity.py`: follow-up alternative volume+equity drop model with the
  final equity snapshot held constant over historical weeks. This is explicitly
  a counterfactual sensitivity, not measured historical TVL or causal evidence.
- `report.py`: builds `../RESULTS.md` from the outputs above.
- `verify.py`: independent Decimal totals, raw snapshot coverage, reconstructed
  position checks, requested APR scenario coverage and strict JSON validation.

All time intervals use `[start,end)`, UTC. Anything after an account's export
end is an extrapolation, even if that account's last observed trade is earlier.
The A1 referral subtraction also depends on A3's separate export coverage.
One-way reported Trade Value is summed once per fill; a completed round trip
therefore contains both its opening and closing notionals.

## Main outputs

- `window_decomposition.csv` and `lifetime_reconciliation.csv`: all snapshot
  intervals and cumulative predictions, including unexplained residuals.
- `drop_fit.csv`, `drop_sensitivity.csv`, `live_evidence.csv`,
  `weekly_activity.csv`: point model inputs and conditional estimates.
- `cost_per_point.csv`: observed-period cost ratios with explicit spread and
  other-referral assumptions. Only A2 and A3 own-point denominators are observed.
- `apr_scenarios.csv`: all 72 requested price/end-points/leverage/structure
  combinations for each of five reward-rate cases.
- `platform_summary.json` and `platform_*.csv`: bounds and tail/growth scenarios.
- `verification.json`: data/output checks; it does not assert model truth.

The supplied public-rule descriptions are recorded assumptions. The optional
live documentation review is described in RESULTS and is not a dependency of
any numeric calculation. No exchange data is fetched.
