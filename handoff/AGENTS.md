# AGENTS.md

Start by reading `TASK.md` in this directory, then `data/lighter/program_facts.md` and `prior_results.md`.
All raw inputs are under `data/`. Write your outputs to `RESULTS.md` and `analysis/` inside this directory.
Python 3 + pandas/numpy is sufficient; no network access is required (do not try to fetch exchange data).
Timestamps in the trade exports are UTC at 1-second resolution; the `.gz` file is plain gzip of a CSV.
