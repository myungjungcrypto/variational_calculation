"""Variational week 2026-10-08 -> 10-15: back out the OI coefficient b given the candidate spread formula

    spread_pts = 0.90 * SUM_fills( (s_bp - 0.34)/1e4 * notional - $0.032 ) / $20

where s_bp is the half-spread the fill paid. Two spread sources are used:
  (1) quoted  : half-spreads read off the order panel on 10/16 (FACTS2.md)
  (2) measured: if slippage_tick_per_trade_<acct>.csv exists in the cwd (output of
                `python slippage_tick.py oct15/data/<acct>.csv`), the per-market midpoint
                ((buy slip + sell slip)/2 vs Binance tick, notional-weighted) replaces the quote.
Then b = (points - spread_pts) / OI_time for swap-volume weight 1x / 2x and swap-OI weight 1x / 2x.

Run from the repo root:  python oct15/week.py oct15/data   (writes week_summary.json)
                         python oct15/formula.py oct15/data
"""
import sys, json, subprocess
from pathlib import Path
import pandas as pd, numpy as np

DATA = Path(sys.argv[1] if len(sys.argv) > 1 else "oct15/data")
HERE = Path(__file__).resolve().parent
W0, W1 = pd.Timestamp("2026-10-08T00:00:00Z"), pd.Timestamp("2026-10-15T00:00:00Z")
A, S0, F, P = 0.90, 0.34, 0.032, 20.0          # candidate formula constants
POINTS = {"acct1": 181.443115, "acct2": 11.620779, "acct3": 7.900982}

QUOTED = {"BTC":0.53, "ETH":0.49, ("XAU","swap"):0.50, ("XAU","perpetual_rwa_future"):1.66, "QQQ":2.47, "US500":2.58,
          "HYPE":3.0, "ENA":7.4, "SOL":0.8, "BNB":1.0, "XAG":2.5, "ZEC":3.0, "NEAR":6.0, "PONS":10.0, "LIGHTER":5.0,
          "WLD":5.0, "ONDO":8.0, "USELESS":8.0, "OPENAI":10.0, "CASHCAT":8.0}
DEFAULT_S = 6.0

def quoted(u, it): return QUOTED.get((u, it), QUOTED.get(u, DEFAULT_S))

def load_week(path):
    t = pd.read_csv(path); t = t[t.status == "confirmed"].copy()
    t["created_at"] = pd.to_datetime(t.created_at, utc=True, format="ISO8601")
    t["notional"] = t.price.astype(float) * t.qty.astype(float)
    return t[(t.created_at >= W0) & (t.created_at < W1)].copy()

def measured_spreads(name):
    """Per (underlying, instrument_type) midpoint half-spread from a tick per-trade file, if present."""
    f = Path(f"slippage_tick_per_trade_{name}.csv")
    if not f.exists():
        return None
    d = pd.read_csv(f)
    d["created_at"] = pd.to_datetime(d.created_at, utc=True, format="ISO8601")
    d = d[(d.created_at >= W0) & (d.created_at < W1) & d.matched.astype(bool) & d.slip_bps.notna()]
    out = {}
    for (u, it), g in d.groupby(["underlying", "instrument_type"]):
        sides = {}
        for side, gg in g.groupby("side"):
            sides[side] = float((gg.slip_bps * gg.notional).sum() / gg.notional.sum())
        if "buy" in sides and "sell" in sides:
            out[(u, it)] = (sides["buy"] + sides["sell"]) / 2.0
    return out

def spread_points(t, s_col):
    net = (t[s_col] - S0) / 1e4 * t.notional - F
    return float((A * net / P).sum()), float((A * net * t.w / P).sum()), float(((A * net / P).clip(lower=0)).sum())

if not Path("week_summary.json").exists():
    subprocess.run([sys.executable, str(HERE / "week.py"), str(DATA)], check=True)
summ = {r["acct"]: r for r in json.load(open("week_summary.json"))}

rows = []
for name in ["acct1", "acct2", "acct3"]:
    t = load_week(DATA / f"{name}.csv")
    t["w"] = np.where(t.instrument_type == "swap", 2.0, 1.0)
    t["s_q"] = [quoted(u, it) for u, it in zip(t.underlying, t.instrument_type)]
    meas = measured_spreads(name)
    if meas:
        t["s_m"] = [meas.get((u, it), quoted(u, it)) for u, it in zip(t.underlying, t.instrument_type)]
    r = summ[name]; oi2 = r["oitime_w"]; oi1 = r["oi_raw_avg"] / 1e6; pts = POINTS[name]
    print(f"=== {name} ===  points {pts:.3f} | fills {len(t)} | volume ${t.notional.sum():,.0f} | OI-time $M-wk swap1x {oi1:.4f} / swap2x {oi2:.4f}")
    for lbl, col in [("quoted", "s_q")] + ([("measured", "s_m")] if meas else []):
        sp1, sp2, spc = spread_points(t, col)
        disp = float((t.notional * t[col] / 1e4).sum())
        print(f"  [{lbl}] displayed-spread $ {disp:,.1f} | formula spread pts: swapvol1x {sp1:.3f} swapvol2x {sp2:.3f} (clipped {spc:.3f})")
        print(f"           implied b: vol1x/OI2x {(pts-sp1)/oi2:.2f} | vol2x/OI2x {(pts-sp2)/oi2:.2f} | vol1x/OI1x {(pts-sp1)/oi1:.2f} | vol2x/OI1x {(pts-sp2)/oi1:.2f}"
              f" | linear a=0.055,b=14.5 pred {0.055*disp + 14.5*oi2:.2f}")
        rows.append(dict(acct=name, source=lbl, points=pts, displayed_cost=disp, sp_vol1=sp1, sp_vol2=sp2, oi1=oi1, oi2=oi2,
                         b_vol1_oi2=(pts-sp1)/oi2, b_vol2_oi2=(pts-sp2)/oi2, b_vol1_oi1=(pts-sp1)/oi1, b_vol2_oi1=(pts-sp2)/oi1))
    by = t.assign(cost_q=t.notional*t.s_q/1e4, pts_q=A*((t.s_q-S0)/1e4*t.notional-F)/P)
    cols = {"n": ("notional", "size"), "vol": ("notional", "sum"), "s_quoted": ("s_q", "first"), "cost_q": ("cost_q", "sum"), "pts_q": ("pts_q", "sum")}
    if meas:
        by = by.assign(pts_m=A*((t.s_m-S0)/1e4*t.notional-F)/P); cols.update(s_meas=("s_m", "first"), pts_m=("pts_m", "sum"))
    print(by.groupby(["underlying", "instrument_type"]).agg(**cols).sort_values("vol", ascending=False).round(3).to_string(), "\n")

print("anchor: acct3 week 10/1-10/8, zero trades, OI $207.6K -> 2.797 pts -> b =", round(2.797166 / 0.2076, 2))
print("\nacct4 (XAU swap long $17.09K + perp short $17.02K, ~3 fills per leg, actual 1.67 pts):")
for wv in (1, 2):
    sw, pp = 3 * 17089.86, 3 * 17017.69
    sp = A * ((((0.50 - S0) / 1e4) * sw - 3 * F) * wv + ((1.66 - S0) / 1e4) * pp - 3 * F) / P
    for b in (13.5, 15.0, 17.0):
        oi = (2 * 17089.86 + 17017.69) / 1e6
        print(f"  swapvol{wv}x b={b}: spread {sp:.3f} + OI(full week) {b*oi:.3f} = {sp + b*oi:.3f}")
print("\ncost per point by market under the formula ($, large fills):",
      {k if isinstance(k, str) else "/".join(k): round(P / A * s / (s - S0), 1) for k, s in QUOTED.items()})
pd.DataFrame(rows).to_csv("formula_summary.csv", index=False)
