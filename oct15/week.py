"""Variational week 2026-10-08 -> 10-15: OI-time (swap x2), volume, est. spread cost, coefficient fit."""
import sys, json
import pandas as pd, numpy as np

DATA = sys.argv[1] if len(sys.argv) > 1 else "data"
W0, W1 = pd.Timestamp("2026-10-08T00:00:00Z"), pd.Timestamp("2026-10-15T00:00:00Z")
SNAP_T = pd.Timestamp("2026-10-16T05:00:00Z")   # screenshot time (next-drop countdown 6d19h01m before Fri 10/23 00:00Z)

# half-spread bps used for cost (measured 10/1-10/8 where available, else prior/assumed)
BPS = {"BTC":0.61,"ETH":1.33,"HYPE":3.4,"XAU":1.5,"QQQ":1.5,"US500":1.5,"SOL":1.8,"BNB":2.7,"ZEC":6.0,
       "ENA":8.1,"NEAR":6.4,"ONDO":8.7,"PONS":12.0,"USELESS":8.5,"ZRO":7.8,"CASHCAT":8.0,"OPENAI":10.0,
       "LIGHTER":5.0,"AAPL":4.5,"CRWD":4.5,"HOOD":4.5,"PUMPFUN":8.0}
DEFAULT_BPS = 8.0

# screenshot positions at SNAP_T: (underlying, instrument_type) -> (signed qty, mark)
SNAP = {
 "acct1": {("BTC","perpetual_future"):(-27.045073,82011.01), ("US500","perpetual_rwa_future"):(-2555.7934,773.695),
           ("QQQ","perpetual_rwa_future"):(2652.7277,730.378), ("ETH","perpetual_future"):(771.05239,2439.89),
           ("XAU","swap"):(156.68251,4172.70), ("XAU","perpetual_rwa_future"):(-119.82864,4174.81),
           ("SOL","perpetual_future"):(1996.9025,105.427), ("HYPE","perpetual_future"):(1500.604,80.8193),
           ("BNB","perpetual_future"):(70.0015,728.510), ("ZEC","perpetual_future"):(30.34587,1210.22),
           ("CASHCAT","perpetual_future"):(114862.8,0.1201), ("PONS","perpetual_future"):(31839.1,0.3356),
           ("OPENAI","perpetual_rwa_future"):(6.50818,1526.27), ("USELESS","perpetual_future"):(55968.5,0.1744)},
 "acct2": {("BTC","perpetual_future"):(-1.977422,82024.69), ("QQQ","perpetual_rwa_future"):(211.6094,730.408),
           ("US500","perpetual_rwa_future"):(-197.756,773.705), ("ETH","perpetual_future"):(62.48028,2440.56),
           ("XAU","swap"):(12.09861,4172.95), ("XAU","perpetual_rwa_future"):(-4.27614,4175.21)},
 "acct3": None,   # no snapshot; account started flat before 2026-09-17 -> forward cumsum
}
POINTS = {"acct1":181.443115, "acct2":11.620779, "acct3":7.900982}

def load(path):
    t = pd.read_csv(path); t = t[t.status=="confirmed"].copy()
    t["created_at"] = pd.to_datetime(t.created_at, utc=True, format="ISO8601")
    t["price"]=t.price.astype(float); t["qty"]=t.qty.astype(float); t["notional"]=t.price*t.qty
    t["signed"]=np.where(t.side=="buy",t.qty,-t.qty)
    return t.sort_values("created_at").reset_index(drop=True)

rows=[]
for name in ["acct1","acct2","acct3"]:
    t = load(f"{DATA}/{name}.csv"); snap = SNAP[name]
    grid = pd.date_range(W0, W1, freq="15min", inclusive="left")
    keys = set(zip(t.underlying, t.instrument_type)) | (set(snap) if snap else set())
    oi_raw = np.zeros(len(grid)); oi_w = np.zeros(len(grid)); per_mkt = {}
    for (u,it) in keys:
        sub = t[(t.underlying==u)&(t.instrument_type==it)]
        if snap is not None:
            q_now, mark = snap.get((u,it),(0.0,np.nan))
            q = np.array([q_now - sub[(sub.created_at>ts)&(sub.created_at<=SNAP_T)].signed.sum() for ts in grid])
        else:
            mark = np.nan
            q = np.array([sub[sub.created_at<=ts].signed.sum() for ts in grid])
        px = np.array([ (sub[sub.created_at<=ts].price.iloc[-1] if (sub.created_at<=ts).any()
                         else (mark if not np.isnan(mark) else (sub.price.iloc[0] if len(sub) else 0.0))) for ts in grid], dtype=float)
        v = np.abs(q)*px; w = 2.0 if it=="swap" else 1.0
        oi_raw += v; oi_w += w*v; per_mkt[f"{u}/{it}"] = round(float((w*v).mean()),0)
    wk = t[(t.created_at>=W0)&(t.created_at<W1)]
    cost = float(sum(r.notional*BPS.get(r.underlying,DEFAULT_BPS)/1e4 for r in wk.itertuples()))
    vol_by = {k: round(float(v),0) for k,v in wk.groupby("underlying").notional.sum().sort_values(ascending=False).items()}
    rows.append(dict(acct=name, points=POINTS[name], fills_week=len(wk), volume=round(float(wk.notional.sum()),0), cost=round(cost,1),
                     oi_raw_avg=round(float(oi_raw.mean()),0), oi_w_avg=round(float(oi_w.mean()),0),
                     oi_w_start=round(float(oi_w[0]),0), oi_w_end=round(float(oi_w[-1]),0),
                     oitime_w=float(oi_w.mean())/1e6, vol_by=vol_by, oi_by_mkt=per_mkt))
df = pd.DataFrame(rows)
for r in rows:
    print(f"=== {r['acct']} ===  points {r['points']}  week fills {r['fills_week']}  volume ${r['volume']:,.0f}  est cost ${r['cost']:,.1f}")
    print(f"  weighted OI avg ${r['oi_w_avg']:,.0f} (start ${r['oi_w_start']:,.0f} -> end ${r['oi_w_end']:,.0f}) | raw avg ${r['oi_raw_avg']:,.0f}")
    print("  volume by market:", r["vol_by"]); print("  weighted OI by market (avg):", r["oi_by_mkt"])

# fits
X = np.array([[r["cost"], r["oitime_w"]] for r in rows]); y = np.array([r["points"] for r in rows])
a,b = np.linalg.lstsq(X,y,rcond=None)[0]; pred = X@[a,b]
print(f"\njoint LS: a={a:.4f} pt/$cost  b={b:.2f} pt/$M-wk  pred {pred.round(2)} vs {y}")
for bb in (12.0,13.5,15.0):
    print(f"  b fixed {bb}: implied a per account =", {r['acct']: round((r['points']-bb*r['oitime_w'])/r['cost'],4) if r['cost']>0 else None for r in rows})
for aa in (0.05,0.077,0.10):
    print(f"  a fixed {aa}: implied b per account =", {r['acct']: round((r['points']-aa*r['cost'])/r['oitime_w'],2) for r in rows})
json.dump(rows, open("week_summary.json","w"), indent=1, default=float)
