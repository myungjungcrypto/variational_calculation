"""Transparent sensitivity model, not a claim to have identified exchange rules."""
import json
from itertools import product
import numpy as np
import pandas as pd
from core import HERE, DATA, OI, load, snapshots, intervals, flow, ts, WEEK, EXPORT_END

LIVE = 5.5
LIVE_RANGE = (4.5, 6.9)


def drop_rows(trades, oi, windows, live=LIVE, rt=4.6, cutoff_hour=15):
    rows = []
    specs = [("A2", "2026-09-18", 7), ("A2", "2026-09-25", 10), ("A3", "2026-09-25", 13)]
    for account, day, i in specs:
        w = windows.iloc[i]
        end = ts(day) + pd.Timedelta(hours=cutoff_hour)
        start = end - pd.Timedelta(days=7)
        rows.append({"account": account, "week_end": end, "volume_m": flow(trades, account, start, end)["volume_m"],
                     "volume_stock_m": flow(trades, account, start, end, "stock")["volume_m"],
                     "oi_m_week": oi.integral(account, start, end),
                     "oi_stock_m_week": oi.integral(account, start, end, "stock"),
                     "drop_estimate": w.delta_points - live*w.oi_m_week - rt*w.volume_m,
                     "observed_delta": w.delta_points,
                     "window_oi_m_week": w.oi_m_week, "window_volume_m": w.volume_m})
    return pd.DataFrame(rows)


def run(trades=None, oi=None):
    trades = load() if trades is None else trades
    oi = OI(trades) if oi is None else oi
    w = intervals(trades, oi)
    w.to_csv(HERE/"windows_raw.csv",index=False)
    clean_a2, burst = w.iloc[9], w.iloc[8]
    local_a2 = clean_a2.delta_points / clean_a2.oi_m_week
    rt = (burst.delta_points - local_a2*burst.oi_m_week) / burst.volume_m
    ds = drop_rows(trades, oi, w, rt=rt)
    x = ds[["volume_m", "oi_m_week"]].to_numpy()
    y = ds.drop_estimate.to_numpy()
    coeff = np.linalg.lstsq(x, y, rcond=None)[0]
    ds["fit_drop"] = x @ coeff
    ds["residual"] = y - ds.fit_drop
    ds["volume_only_effective_rate"] = y / ds.volume_m
    a_volume = float(x[:, 0] @ y / (x[:, 0] @ x[:, 0]))
    ds["volume_only_pooled_residual"] = y - a_volume*x[:, 0]
    ds["prior_47_5_10_residual"] = y - 47.5*x[:, 0] - 10*x[:, 1]
    ds.to_csv(HERE/"drop_fit.csv", index=False)

    sensitivity = []
    for live, rate, scaling, hour in product(LIVE_RANGE+(LIVE,), (rt,4.1,5.3), (0.9,1.,1.1), (0,12,15,15.5)):
        d = drop_rows(trades, oi, w, live=live*scaling, rt=rate, cutoff_hour=hour)
        xx = d[["volume_m", "oi_m_week"]].to_numpy()
        xx[:,1] *= scaling
        fit = np.linalg.lstsq(xx, d.drop_estimate, rcond=None)[0]
        residual = d.drop_estimate.to_numpy() - xx@fit
        sensitivity.append({"live":live,"rt":rate,"oi_scale":scaling,"cutoff_hour":hour,
                            "volume_coeff":fit[0],"oi_coeff":fit[1],"max_abs_residual":abs(residual).max(),
                            "rmse":np.sqrt(np.mean(residual**2))})
    sens = pd.DataFrame(sensitivity)
    sens.to_csv(HERE/"drop_sensitivity.csv",index=False)

    # All snapshots: show inferred components and residuals, never force equality.
    weekly = []
    for account in trades:
        for day in pd.date_range("2026-08-14", "2026-10-02", freq="7D", tz="UTC"):
            end = day + pd.Timedelta(hours=15)
            start = end-pd.Timedelta(days=7)
            v = flow(trades, account, start, end)["volume_m"]
            integ = oi.integral(account,start,end)
            weekly.append({"account":account,"week_start":start,"week_end":end,
                           "paid_time":end+pd.Timedelta(minutes=30),"volume_m":v,"oi_m_week":integ,
                           "volume_stock_m":flow(trades,account,start,end,"stock")["volume_m"],
                           "oi_stock_m_week":oi.integral(account,start,end,"stock"),
                           "drop_model":coeff[0]*v+coeff[1]*integ,
                           "export_covers_end":end<=ts(EXPORT_END[account])})
    weeks = pd.DataFrame(weekly)
    weeks.to_csv(HERE/"weekly_activity.csv",index=False)
    def own_parts(account,start,end):
        d=weeks[(weeks.account==account)&(weeks.paid_time>=ts(start))&(weeks.paid_time<ts(end))]
        return LIVE*oi.integral(account,start,end),rt*flow(trades,account,start,end)["volume_m"],d.drop_model.sum()
    for i,row in w.iterrows():
        live,credit,drop=own_parts(row.account,row.start,row.end)
        ref=0.1*sum(own_parts("A3",row.start,row.end)) if row.account=="A1" else 0.
        w.loc[i,"live_model"]=live
        w.loc[i,"credit_model"]=credit
        w.loc[i,"drop_model"]=drop
        w.loc[i,"a3_referral_model"]=ref
        w.loc[i,"a3_export_covers_referral_end"]=row.end<=ts(EXPORT_END["A3"]) if row.account=="A1" else True
        w.loc[i,"unexplained_or_other_referral"]=row.delta_points-live-credit-drop-ref
    w.to_csv(HERE/"window_decomposition.csv",index=False)
    lifetime=[]
    for _,p in snapshots().iterrows():
        start="2026-08-10T00:00Z"
        l,r,d=own_parts(p.account,start,p.time)
        ref=0.1*sum(own_parts("A3",start,p.time)) if p.account=="A1" else 0.
        lifetime.append({"account":p.account,"time":p.time,"observed_points":p.points,"live_model":l,
                         "credit_model":r,"drop_model":d,"a3_referral_model":ref,
                         "residual":p.points-l-r-d-ref})
    pd.DataFrame(lifetime).to_csv(HERE/"lifetime_reconciliation.csv",index=False)

    # Internal consistency against the independent size/mark snapshots.
    check=[]
    for _,p in pd.read_csv(DATA/"positions_snapshots.csv").iterrows():
        actual=oi.point(p.account,p.timestamp_utc).get(p.market,{"size":0,"value":0})
        reported=p["size"]*(1 if p.side=="long" else -1)
        check.append({"account":p.account,"market":p.market,"timestamp":p.timestamp_utc,
                      "reconstructed_size":actual["size"],"reported_signed_size":reported,
                      "size_difference":actual["size"]-reported,"reported_value":p.value_usd,
                      "mark_size_value":abs(actual["size"])*p.mark_price})
    pd.DataFrame(check).to_csv(HERE/"position_validation.csv",index=False)
    assert max(abs(row["size_difference"]) for row in check)<1e-7
    summary={"live_center":LIVE,"live_sensitivity_range":LIVE_RANGE,"rt_local_a2":rt,
             "a2_clean_live":local_a2,"a3_clean_live":w.iloc[12].naive_live_rate,
             "a1_clean_common_live_with_10pct_a3":float(w.iloc[2:4].delta_points.sum()/(w.iloc[2:4].oi_m_week.sum()+.1*w.iloc[2:4].a3_oi_m_week.sum())),
             "drop_volume_coeff":float(coeff[0]),"drop_oi_coeff":float(coeff[1]),
             "drop_unscaled_condition_number":float(np.linalg.cond(x)),
             "drop_column_normalized_condition_number":float(np.linalg.cond(x/np.linalg.norm(x,axis=0))),
             "drop_sensitivity_volume_range":[float(sens.volume_coeff.min()),float(sens.volume_coeff.max())],
             "drop_sensitivity_oi_range":[float(sens.oi_coeff.min()),float(sens.oi_coeff.max())],
             "max_position_size_error":max(abs(row["size_difference"]) for row in check),
             "drop_volume_only_pooled_coeff":a_volume}
    evidence=[]
    for account, ids in [("A1",[2,3]),("A2",[9]),("A3",[12])]:
        z=w.iloc[ids]
        hours=z.hours.sum()
        points=z.delta_points.sum()
        referral=.1*summary["a3_clean_live"]*z.a3_oi_m_week.sum() if account=="A1" else 0.
        evidence.append({"account":account,"hours":hours,"gross_average_oi_m":z.oi_m_week.sum()*168/hours,
                         "delta_points":points,"a3_referral_at_a3_clean_rate":referral,
                         "own_live_rate":(points-referral)/z.oi_m_week.sum(),
                         "own_points_per_week":(points-referral)*168/hours})
    ev=pd.DataFrame(evidence)
    ev.to_csv(HERE/"live_evidence.csv",index=False)
    power=np.polyfit(np.log(ev.gross_average_oi_m),np.log(ev.own_points_per_week),1)
    summary["live_cross_section_power_exponent"]=float(power[0])
    summary["live_cross_section_power_scale"]=float(np.exp(power[1]))
    nonlinear_alpha=float(np.log(y[2]/y[1])/np.log(x[2,0]/x[1,0]))
    nonlinear_k=float(y[1]/x[1,0]**nonlinear_alpha)
    other_rate=float(y[1]/x[1,0])
    stock_rate=float((y[2]-other_rate*(x[2,0]-ds.iloc[2].volume_stock_m))/ds.iloc[2].volume_stock_m)
    summary["alternative_no_oi_superlinear_alpha"]=nonlinear_alpha
    summary["alternative_no_oi_superlinear_k"]=nonlinear_k
    summary["alternative_no_oi_other_volume_rate"]=other_rate
    summary["alternative_no_oi_stock_volume_rate"]=stock_rate
    summary["alternative_no_oi_superlinear_sep18_residual"]=float(y[0]-nonlinear_k*x[0,0]**nonlinear_alpha)
    summary["prior_drop_volume_coeff_sensitivity"]=[]
    for vp,op in [(45,8),(45,11),(50,8),(50,11)]:
        summary["prior_drop_volume_coeff_sensitivity"].append({"volume":vp,"oi":op,"residuals":(y-vp*x[:,0]-op*x[:,1]).tolist()})
    (HERE/"model_summary.json").write_text(json.dumps(summary,indent=2)+"\n")
    print(json.dumps(summary,indent=2))
    return summary


if __name__=="__main__":
    run()
