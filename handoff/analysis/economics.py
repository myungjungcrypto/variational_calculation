"""Referral, cost and APR scenarios. All unmeasured inputs remain explicit."""
import json
from itertools import product
import numpy as np
import pandas as pd
from core import HERE, DATA, WEEK, ts

POOL_LIT = 11_000_000
END_POINTS = (1_500_000, 2_000_000, 3_600_000, 5_200_000)
PRICES = (3,4,5)
LEVERAGES = (10,15,20)
SPREAD_BPS = (-0.5,0,0.2,0.5,1)


def run():
    m = json.loads((HERE/"model_summary.json").read_text())
    w = pd.read_csv(HERE/"window_decomposition.csv")
    ds = pd.read_csv(HERE/"drop_sensitivity.csv")
    ds["total_oi_rate"] = ds.live + ds.oi_coeff
    rate = m["live_center"] + m["drop_oi_coeff"]
    rates = {"fitted_oi_drop":rate,"live_only":m["live_center"],
             "conditional_sensitivity_low":ds.total_oi_rate.min(),
             "conditional_sensitivity_high":ds.total_oi_rate.max(),"prior_scenario":15.5}
    apr=[]
    for name,q in rates.items():
        for price,total,leverage,structure in product(PRICES,END_POINTS,LEVERAGES,("A","B")):
            eligible=1 if structure=="A" else .5
            value=POOL_LIT*price/total
            apr.append({"rate_case":name,"oi_points_per_m_week":q,"lit_price_usd":price,
                        "program_end_points":total,"lit_per_point":POOL_LIT/total,"usd_per_point":value,
                        "gross_all_venues_oi_to_total_equity":leverage,"structure":structure,
                        "earning_fraction":eligible,"gross_apr_pct":52*q*leverage*eligible*value/1e6*100})
    pd.DataFrame(apr).to_csv(HERE/"apr_scenarios.csv",index=False)
    funding=pd.read_csv(DATA/"positions_snapshots.csv").groupby("account").funding_cum_usd.sum().to_dict()
    clean = w.iloc[2:4]
    ref_implied=(clean.delta_points.sum()-m["live_center"]*clean.oi_m_week.sum())/(m["live_center"]*clean.a3_oi_m_week.sum())
    ref_extremes=[]
    for l,s1,s3 in product(m["live_sensitivity_range"],(.9,1.1),(.9,1.1)):
        ref_extremes.append((clean.delta_points.sum()-l*s1*clean.oi_m_week.sum())/(l*s3*clean.a3_oi_m_week.sum()))
    costs=[]
    for index in (4,10,13):
        row=w.iloc[index]
        known_ref=row.a3_referral_model
        # Extra-referral values are scenarios, not an estimated distribution.
        for other_ref in ((0,20,40) if row.account=="A1" else (0,)):
            own=row.delta_points-known_ref-other_ref
            assert own>0
            for spread in SPREAD_BPS:
                fees_plus_spread=row.fee_usd+100*row.volume_m*spread
                costs.append({"account":row.account,"start":row.start,"end":row.end,
                              "total_point_delta":row.delta_points,"a3_referral_assumed":known_ref,
                              "other_referral_assumed":other_ref,"own_points_conditional":own,
                              "explicit_fees_usd":row.fee_usd,"volume_usd":row.volume_m*1e6,
                              "spread_bp_assumed":spread,"total_cost_scenario_usd":fees_plus_spread,
                              "cost_per_own_point_usd":fees_plus_spread/own,
                              "point_denominator_identified":row.account!="A1"})
    pd.DataFrame(costs).to_csv(HERE/"cost_per_point.csv",index=False)
    # A held position pays entry+exit fees on both sides of combined gross OI.
    turnover=[]
    for lev,h,s in product(LEVERAGES,(1,4,13,52),(0,.2,1)):
        drag_pct=52/h*2*lev*(3.41+s)/10000*100
        turnover.append({"leverage":lev,"holding_weeks":h,"fee_bp_assumed":3.41,
                         "spread_bp_assumed":s,"round_trip_cost_pct_equity":2*lev*(3.41+s)/100,
                         "annualized_cost_drag_percentage_points":drag_pct})
    pd.DataFrame(turnover).to_csv(HERE/"oi_transaction_cost_scenarios.csv",index=False)
    # A3 referral table says 'exact'; independently compare full exported sums.
    refs=pd.read_csv(DATA/"referrals.csv")
    displayed=refs[(refs.referee_masked=="0xc2...9664")&(refs.snapshot_date=="2026-09-27")].cumulative_volume_usd.iloc[0]
    c=json.loads((HERE/"costs_summary.json").read_text())
    raw_a3=next(x["volume_usd"] for x in c["window_summaries"] if x["account"]=="A3" and x["window"]=="lifetime")
    extra=refs[(refs.snapshot_date=="2026-09-27")&(~refs.referee_masked.isin(["0xc2...9664","0xaF...71f0","0x10...4890"]))].cumulative_volume_usd.sum()
    summary={"oi_rate_cases":rates,"referral_live_implied_at_common_live":ref_implied,
             "referral_live_sensitivity_range":[min(ref_extremes),max(ref_extremes)],
             "cumulative_funding_usd_at_position_snapshots":funding,
             "a3_referral_display_minus_export_volume_usd":displayed-raw_a3,
             "new_referee_reported_volume_usd":extra,
             "new_referee_volume_points_referral_scenario_min":.1*m["rt_local_a2"]*extra/1e6,
             "new_referee_volume_points_referral_scenario_max":.1*(m["rt_local_a2"]+m["drop_volume_coeff"])*extra/1e6,
             "a1_all_points_fee_ratio_invalid_own_denominator":w.iloc[4].fee_usd/w.iloc[4].delta_points,
             "pool_lit_assumed":POOL_LIT}
    (HERE/"economics_summary.json").write_text(json.dumps(summary,indent=2)+"\n")
    print(json.dumps(summary,indent=2))
    return summary


if __name__=="__main__":
    run()
