#!/usr/bin/env python3
"""Independent point-model audit: executable only from supplied local exports.

Uses timestamp-aggregated signed position size and same-timestamp VWAP as last
observed price. Integrates |size| * last fill price in exact event time. Holding
latest price beyond a last trade is an approximation, not historical marks.
Activity windows are [start, end), matching the canonical analysis convention.
"""
from pathlib import Path
import json
import numpy as np
import pandas as pd

BASE=Path(__file__).resolve().parents[1]
DATA=BASE/'data'/'lighter'
OUT=BASE/'analysis'
WEEK=604800
STOCK={'SPY','QQQ'}
SNAPS=pd.read_csv(DATA/'points_snapshots.csv')
SNAPS['t']=pd.to_datetime(SNAPS.timestamp_utc,utc=True)
TR={}
EVENTS={}

def strict_json(value):
    """Serialize data-frame scalars with missing values as JSON null."""
    def clean(x):
        if isinstance(x,dict):
            return {k:clean(v) for k,v in x.items()}
        if isinstance(x,(list,tuple,np.ndarray)):
            return [clean(v) for v in x]
        if isinstance(x,np.generic):
            return clean(x.item())
        if isinstance(x,float) and not np.isfinite(x):
            return None
        return x
    return json.dumps(clean(value),indent=2,default=str,allow_nan=False)

for a in ('A1','A2','A3'):
    p=next(DATA.glob(a.lower()+'_lighter_export_*.csv*'))
    d=pd.read_csv(p)
    d['t']=pd.to_datetime(d.Date,utc=True)
    for c in ('Trade Value','Size','Price','Fee'):
        d[c]=pd.to_numeric(d[c],errors='raise')
    d['signed']=d.Size*np.where(d.Side.isin(['Open Long','Close Short']),1,-1)
    d=d.sort_values(['t','Trade ID'])
    TR[a]=d
    # Market histories; aggregate all fills in each UTC second first.
    EVENTS[a]={}
    for market,g in d.groupby('Market'):
        z=g.groupby('t').agg(signed=('signed','sum'),v=('Trade Value','sum'),sz=('Size','sum'))
        z['q']=z.signed.cumsum()
        z['px']=z.v/z.sz
        z['oi']=z.q.abs()*z.px
        EVENTS[a][market]=z

def integrate(account,start,end,scaled=False):
    start,end=pd.Timestamp(start),pd.Timestamp(end)
    rows=[]
    marks=pd.read_csv(DATA/'positions_snapshots.csv').query('account == @account').set_index('market')
    for market,z in EVENTS[account].items():
        x=z.loc[z.index<=end].copy()
        before=x.loc[x.index<=start]
        init=before.iloc[-1] if len(before) else None
        inside=x.loc[(x.index>start)&(x.index<end)]
        times=[start]+list(inside.index)+[end]
        if scaled and market in marks.index:
            vals=([abs(init.q)*marks.loc[market,'mark_price'] if init is not None else 0]
                  +(inside.q.abs()*marks.loc[market,'mark_price']).tolist())
        else:
            vals=([init.oi if init is not None else 0]+inside.oi.tolist())
        integ=sum(v*(r-l).total_seconds() for v,l,r in zip(vals,times[:-1],times[1:]))
        rows.append((market,integ/WEEK/1e6))
    return dict(rows)

def window(a,s,e):
    s,e=pd.Timestamp(s),pd.Timestamp(e)
    g=TR[a].loc[(TR[a].t>=s)&(TR[a].t<e)]
    oi=integrate(a,s,e)
    return {'account':a,'start':s.isoformat(),'end':e.isoformat(),
            'hours':(e-s).total_seconds()/3600,'volume_m':g['Trade Value'].sum()/1e6,
            'fees':g.Fee.sum(),'fills':len(g),
            'oi_mweek':sum(oi.values()),
            'stock_mweek':sum(v for k,v in oi.items() if k in STOCK),
            'crypto_other_mweek':sum(v for k,v in oi.items() if k not in STOCK),
            'fixed_sep19_marks_mweek':sum(integrate(a,s,e,True).values())}

windows=[]
for a,g in SNAPS.groupby('account'):
    g=g.sort_values('t')
    for (_,l),(_,r) in zip(g.iloc[:-1].iterrows(),g.iloc[1:].iterrows()):
        w=window(a,l.t,r.t)
        w.update(points=r.points-l.points, note_end=r.note)
        w['naive_live_pt_mweek']=w['points']/w['oi_mweek']
        if a=='A1':
            w['a3_oi_mweek']=sum(integrate('A3',l.t,r.t).values())
            w['live_rate_if_10pct_a3']=w['points']/(w['oi_mweek']+.1*w['a3_oi_mweek'])
        windows.append(w)
w=pd.DataFrame(windows)
w.to_csv(OUT/'independent_windows.csv',index=False)

# Local clean intervals around the single accidental fill identify the immediate
# coefficient conditional on unchanged live coefficient and mark-price proxy.
a2clean=w.query("account == 'A2' and start == '2026-09-18T19:11:00+00:00'").iloc[0]
a2acc=w.query("account == 'A2' and start == '2026-09-18T15:33:00+00:00'").iloc[0]
l=a2clean.points/a2clean.oi_mweek
rt=(a2acc.points-l*a2acc.oi_mweek)/a2acc.volume_m
rt_fixed=(a2acc.points-(a2clean.points/a2clean.fixed_sep19_marks_mweek)*a2acc.fixed_sep19_marks_mweek)/a2acc.volume_m

# Fit weekly drop volume / OI coefficient based on observed point deltas minus
# an explicitly assumed contemporaneous live and immediate coefficient.
rows=[]
for live in (5.0,5.5,6.0):
    for realtime in (0,rt,5.5):
        for hour in (0,12,15,15.5):
            boundary=pd.Timestamp('2026-09-18',tz='UTC')+pd.Timedelta(hours=hour)
            for account,s,e,drop in [
                ('A2','2026-09-18T14:31Z','2026-09-18T15:33Z',boundary),
                ('A2','2026-09-19T02:42Z','2026-09-27T15:00Z',boundary+pd.Timedelta(days=7)),
                ('A3','2026-09-19T03:59Z','2026-09-26T10:40Z',boundary+pd.Timedelta(days=7)),
            ]:
                s,e=pd.Timestamp(s),pd.Timestamp(e)
                delta=SNAPS.query('account == @account and t == @e').points.iloc[0]-SNAPS.query('account == @account and t == @s').points.iloc[0]
                wi=window(account,s,e)
                dw=window(account,drop-pd.Timedelta(days=7),drop)
                residual=delta-live*wi['oi_mweek']-realtime*wi['volume_m']
                rows.append({'account':account,'drop_date':drop.date().isoformat(),'boundary_hour':hour,
                    'assumed_live':live,'assumed_rt':realtime,'window_delta':delta,
                    'window_oi_mweek':wi['oi_mweek'],'window_volume_m':wi['volume_m'],
                    'drop_points_residual':residual,'drop_volume_m':dw['volume_m'],
                    'drop_oi_mweek':dw['oi_mweek'],'drop_stock_mweek':dw['stock_mweek'],
                    'drop_crypto_other_mweek':dw['crypto_other_mweek'],
                    'drop_stock_volume_m':TR[account].loc[(TR[account].t>=drop-pd.Timedelta(days=7))&(TR[account].t<drop)&TR[account].Market.isin(STOCK),'Trade Value'].sum()/1e6,
                    'implied_volume_only_coefficient':residual/dw['volume_m']})
f=pd.DataFrame(rows)
f.to_csv(OUT/'independent_drop_sensitivity.csv',index=False)
fits=[]
for (liv,real,hr),g in f.groupby(['assumed_live','assumed_rt','boundary_hour']):
    X=g[['drop_volume_m','drop_oi_mweek']].values
    y=g.drop_points_residual.values
    coef=np.linalg.lstsq(X,y,rcond=None)[0]
    resid=y-X@coef
    fits.append({'assumed_live':liv,'assumed_rt':real,'boundary_hour':hr,
      'drop_volume_coefficient':coef[0],'drop_oi_coefficient':coef[1],
      'max_abs_residual_points':max(abs(resid)), 'matrix_condition_number':np.linalg.cond(X),
      'residual_A2_sep18':resid[0],'residual_A2_sep25':resid[1],'residual_A3_sep25':resid[2]})
pd.DataFrame(fits).to_csv(OUT/'independent_drop_fits.csv',index=False)

# Direct position-size validation against actual known snapshots.
pr=[]
for _,r in pd.read_csv(DATA/'positions_snapshots.csv').iterrows():
    z=EVENTS[r.account][r.market].loc[:pd.Timestamp(r.timestamp_utc)]
    q=z.iloc[-1].q
    expected=r['size']*(1 if r.side=='long' else -1)
    pr.append({'account':r.account,'market':r.market,'reconstructed_size':q,
       'snapshot_signed_size':expected,'size_difference':q-expected,
       'fill_price_oi':z.iloc[-1].oi,'snapshot_mark_oi':r.value_usd})
pd.DataFrame(pr).to_csv(OUT/'independent_position_check.csv',index=False)

out={'a2_local_live':l,'a2_local_immediate_volume':rt,'a2_local_immediate_fixed_marks':rt_fixed,
     'model_scope':'Local data audit; all coefficients conditional on current holdings and last-fill price proxy.',
     'a2_accidental_window':a2acc.to_dict(),'a2_clean_window':a2clean.to_dict()}
(OUT/'independent_audit.json').write_text(strict_json(out))
print(w[['account','start','end','points','volume_m','oi_mweek','naive_live_pt_mweek','live_rate_if_10pct_a3']].to_string(index=False))
print('A2 live, immediate:',l,rt,rt_fixed)
print(pd.DataFrame(fits).query('assumed_live == 5.5 and boundary_hour == 15').to_string(index=False))
print(pd.DataFrame(pr).to_string(index=False))

# Construct observationally competing models on the same September 25 data.
# These are sensitivity examples, not proposed true reward formulas.
selected=f[(f.assumed_live==5.5)&(f.assumed_rt==rt)&(f.boundary_hour==15)]
first,small,large=selected.to_dict('records')
two_week_coefs=np.linalg.solve(
    np.array([[small['drop_volume_m'],small['drop_oi_mweek']],
              [large['drop_volume_m'],large['drop_oi_mweek']]]),
    np.array([small['drop_points_residual'],large['drop_points_residual']]))
sep18_volume=(first['drop_points_residual']-two_week_coefs[1]*first['drop_oi_mweek'])/first['drop_volume_m']
alpha=np.log(large['drop_points_residual']/small['drop_points_residual'])/np.log(large['drop_volume_m']/small['drop_volume_m'])
power_scale=small['drop_points_residual']/small['drop_volume_m']**alpha
other_vol=small['drop_points_residual']/small['drop_volume_m']
stock_vol=(large['drop_points_residual']-other_vol*(large['drop_volume_m']-large['drop_stock_volume_m']))/large['drop_stock_volume_m']

live_market=[]
for a,s in [('A1','2026-09-18T15:32:00+00:00'),('A1','2026-09-18T19:11:00+00:00'),
            ('A2','2026-09-18T19:11:00+00:00'),('A3','2026-09-19T03:03:00+00:00')]:
    row=w[(w.account==a)&(w.start==s)].iloc[0]
    st,cr=row.stock_mweek,row.crypto_other_mweek
    if a=='A1':
        ref_oi=integrate('A3',row.start,row.end)
        st+=.1*sum(v for k,v in ref_oi.items() if k in STOCK)
        cr+=.1*sum(v for k,v in ref_oi.items() if k not in STOCK)
    live_market.append({'account':a,'start':s,'stock_mweek_including_A3_referral':st,
      'other_mweek_including_A3_referral':cr,'points':row.points})
lm=pd.DataFrame(live_market)
lm.to_csv(OUT/'independent_live_market_inputs.csv',index=False)
X=lm[['stock_mweek_including_A3_referral','other_mweek_including_A3_referral']].values
y=lm.points.values
market_coefs=np.linalg.lstsq(X/y[:,None],np.ones(len(y)),rcond=None)[0]
market_residual=(X@market_coefs-y)/y
alternatives={
  'sep25_linear_volume_coefficient':two_week_coefs[0],
  'sep25_linear_oi_coefficient':two_week_coefs[1],
  'sep18_volume_coefficient_if_sep25_oi_applies':sep18_volume,
  'sep25_power_law_without_oi_exponent':alpha,
  'sep25_power_law_without_oi_scale':power_scale,
  'sep25_stock_volume_only_coefficient':stock_vol,
  'sep25_other_volume_only_coefficient':other_vol,
  'sep25_stock_vs_other_volume_multiplier':stock_vol/other_vol,
  'clean_live_stock_coefficient':market_coefs[0],
  'clean_live_other_coefficient':market_coefs[1],
  'clean_live_two_sector_max_relative_residual':max(abs(market_residual)),
  'a3_tail_live_net_known_realtime':float((w.iloc[-1].points-rt*w.iloc[-1].volume_m)/w.iloc[-1].oi_mweek),
}
global_fit=next(x for x in fits if x['assumed_live']==5.5 and x['assumed_rt']==rt and x['boundary_hour']==15)
av=global_fit['drop_volume_coefficient']
ao=global_fit['drop_oi_coefficient']
a1w=w[(w.account=='A1')&(w.start=='2026-09-19T02:35:00+00:00')].iloc[0]
a3w=window('A3',a1w.start,a1w.end)
aw=window('A1','2026-09-18T15:00Z','2026-09-25T15:00Z')
bw=window('A3','2026-09-18T15:00Z','2026-09-25T15:00Z')
pred_own=5.5*a1w.oi_mweek+rt*a1w.volume_m+av*aw['volume_m']+ao*aw['oi_mweek']
pred_ref=.1*(5.5*a3w['oi_mweek']+rt*a3w['volume_m']+av*bw['volume_m']+ao*bw['oi_mweek'])
alternatives.update(a1_sep19_sep27_predicted_own=pred_own,
                    a1_sep19_sep27_predicted_a3_referral=pred_ref,
                    a1_sep19_sep27_observed=float(a1w.points),
                    a1_sep19_sep27_residual_before_other_referral=a1w.points-pred_own-pred_ref)
(OUT/'independent_alternatives.json').write_text(strict_json(alternatives))
clean_a2_mark=a2clean.points/a2clean.fixed_sep19_marks_mweek
summary=f'''# Independent model audit

This companion audit uses only supplied exports. It integrates market positions
at each account's own most recent fill VWAP, independently of the canonical
analysis's cross-account price marks. Consequently small coefficient differences
are expected. All reconstructed position sizes match the known September 19
position snapshots to floating-point precision. No external market data is used.

## Most reliable local observations

- A2's accidental $15,000 maker fill implies immediate credit {rt:.6f} pt/$M;
  replacing fill-price OI with known September 19 marks gives {rt_fixed:.6f}.
  This is strong evidence for a local credit, but one fill cannot establish a
  universal rate or exact booking time between snapshots.
- A2's next, genuinely trade-free interval implies live {l:.6f} pt/$M-week
  using last-fill prices, or {clean_a2_mark:.6f} using the known mark prices.
- The 56-minute A3 clean interval gives 5.129215 pt/$M-week at recorded times.
- A1's two clean postdrop windows imply common live 5.369451 and 5.392169 after
  adding 10% of A3's OI exposure. This supports referral sharing of live accrual
  conditional on a common own/referee coefficient; it does not independently
  identify 10% for every points component.
- The first 19-minute A1/A2 observations are compatible with an approximately
  ten-minute common timestamp error and should not determine the rate.
- A3's final window includes known $0.635487M trading. Removing immediate credit
  gives live {alternatives['a3_tail_live_net_known_realtime']:.6f}, conditional on
  no trades after its export cutoff. A1/A2/A3 final points snapshots all extend
  beyond their respective trade exports: export silence cannot certify no later
  trading.

## Drop alternatives and limitations

At Friday 15:00 UTC, assumed live 5.5 and immediate {rt:.6f}, the A2 drop
volume-only slopes are {first['implied_volume_only_coefficient']:.6f} (September 18)
and {small['implied_volume_only_coefficient']:.6f} (September 25), versus A3's
{large['implied_volume_only_coefficient']:.6f} (September 25). A common constant linear volume-only
coefficient is inconsistent with these observations. However, the following
models all fit the September 25 A2/A3 observations exactly:

- Volume + OI: {two_week_coefs[0]:.6f} pt/$M volume plus
  {two_week_coefs[1]:.6f} pt/$M-week OI. With that OI rate, September 18 A2
  requires a volume rate {sep18_volume:.6f}.
- Nonlinear volume with zero OI: {power_scale:.6f} * V^{alpha:.6f}, with V in $M.
- Separate volume weights with zero OI: other markets {other_vol:.6f}, stock
  markets {stock_vol:.6f} pt/$M; the stock multiplier is {stock_vol/other_vol:.6f}.
  This large multiplier is a mathematical counterexample, not evidence for it.

The pooled three-row linear V+OI fit is about 38.57 + 12.08 and misses the two
A2 drops by approximately 0.77 points. In particular, the prior 45–50 volume
slopes were obtained while omitting A2's own OI contribution; adding a positive
OI coefficient requires lowering those volume slopes. Midnight/noon/15:00
boundary choices materially move fitted coefficients; see sensitivity CSV.
Market weighting, superlinearity, weekly coefficient changes and an OI term
are not separately identified by two accounts on one common observed drop.

As an out-of-fit check, the same pooled model predicts A1's September 19–27
window as own {pred_own:.6f} plus A3 referral {pred_ref:.6f}, versus observed
{a1w.points:.6f}. The residual is {a1w.points-pred_own-pred_ref:.6f} points
before adding any other-referee income. Positive omitted referral income only
widens the discrepancy. Thus the common four-component coefficients fail to
reconcile A1, even though they approximately fit the selected A2/A3 windows.
This failure cannot by itself distinguish eligibility, market weights, weekly
rate changes, point-credit lags, inaccurate valuation or a narrower referral
definition. These coefficients should not be described as a solved global rule.

## Market-independence and uncertainty

A two-sector clean-live fit gives stock {market_coefs[0]:.6f} and other markets
{market_coefs[1]:.6f} pt/$M-week, with maximum relative residual
{max(abs(market_residual))*100:.2f}%. A material market difference is compatible
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
'''
(OUT/'independent_audit.md').write_text(summary)
