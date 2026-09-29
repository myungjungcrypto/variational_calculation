"""Counterfactual equity proxy test; historical TVL is NOT observed.

Compare two equally sized drop models using the same baseline live/credit
subtractions. End-snapshot equity is held constant over each earlier week
solely as an explicit what-if assumption. This cannot establish TVL causality.
"""
import json
import numpy as np
import pandas as pd
from core import HERE, DATA


def run():
    d=pd.read_csv(HERE/'drop_fit.csv')
    w=pd.read_csv(HERE/'window_decomposition.csv')
    weeks=pd.read_csv(HERE/'weekly_activity.csv')
    snap=pd.read_csv(DATA/'points_snapshots.csv').dropna(subset=['equity_usd']).set_index('account')
    assert not snap.index.duplicated().any()
    d['constant_final_equity_proxy_m_week']=d.account.map(snap.equity_usd)/1e6
    x=d[['volume_m','constant_final_equity_proxy_m_week']].to_numpy()
    y=d.drop_estimate.to_numpy()
    coeff=np.linalg.lstsq(x,y,rcond=None)[0]
    d['counterfactual_volume_equity_drop']=x@coeff
    d['counterfactual_residual']=y-x@coeff
    d.to_csv(HERE/'tvl_counterfactual_fit.csv',index=False)
    a1week=weeks[(weeks.account=='A1') & weeks.week_end.str.startswith('2026-09-25')].iloc[0]
    a1window=w[(w.account=='A1') & w.start.str.startswith('2026-09-19')].iloc[0]
    remaining=a1window.delta_points-a1window.live_model-a1window.credit_model-a1window.a3_referral_model
    alternate=coeff[0]*a1week.volume_m+coeff[1]*snap.loc['A1','equity_usd']/1e6
    a3=d[d.account=='A3'].iloc[0]
    referral_adjustment=.1*(a3.counterfactual_volume_equity_drop-a3.fit_drop)
    alternate_remaining=remaining-referral_adjustment
    rows=[]
    for _,r in weeks[weeks.week_end.str.startswith('2026-09-25')].iterrows():
        equity=snap.loc[r.account,'equity_usd']
        rows.append({'account':r.account,'equity_observed_utc':snap.loc[r.account,'timestamp_utc'],
                     'equity_snapshot_usd':equity,'week25_average_oi_m':r.oi_m_week,
                     'counterfactual_leverage':r.oi_m_week/(equity/1e6),
                     'counterfactual_equity_per_dollar_oi':equity/1e6/r.oi_m_week})
    pd.DataFrame(rows).to_csv(HERE/'tvl_equity_proxy_inputs.csv',index=False)
    result={
        'warning':'Historical time-weighted TVL is missing. Final equity is a counterfactual constant proxy, not measured historical TVL.',
        'equity_definition':'Account equity including PnL, not necessarily deposited collateral, net deposits or free margin.',
        'live_and_credit_subtractions':'Held fixed at baseline OI-live model and A2 credit estimate.',
        'assumed_oi_drop_coefficient':0.,
        'volume_coefficient':float(coeff[0]),'equity_coefficient':float(coeff[1]),
        'baseline_drop_rmse':float(np.sqrt(np.mean(d.residual**2))),
        'counterfactual_drop_rmse':float(np.sqrt(np.mean(d.counterfactual_residual**2))),
        'a1_drop_available_before_other_referrals':float(remaining),
        'a1_baseline_predicted_drop':float(a1window.drop_model),
        'a1_baseline_residual_before_other_referrals':float(remaining-a1window.drop_model),
        'a1_counterfactual_predicted_drop':float(alternate),
        'a1_counterfactual_residual_before_other_referrals':float(alternate_remaining-alternate),
        'a3_referral_adjustment_under_tvl_model':float(referral_adjustment),
        'caveats':[
            'Same number of drop coefficients as baseline (two), but different unmeasured proxy assumption.',
            'Only three calibration observations from two accounts; no statistical model selection claim.',
            'A1 A3 referral is recomputed for the changed weekly drop; live and credit remain fixed.',
            'A1 remains a conditional diagnostic with unobserved other referrals and missing-export-tail assumption.',
            'A1 other-referee points are unknown; positive residual is compatible, not a measurement.',
            'Positive TVL coefficient is not evidence that the exchange uses this metric.',
            'Reported sensitivity does not replace the original APR estimate with a newly verified value.',
        ],'inputs':rows,
    }
    (HERE/'tvl_summary.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    note=f'''## 후속 검토: TVL / 자기자본 보상 가설

계정에 맡긴 자본에 보상이 붙는다는 가설은 합리적인 추가 후보다. 단, 예치금·계정 equity·미사용 담보·포지션 margin은 서로 다르다. 현재 CSV에는 A1/A2는 9/28 13:00 UTC, A3는 9/28 03:42 UTC의 equity 한 번씩만 있다. 실제 보상 주간의 평균 TVL은 없다.

확장 모형을 `D = a V + b I + c T`로 쓰면, `T`는 시간가중 평균 자본의 $1M·주다. `L=I/T`라 할 때 TVL을 누락하고 OI로 전부 귀속한 실효 계수는 `b + c/L`이 된다. 같은 OI에서 레버리지가 낮은 계정은 자본이 더 많이 묶여 있어, 양수 TVL 보상이 있다면 OI 계수가 더 높아 보인다. 이 방향은 A3 기반 OI 계수를 A1에 적용할 때의 과대예측과 맞지만, 원인이 TVL이라는 증거는 아니다.

다음은 **9/28 equity가 보상 주간 내내 일정했다는 반사실적 가정**으로 확인한 계산이다. 기존 라이브·거래 크레딧 차감은 그대로 두고, 주간 드롭을 `V+OI` 또는 `V+equity`의 두 계수로 각각 적합했다. 변수 수를 늘려 정확히 맞춘 계산은 아니다.

| 조건부 모형 | 거래량 계수 pt/$1M | OI 계수 pt/$1M·주 | equity 계수 pt/$1M·주 |
|---|---:|---:|---:|
| 기존 V+OI | {json.loads((HERE/'model_summary.json').read_text())['drop_volume_coeff']:.3f} | {json.loads((HERE/'model_summary.json').read_text())['drop_oi_coeff']:.3f} | 0 가정 |
| 대체 V+equity | {coeff[0]:.3f} | 0 가정 | {coeff[1]:.3f} |

두 모형의 세 학습 관측 RMSE는 각각 {result['baseline_drop_rmse']:.3f}, {result['counterfactual_drop_rmse']:.3f}pt로 비슷하다. A1에서 다른 피추천인 보상을 차감하기 전 드롭 잔여는 {remaining:.3f}pt다.

| A1 비교 | 예측 드롭 | 잔여−예측 |
|---|---:|---:|
| 기존 V+OI | {a1window.drop_model:.3f} | {remaining-a1window.drop_model:.3f} |
| 대체 V+equity | {alternate:.3f} | {alternate_remaining-alternate:.3f} |

대체 모형의 양수 잔차는 추가 추천 수입과 양립 가능하다. A3 추천분도 바뀐 드롭으로 다시 계산했으며 A1 차감액 변화는 {referral_adjustment:.8f}pt로 매우 작다. 그러나 실제 과거 equity 및 다른 피추천인 포인트를 모르므로 독립 검증이나 TVL 보상 발견으로 해석할 수 없다. 특히 마지막 equity를 과거 평균으로 쓰면 그 사이 입출금·손익·펀딩 변화가 사라진다.

따라서 앞선 49.4% 또는 A1의 41.3% 같은 수치는 동일 조건에서 확인된 계정별 APR이 아니라, 서로 다른 귀속 가정에서 역산한 값이다. TVL이 있다면 보유 보상의 자본당 비율은 `l L + b L + c`에 비례하므로 레버리지를 두 배로 해도 TVL 부분은 두 배가 되지 않는다. 이 가설을 확인하려면 **OI가 비슷하고 자본이 다른 구간**, 또는 **자본이 비슷하고 OI가 다른 구간**의 시간가중 자료와 추천을 제외한 자체 포인트가 필요하다. 기존 입출금 이력을 먼저 확보하는 것이 적절하다.

재현: `python3 handoff/analysis/tvl_sensitivity.py`. `tvl_counterfactual_fit.csv`, `tvl_equity_proxy_inputs.csv`, `tvl_summary.json`에 입력·가정·잔차가 있다.
'''
    (HERE/'tvl_notes.md').write_text(note)
    print(json.dumps({k:v for k,v in result.items() if k not in ('caveats','inputs')},indent=2))
    return result


if __name__=='__main__':
    run()
