"""Generate RESULTS.md from the reproducible analysis outputs."""
import json
import pandas as pd
from core import HERE, DATA, snapshots, ts


def table(headers,rows):
    return "\n".join(["| "+" | ".join(headers)+" |", "|"+"|".join(["---"]*len(headers))+"|"]+["| "+" | ".join(map(str,r))+" |" for r in rows])


def f(x,n=3):
    return f"{x:,.{n}f}"


def run():
    j=lambda name:json.loads((HERE/name).read_text())
    c=lambda name:pd.read_csv(HERE/name)
    m,e,p,cost=j("model_summary.json"),j("economics_summary.json"),j("platform_summary.json"),j("costs_summary.json")
    w,d,life,sens=c("window_decomposition.csv"),c("drop_fit.csv"),c("lifetime_reconciliation.csv"),c("drop_sensitivity.csv")
    cp,apr=c("cost_per_point.csv"),c("apr_scenarios.csv")
    ev=c("live_evidence.csv")
    ind=c("independent_windows.csv")
    getcost=lambda a,k:next(x for x in cost["window_summaries"] if x["account"]==a and x["window"]==k)
    out=[]
    add=out.append
    add("# Lighter Robinhood Chain 포인트 독립 검증\n")
    add("분석 기준: 제공된 2026-09-28까지의 관측, 원본 커밋 `8475623`. 계산 입력은 `data/lighter/`뿐이다. 선택 자료인 Variational은 Lighter 계수 추정에 사용하지 않았다. 아래 표는 `python3 handoff/analysis/run_all.py`로 다시 생성된다.\n")
    add(f"**라이브 약 {f(m['live_center'],1)} pt / $1M OI·주는 재현되지만, 전체 포인트 공식을 확정할 수는 없다.** HYPE 메이커 거래에서 비-OI 크레딧 {f(m['rt_local_a2'])} pt/$1M이 관측된다. 선형 드롭 가정의 공동 적합은 거래량 {f(m['drop_volume_coeff'],2)} pt/$1M + OI {f(m['drop_oi_coeff'],2)} pt/$1M·주다. 그러나 이 식은 A1과 누적 포인트를 완전히 설명하지 못하며, OI 드롭은 비선형 거래량 보상과도 구분되지 않는다.\n")
    add("## 1. 입력 검증과 계산 방법\n")
    add(table(["계정","체결 수","전체 거래대금 USD","전체 Fee USD","마지막 체결 UTC"],[[a,f(getcost(a,'lifetime')['fills'],0),f(getcost(a,'lifetime')['volume_usd'],6),f(getcost(a,'lifetime')['fees_usd'],4),getcost(a,'lifetime')['last_fill_utc']] for a in ['A1','A2','A3']]))
    add("\n매수는 Open Long / Close Short, 매도는 Open Short / Close Long으로 매핑했다. 같은 초의 체결을 합산한 순수량을 누적하고, 시장별 `abs(수량) × 가격`을 합산한다. gross OI는 롱·숏 명목금액 합계이며 마진금이나 순노출이 아니다. 거래량은 export의 편도 체결금액을 한 번씩 더한다.\n")
    add(f"가격은 세 계정의 해당 시장 체결 VWAP를 시각 순서대로 전방 유지하고, 제공된 포지션 mark를 해당 시각부터 반영했다. 미래 mark를 과거에 소급하지 않았다. 실제 연속 mark 이력은 없으므로 OI 적분은 근사값이다. 별도 검증은 각 계정 마지막 체결가만 사용한다. 포지션 스냅샷의 모든 수량은 재구성과 일치하며 최대 절대 차이는 {m['max_position_size_error']:.2g}이다. `position_validation.csv`에 검증값이 있다.\n")
    burst_fills=int(w.iloc[8].fills)
    add(f"A3 레퍼럴 표의 'export와 정확히 일치'라는 메모는 틀렸다. 9/27 누적 표시값과 CSV 합계 차이는 ${f(e['a3_referral_display_minus_export_volume_usd'],6)}다. 계정 연결은 소유자 제공 사실로 유지했지만, 합계는 export 값을 사용했다. A2의 '한 번의 $15K 거래'는 실제로 {burst_fills}건의 부분 체결(16:07:45–16:07:50 UTC)이다.\n")
    add("모든 계산 구간은 UTC `[start, end)`다. 기본 주간 기준은 금요일 15:00 → 다음 금요일 15:00, 지급 시각은 15:30으로 가정했다. 지급 이후 거래를 해당 드롭에 넣지 않는다. 가격 ±10%, 라이브 4.5/5.5/6.9, 거래 크레딧 4.1/관측 중심/5.3, 금요일 cutoff 00:00/12:00/15:00/15:30을 민감도 분석했다. 이 범위는 **선택한 조건부 민감도이며 통계적 신뢰구간이 아니다**.\n")
    add("거래 export 종료는 A1 9/27 15:00, A2 9/27 15:04, A3 9/27 10:40이다. 이후에는 추가 거래가 없었다고 확인할 수 없다. A3 9/26 10:40 포인트는 export보다 24시간 앞서며, 모든 비용·포인트 비교에서 이 차이를 반영했다.\n")
    add("## 2. 라이브, 거래 크레딧, 드롭 모형\n")
    add("`I(a,b) = ∫ gross_OI_USD(t) dt / (1,000,000 × 604,800)` ($1M·주), `V(a,b) = Σ Trade Value / 1,000,000` ($1M).\n\n```text\nΔP_own = l × I + r × V + Σ D_week + residual\nD_week = a × V_week + b × I_week                 [조건부 선형 모형]\nΔP_A1 = ΔP_own,A1 + f × ΔP_eligible,A3 + R_other\n```\n")
    vr,ir=m['drop_sensitivity_volume_range'],m['drop_sensitivity_oi_range']
    add(table(["계수","작업 중심값","범위 / 한계","주요 근거"],[
        ["l: 라이브",f(m['live_center'],1),"4.5–6.9 민감도; 시간 오차 전부 포함한 CI 아님","A1·A2·A3 무거래 구간"],
        ["r: 거래 연관 크레딧",f(m['rt_local_a2']),"4.1–5.3 스트레스 가정; 다른 시장 미식별","A2 HYPE $15K 메이커 체결"],
        ["a: 드롭 거래량",f(m['drop_volume_coeff'],2),f"{f(vr[0],2)}–{f(vr[1],2)} 조건부", "A2 두 주 + A3 한 주"],
        ["b: 드롭 OI",f(m['drop_oi_coeff'],2),f"{f(ir[0],2)}–{f(ir[1],2)} 조건부; 모형을 바꾸면 0도 배제 못함", "동일 세 관측"],
        ["f: 레퍼럴","10% 제공 규칙",f"라이브 역산 {f(100*e['referral_live_implied_at_common_live'],2)}%; 스트레스 {f(100*e['referral_live_sensitivity_range'][0],1)}–{f(100*e['referral_live_sensitivity_range'][1],1)}%", "A1과 A3 OI 비교; 범위는 다른 피추천인 수입=0 가정"]]))
    add("\n### 무거래 라이브 관측\n")
    live_rows=[]
    for _,r in ev.iterrows():
        z=ind[(ind.account==r.account)&(ind.volume_m==0)]
        if r.account=='A1':
            z=z[z.start.isin(['2026-09-18T15:32:00+00:00','2026-09-18T19:11:00+00:00'])]
            own_fill=z.points.sum()/(z.oi_mweek.sum()+.1*z.a3_oi_mweek.sum())
            rate=m['a1_clean_common_live_with_10pct_a3']
            note="A3와 공통 l, 레퍼럴 10% 가정"
        else:
            start='2026-09-18T19:11:00+00:00' if r.account=='A2' else '2026-09-19T03:03:00+00:00'
            z=z[z.start==start]
            own_fill=z.naive_live_pt_mweek.iloc[0]
            rate=r.own_live_rate
            note="A2 19:11→02:42" if r.account=='A2' else "A3 03:03→03:59"
        live_rows.append([r.account,f(r.hours,3),f(r.gross_average_oi_m,3),f(rate,3),f(own_fill,3),note])
    add(table(["계정","시간 h","평균 gross OI $M","공유 체결가 기준 l","자기 체결가 기준 l","비고"],live_rows))
    mark_a2=ind[(ind.account=='A2')&(ind.start=='2026-09-18T19:11:00+00:00')].iloc[0]
    add(f"\nA2에 9/19 스냅샷 mark를 고정 적용하면 l={f(mark_a2.points/mark_a2.fixed_sep19_marks_mweek,3)}다. A1은 레퍼럴을 빼지 않으면 무거래에서도 약 {f(w.iloc[2:4].delta_points.sum()/w.iloc[2:4].oi_m_week.sum(),2)}가 되어 자체 OI 보상을 과대평가한다. A3에서 측정한 별도 l을 A1 추천분에 적용하면 A1 자체 l은 {f(ev.iloc[0].own_live_rate,3)}다.\n")
    add(f"계정별 시간당 자체 포인트와 평균 OI의 횡단면 power fit은 지수 {f(m['live_cross_section_power_exponent'],3)}으로 선형에 가깝다. 그러나 계정·시장 구성·시간이 함께 달라지므로 선형성의 인과 검증이 아니다. SPY/QQQ를 한 묶음, 나머지를 한 묶음으로 나눈 독립 적합도 가능하다(`independent_live_market_inputs.csv`, `independent_audit.md`). 따라서 **주식·크립토 시장 가중치가 동일하다는 결론은 내리지 않는다**. CASHCAT/ANTHROPIC 등도 있어 두 묶음은 순수한 자산군 구분이 아니다.\n")
    add("처음 14:12→14:31 구간의 낮은 증가율은 ±10분 시각 오차 가능성이 커서 추정에서 제외했다. A3의 56분 구간도 중요하다. ±30분이 두 스냅샷 각각의 독립 오차라면 실제 길이는 거의 0부터 116분까지 가능해 유한한 상한을 정할 수 없다. 위 A3 수치는 기록 시각이 맞거나 공통 시계 오프셋이어서 간격이 유지된다는 조건이다.\n")
    add("### 거래 크레딧의 근거\n")
    burst=w.iloc[8]
    add(f"A2 9/18 15:33→19:11 포인트 증가 {f(burst.delta_points,6)}에서 다음 무거래 구간의 l={f(m['a2_clean_live'],4)}로 계산한 OI 보상 {f(m['a2_clean_live']*burst.oi_m_week,6)}를 빼면 {f(m['rt_local_a2']*burst.volume_m,6)}가 남는다. 이를 ${f(burst.volume_m*1e6,6)} 거래량의 $1M 단위 값으로 나누면 r={f(m['rt_local_a2'],4)} pt/$1M이다. 가격 대안을 써도 약 4.574로 유지된다. 이는 거래와 연관된 별도 크레딧의 근거지만, 관측 간격이 {f(burst.hours,2)}시간이어서 '체결 순간 지급'까지 확인한 것은 아니다. maker·HYPE 외 체결에 같은 계수가 적용되는지도 미확인이다.\n")
    add("### 금요일 드롭 분해\n")
    add(table(["계정 / 지급일","주간 V $M","주간 I $M·주","추정 드롭","V만으로 나눈 계수","공동 적합 드롭","관측−적합"],[[r.account+' / '+str(r.week_end)[:10],f(r.volume_m,6),f(r.oi_m_week,6),f(r.drop_estimate,6),f(r.volume_only_effective_rate,3),f(r.fit_drop,6),f(r.residual,6)] for _,r in d.iterrows()]))
    add(f"\n기존 50.1/45.0은 A2 드롭 전체를 거래량으로 나눈 **실효 계수**로 재현된다. 이를 순수 거래량 계수로 사용하면서 OI 드롭을 추가하면 중복 계산이 된다. 공동 적합에서는 A2의 주간 OI 보상도 약 {f(m['drop_oi_coeff']*d.iloc[0].oi_m_week,2)}–{f(m['drop_oi_coeff']*d.iloc[1].oi_m_week,2)} pt라 무시할 수 없다.\n")
    add(f"세 식으로 두 계수를 맞췄고 잔차 자유도는 하나뿐이다. 열 정규화한 설계행렬 조건수는 {f(m['drop_column_normalized_condition_number'],1)}이며, 계수 분해가 불안정하다. A3의 거의 0인 잔차는 적합에 사용했기 때문이며 독립 검증이 아니다. 공통 선형 거래량만으로는 A2 약 45–50과 A3 약 100을 함께 설명하기 어렵다.\n")
    add(f"반면 9/25 두 관측만 쓰면 OI 없이 `D={f(m['alternative_no_oi_superlinear_k'],3)} × V^{f(m['alternative_no_oi_superlinear_alpha'],4)}`도 정확히 맞는다(V는 $M). 같은 두 관측은 나머지 시장 {f(m['alternative_no_oi_other_volume_rate'],2)}, SPY/QQQ {f(m['alternative_no_oi_stock_volume_rate'],2)} pt/$M로도 맞출 수 있다. 후자는 극단적인 가중치의 예시이며 추정 규칙으로 채택하지 않는다. 비선형 모형의 9/18 잔차는 {f(m['alternative_no_oi_superlinear_sep18_residual'],3)} pt여서 전체 자료를 해결한 대안도 아니다. **OI 드롭의 존재·크기를 단독으로 식별했다고 볼 수 없다.**\n")
    base_sens=sens[(sens.live==5.5)&(abs(sens.rt-m['rt_local_a2'])<1e-8)&(sens.oi_scale==1)]
    add(table(["금요일 활동 cutoff UTC","a","b","최대 절대 잔차 pt"],[[f(r.cutoff_hour,1),f(r.volume_coeff,3),f(r.oi_coeff,3),f(r.max_abs_residual,3)] for _,r in base_sens.iterrows()]))
    add("\n지급 시각이 금요일이라는 사실은 활동 집계 cutoff를 증명하지 않는다. 이 자료로 15:00을 확정할 수 없다. `drop_sensitivity.csv`는 위 cutoff와 계수·가격 민감도를 전부 보존한다.\n")
    add("## 3. 모든 스냅샷 구간의 분해와 누적 검증\n")
    add("아래는 공통 중심 모형의 예측 성분이다. A1 referral은 A3만의 10%이며 다른 피추천인 수입은 포함하지 않았다. 잔차를 억지로 레퍼럴이나 드롭으로 배정하지 않았다. 따라서 성분과 잔차를 더하면 관측 증가가 된다. 단위는 pt. `†`는 본인 또는 A3 추천분에 필요한 거래 export가 종료된 이후가 포함되는 조건부 계산이다.\n")
    rows=[]
    for _,r in w.iterrows():
        incomplete=not r.export_covers_end or (r.account=='A1' and not r.a3_export_covers_referral_end)
        label=r.account+('†' if incomplete else '')
        span=ts(r.start).strftime('%m/%d %H:%M')+' → '+ts(r.end).strftime('%m/%d %H:%M')
        rows.append([label,span,f(r.delta_points,6),f(r.live_model,6),f(r.credit_model,6),f(r.drop_model,6),f(r.a3_referral_model,6),f(r.unexplained_or_other_referral,6)])
    add(table(["계정","UTC 구간","관측 Δ","live","거래 credit","drop","A3 referral","잔차"],rows))
    a1=w.iloc[4]
    add(f"\n특히 A1 9/19→9/27에서 자체 모형 {f(a1.live_model+a1.credit_model+a1.drop_model,3)} + A3 추천분 {f(a1.a3_referral_model,3)}가 관측 {f(a1.delta_points,3)}를 {f(-a1.unexplained_or_other_referral,3)} pt 초과한다. 양수인 다른 피추천인 수입을 더하면 오차가 커진다. 독립적인 자기 체결가 방법에서도 약 16pt 초과가 나므로 공유 mark 방식만의 문제로 볼 수 없다. 시장·계정 가중치, 적격 거래, 추천 범위, 시점 또는 공식의 변화가 남아 있다.\n")
    add("A1 장기 구간의 A3 추천분은 9/27 10:40 이후 4시간 20분에 추가 거래가 없다는 가정까지 필요하다. A3 포인트 스냅샷 사이에서 모델로 보간한 값이므로 직접 관측된 추천 수입은 아니다.\n")
    last=life[life.apply(lambda r:ts(r.time)<= {'A1':ts('2026-09-27T15:00Z'),'A2':ts('2026-09-27T15:04Z'),'A3':ts('2026-09-27T10:40Z')}[r.account],axis=1)].groupby('account').tail(1)
    add(table(["계정 / 포인트 시각 UTC","누적 관측","누적 live","누적 credit","누적 drop","A3 추천분","관측−예측"],[[r.account+' / '+str(r.time)[:16],f(r.observed_points,3),f(r.live_model,3),f(r.credit_model,3),f(r.drop_model,3),f(r.a3_referral_model,3),f(r.residual,3)] for _,r in last.iterrows()]))
    add("\n누적 계산은 활동 시작부터 모든 거래를 동일하게 취급한 외삽 검증이다. A1·A2의 초기 Standard 기간, 상대방 유형에 따른 적격 여부, 과거 주간 배분 변화를 알 수 없어 전체 기간에 적용하면 실패한다. `lifetime_reconciliation.csv`에는 첫 스냅샷 이전 성분과 모든 누적 시점의 잔차가 포함되어 있다. '공식 해결'로 보고하면 안 되는 이유다.\n")
    add("## 4. 레퍼럴 10%의 검증 범위\n")
    add(f"A1의 깨끗한 두 구간에서 l=5.5, 다른 피추천인 수입=0을 가정하면 `f=(ΔP_A1−l I_A1)/(l I_A3)` = {f(e['referral_live_implied_at_common_live']*100,2)}%다. 제공된 10% 규칙과 일치 가능한 결과이며 라이브에도 추천 포인트가 포함된다는 해석을 지지한다. 하지만 별도 추천 포인트 원장이 없으므로 정확한 10%를 독립 검증한 것은 아니다. 드롭·거래 크레딧 각각에 같은 10%가 붙는지는 직접 분리되지 않는다.\n")
    add(f"9/27 새 피추천인들의 표시 거래량 합계는 ${f(e['new_referee_reported_volume_usd'],2)}다. 그 거래가 모두 비교 구간에 적격이고 r이 공통이며, 드롭에 포함된 비중을 0–100%로 두는 단순 예시에서 거래 관련 A1 추천분은 {f(e['new_referee_volume_points_referral_scenario_min'],2)}–{f(e['new_referee_volume_points_referral_scenario_max'],2)} pt다. 피추천인의 OI·타이밍·포인트가 없어 이는 총추천 수입의 상하한이 아니다. 기존의 ±40pt도 자료에서 얻은 신뢰구간이 아니다.\n")
    add("## 5. 포인트당 비용\n")
    add("`cost/own_point = (Σ Fee + V_USD × s_bp / 10,000) / (관측 포인트 증가 − 추천 수입)`\n\n스프레드/체결 손실을 측정할 주문 시점 midpoint·best bid/ask·외부 tick이 없다. Closed PnL은 가격 변동을 포함하므로 스프레드 대신 사용할 수 없다. 이전 분석의 Binance 기준 +0.2/−0.6bp는 이 패키지로 재현할 수 없다. 아래 s=0/0.2/1bp는 가정별 민감도이며, 실제 spread 추정이나 신뢰구간이 아니다. 음수 spread의 메이커 이익 시나리오도 CSV에 있다.\n")
    costrows=[]
    for account in ['A1','A2','A3']:
        z=cp[(cp.account==account)&(cp.other_referral_assumed==0)]
        first=z.iloc[0]
        cc=getcost(account,'aligned_points_window')
        vals=[z[abs(z.spread_bp_assumed-s)<1e-9].cost_per_own_point_usd.iloc[0] for s in (0,.2,1)]
        costrows.append([account+(' (추천 가정)' if account=='A1' else ''),f(cc['volume_usd'],2),f(cc['fees_usd'],4),f(first.own_points_conditional,6),f(vals[0],3),f(vals[1],3),f(vals[2],3),f(cc['maker_volume_fraction']*100,2)+'%'])
    add(table(["계정","거래대금 USD","Fee USD","자체 포인트","s=0 $/pt","s=0.2 $/pt","s=1 $/pt","메이커 거래대금 비중"],costrows))
    add("\n기간: A1 9/19 02:35→9/27 15:00, A2 9/19 02:42→9/27 15:00, A3 9/19 03:59→9/26 10:40 UTC. A3 스냅샷 이후 Fee $200.1340은 제외했다. A2 메이커 비중은 체결 횟수가 아니라 금액 기준이다.\n")
    z=cp[(cp.account=='A1')&(cp.spread_bp_assumed==0)]
    add(table(["A1의 다른 추천 수입 가정 pt","자체 포인트","Fee만 $/pt"],[[f(r.other_referral_assumed,0),f(r.own_points_conditional,3),f(r.cost_per_own_point_usd,3)] for _,r in z.iterrows()]))
    add(f"\n위 A1 표는 A3 추천분 {f(a1.a3_referral_model,3)}pt를 전제한다. 기존 약 $3.5/pt는 다른 추천 수입을 사실상 0으로 두는 조건부 값이다. A1의 정확한 자체 포인트와 총 거래원가는 식별되지 않는다. 추천 수입을 전혀 빼지 않은 ${f(e['a1_all_points_fee_ratio_invalid_own_denominator'],3)}/pt는 자체 활동 비용 지표로 사용할 수 없다.\n")
    add("이 표는 **해당 기간에 발생한 수수료 / 해당 기간에 지급된 포인트**다. 금요일 드롭은 기간 시작 전 거래를 보상할 수 있고 종료 전 거래의 다음 드롭은 아직 포함되지 않아, 활동 발생분을 정확히 대응시킨 원가나 한계 원가가 아니다. 보유 중인 포지션의 미래 청산 수수료도 빠져 있다. 펀딩·헤지 가격손익은 요청된 Fee+spread 원가 정의와 별도다.\n")
    qvol=m['rt_local_a2']+m['drop_volume_coeff']
    add(f"순수 거래량의 조건부 모형은 {f(qvol,2)} pt/$1M이다. 수수료 3.41bp라는 예시에서 $1M 거래 비용은 spread 0.2–1bp를 더해 $361–441, 따라서 ${f(361/qvol,2)}–{f(441/qvol,2)}/pt다. 실제 A3/A2 수수료율, r의 적용 시장, 드롭 비선형성이 달라 이 값을 보편적인 거래 전략 원가로 외삽할 수 없다.\n")
    add("## 6. OI 보유의 APR 시나리오\n")
    q=e['oi_rate_cases']['fitted_oi_drop']
    add("가정: 배정 풀 11,000,000 LIT 전체를 최종 포인트 비율로 배분, 수수료·펀딩·가격손익 차감 전, 단리 52주 연환산. 프로그램 종료일·확정 교환비율은 제공되지 않았다. 아래 값은 연환산 보상률이며 1년 동안 계속 지급된다는 뜻이 아니다.\n")
    add("```text\nL = 모든 거래소의 양쪽 gross OI 합계 / 모든 거래소에 투입한 총 자기자본\nvalue_per_point = 11,000,000 × LIT_price / program_end_total_points\nAPR = 52 × q × L × eligible_fraction × value_per_point / 1,000,000\nA: 양쪽이 Lighter에 있어 eligible_fraction=1\nB: 동일 명목의 한쪽만 Lighter에 있어 eligible_fraction=0.5\n```\n")
    add("B를 절반으로 하는 것은 위 gross OI와 **양쪽 총자본**의 분모가 같을 때뿐이다. Lighter 쪽 OI/자본만으로 레버리지를 정의하면 단순히 절반으로 바꾸면 안 된다.\n")
    add(f"중심 시나리오 q=l+b={f(q,4)} pt/$1M·주. 선택한 선형 모형 민감도에서 q는 {f(e['oi_rate_cases']['conditional_sensitivity_low'],2)}–{f(e['oi_rate_cases']['conditional_sensitivity_high'],2)}다. OI 드롭이 없다는 별도 구조에서는 q=5.5만 남는다. 이 구조적 불확실성을 숫자 범위 안에 숨기지 않았다.\n")
    values=apr[(apr.rate_case=='fitted_oi_drop')&(apr.structure=='A')&(apr.gross_all_venues_oi_to_total_equity==10)]
    add(table(["LIT USD","최종 1.5M pt: $/pt","2.0M","3.6M","5.2M"],[[str(price)]+[f(values[(values.lit_price_usd==price)&(values.program_end_points==total)].usd_per_point.iloc[0],2) for total in [1500000,2000000,3600000,5200000]] for price in [3,4,5]]))
    add("\n**조건부 OI 드롭 포함 APR. 각 셀은 A / B (%).**\n")
    rows=[]
    for price in [3,4,5]:
        for lev in [10,15,20]:
            z=apr[(apr.rate_case=='fitted_oi_drop')&(apr.lit_price_usd==price)&(apr.gross_all_venues_oi_to_total_equity==lev)]
            rows.append([f(price,0),f(lev,0)+'x']+[f(z[(z.program_end_points==total)&(z.structure=='A')].gross_apr_pct.iloc[0],2)+' / '+f(z[(z.program_end_points==total)&(z.structure=='B')].gross_apr_pct.iloc[0],2) for total in [1500000,2000000,3600000,5200000]])
    add(table(["LIT USD","gross 레버리지","최종 1.5M pt","2.0M","3.6M","5.2M"],rows))
    add("\n**OI 드롭을 인정하지 않는 라이브만의 APR. 각 셀은 A / B (%).**\n")
    rows=[]
    for price in [3,4,5]:
        for lev in [10,15,20]:
            z=apr[(apr.rate_case=='live_only')&(apr.lit_price_usd==price)&(apr.gross_all_venues_oi_to_total_equity==lev)]
            rows.append([f(price,0),f(lev,0)+'x']+[f(z[(z.program_end_points==total)&(z.structure=='A')].gross_apr_pct.iloc[0],2)+' / '+f(z[(z.program_end_points==total)&(z.structure=='B')].gross_apr_pct.iloc[0],2) for total in [1500000,2000000,3600000,5200000]])
    add(table(["LIT USD","gross 레버리지","최종 1.5M pt","2.0M","3.6M","5.2M"],rows))
    add(f"\n선형 모형 민감도 APR은 중심 표에 {f(e['oi_rate_cases']['conditional_sensitivity_low']/q,3)}–{f(e['oi_rate_cases']['conditional_sensitivity_high']/q,3)}배, 기존 q=15.5 시나리오는 {f(15.5/q,3)}배다. 모든 72개 조합에 대해 중심·라이브만·민감도 양끝·기존 가정을 `apr_scenarios.csv`에 저장했다. 수익 보장 하한을 의미하지 않는다.\n")
    add("비용 차감 후 비교에는 보유 기간과 외부 거래소 비용이 필요하다. A의 동일 수수료·spread 예시는 `APR_net ≈ APR_gross − (52/H) × 2L × (fee_bp+s_bp)/10000 − annual_funding_cost/equity`다(H=보유 주수). 거래량으로 받는 진입·청산 포인트는 위 OI 전용 APR에 포함하지 않았다. B는 외부 거래소의 수수료와 펀딩을 별도로 더해야 한다.\n")
    tx=c('oi_transaction_cost_scenarios.csv')
    tx=tx[(tx.leverage==20)&(tx.spread_bp_assumed==0)]
    add(table(["예시: 20x, Fee 3.41bp, spread 0","왕복 비용 / 자본 %","연환산 비용 차감 %p"],[[str(int(r.holding_weeks))+'주 보유',f(r.round_trip_cost_pct_equity,3),f(r.annualized_cost_drag_percentage_points,3)] for _,r in tx.iterrows()]))
    add(f"\n펀딩이 상쇄된다는 가정은 같은 시장에서 같은 명목의 반대 포지션을 같은 기간 유지할 때 적용할 수 있다. SPY/QQQ, BTC/ETH처럼 서로 다른 시장이면 자동으로 상쇄되지 않는다. 제공된 9/19 포지션 표의 Funding 누적 합도 A1 ${f(e['cumulative_funding_usd_at_position_snapshots']['A1'],2)}, A2 ${f(e['cumulative_funding_usd_at_position_snapshots']['A2'],2)}로 0이 아니다. 누적 시작시각·현금흐름 원장이 없어 이 값을 미래 펀딩 APR로 환산하지 않았다.\n")
    add("## 7. 플랫폼 전체 포인트와 증가율\n")
    add("각 계정의 누적 포인트가 감소·삭제되지 않고 rank r이 최소 r명의 보유자를 뜻한다고 가정한다. 같은 날뿐 아니라 과거의 rank·points 제약을 누적해 각 순위가 가질 최소값을 합산했다. 미관측 계정의 수에 대한 정보가 없으므로 유한한 총량 상한은 없다.\n")
    add(table(["기준 UTC","총포인트 관측 하한","상한"],[[r['as_of_utc'],f(r['total_points_lower'],3),'식별 불가'] for r in p['total_bounds']]))
    ns=p['near_synchronous_model']
    add(f"\n9/19 근접 시각의 top 10 합은 {f(ns['top_10_points'],3)}pt. top 10과 rank 29/115/1643을 log-log 보간하고 마지막 구간 지수 α={f(ns['tail_alpha_from_ranks_115_1643'],4)}를 바깥 순위까지 연장하면 다음과 같다. 계정 수 N과 미관측 꼬리 모양을 가정한 값이지 신뢰구간이 아니다.\n")
    add(table(["가정한 포인트 계정 수 N","9/19 총포인트 모형값"],[[str(r['population_N']),f(r['total_model_points'],0)] for r in ns['finite_N_scenarios_at_observed_alpha']]))
    g=p['growth']
    add(f"\n따라서 기존 0.5M은 가능한 시나리오이지만 확정 분모가 아니다. 9/19 03:59→9/28 03:42의 {f(g['elapsed_days'],6)}일 동안 top 2 합은 {f(g['top_two']['points_change'],3)}pt 증가했다(누적 {f(g['top_two']['relative_change']*100,2)}%, 복리 환산 주당 {f(g['top_two']['compound_change_per_7_days']*100,2)}%). 여기에 별도 계정 A3 증가 {f(g['a3_change'],6)}pt를 더하면 플랫폼 증가는 최소 {f(g['platform_absolute_change_lower'],6)}pt, 단순 7일 환산 최소 {f(g['platform_average_weekly_change_lower'],2)}pt/주다. 계정별 누적이 감소하지 않는다는 조건에서 순위의 주인이 달라도 성립하는 하한이며, 전체 증가량의 추정치는 아니다.\n")
    add("top 2의 증가율을 전체 계정에 적용할 근거가 없다. 전체 주간 증가 100K–150K는 이 스냅샷으로 식별되지 않으며, 위 두 날짜 총량 하한의 차이를 전체 증가량이나 증가량 하한으로 사용해서도 안 된다. 총량·증가량 상한과 플랫폼 전체 증가율은 미확인이다.\n")
    add("## 8. 기존 결과와의 대조\n")
    add(table(["기존 주장","독립 검증 판정"],[
        ["live 5.5, 선형·시장 공통","5.5 근방은 재현. 선형성은 양립 가능하지만 시장 공통은 미입증"],
        ["체결 시 4.6 pt/$M","A2 HYPE 메이커 4.575 재현. 즉시성·타시장 일반화는 미확인"],
        ["드롭 V 45–50 + OI 8–11",f"A2 실효 V 계수를 OI와 분리해야 함. 공동 적합 {f(m['drop_volume_coeff'],2)} + {f(m['drop_oi_coeff'],2)}, 공식 확정 불가"],
        ["추천 10%가 모든 성분에 적용","라이브는 일치 가능. 드롭·거래 크레딧의 개별 적용 미식별"],
        ["Fee 원가 A1 3.5 / A2 2.35 / A3 2.64","A2/A3 재현. A1은 다른 추천 수입을 거의 0으로 둔 조건부 값"],
        ["spread +0.2/−0.6bp","기준 tick 미포함으로 재현 불가"],
        ["OI 합계 15.5 기반 APR","산술은 재현 가능. OI 드롭 불확실성을 별도 시나리오로 반영"],
        ["총량 0.5M, 증가 100–150K/주","총량은 꼬리 가정에 의존. 전체 증가율·증가량은 미식별"]]))
    add("\n## 9. 아직 식별되지 않은 것과 다음 측정\n")
    add("미식별: 실제 mark 시계열, 정확한 활동 cutoff와 지급 지연, Standard 상대방에 따른 적격 거래, 시장·계정별 가중치, 드롭의 OI 대 비선형 거래량 효과, 전체 추천 원장, spread, 시점이 맞는 활동 발생분 원가, 미래 펀딩·외부 거래소 비용, 전체 포인트 계정 수·분포·성장, 최종 교환비·종료일.\n")
    add("1. **피추천인이 없는 A3와 A2를 한 주 전체 관측한다.** 신규 거래 없이 기존 OI를 유지한 구간이 가능하면, 활동 주 시작 전부터 다음 드롭 이후까지 포인트·수량·mark·Funding을 동기화해 기록한다. 기존 거래 주간이 끝나야 '드롭이 무거래 주에 발생했다'는 검증이 된다. A3의 큰 OI와 A2의 작은 OI를 비교하면 OI 성분 식별력이 가장 크다.\n2. **A1 추천 원장의 분해값을 받는다.** A1과 A3의 자체/추천 포인트를 같은 UTC 초에 드롭 직전·직후 기록하고 다른 피추천인도 같은 구간을 확보한다. 이것이 A1 원가의 가장 큰 불확실성을 줄인다.\n3. **실제 필요한 소규모 거래를 관측한다.** 기존 OI가 안정적인 구간에서 거래 직전부터 직후까지 초 단위 포인트, 체결 role, 시장, Fee, 적격 여부를 기록한다. 일반 거래로 HYPE/주식·maker/taker를 구분해 r의 범위와 지급 지연을 확인한다.\n4. **시장별 OI와 주간 거래량을 따로 기록한다.** 다른 계정·시장·기간이 뒤섞이지 않는 비교가 있어야 주식/크립토 가중치와 거래량 비선형을 구분할 수 있다.\n5. **동시각 전체 리더보드와 quote를 보관한다.** 전체 포인트 보유 계정 수와 말단 순위, 같은 시각 전체 합을 매주 얻고, 원가를 위해 주문 도착시 midpoint·체결가·외부 헤지 체결을 저장한다.\n")
    add("## 10. 출처와 재현\n")
    add("원본: `data/lighter/*export*`, `points_snapshots.csv`, `positions_snapshots.csv`, `leaderboard_snapshots.csv`, `referrals.csv`, `fee_tiers.csv`, `program_facts.md`. `prior_results.md`는 검증 대상 가설이며 추정의 정답으로 사용하지 않았다. `analysis/input_manifest.json`에 실제 입력 SHA-256을 기록한다.\n")
    add("공개 문서 확인: [Robinhood Chain points](https://docs.lighter.xyz/points-program/lighter-on-robinhood-chain-points)는 조회 도구에서 열리지 않아 11M 풀·10% 추천 규칙은 제공된 `program_facts.md`의 사실관계/시나리오로 두었다. [과거 Retail 문서](https://docs.lighter.xyz/points-program/retail)는 가중치·비선형과 여러 보상 항목을 설명하지만 별도 시즌이므로 그 예산·주간 cutoff를 이 프로그램에 이식하지 않았다. [현재 Trading Fees](https://docs.lighter.xyz/trading/trading-fees)는 제공 수수료표와 다른 구조를 표시하므로 과거 체결의 비용은 실제 Fee 열로 계산했다. 문서 확인은 모델 입력이나 재실행에 필요하지 않으며 거래소 API는 호출하지 않았다.\n")
    add("재현 명령(저장소 루트):\n\n```bash\npython3 -m pip install -r handoff/analysis/requirements.txt\npython3 handoff/analysis/run_all.py\n```\n\n`core.py`: 원본·OI 적분, `model.py`: 포인트 적합·분해·민감도, `costs.py`: 수수료 독립 합계, `platform_points.py`: 총량·성장, `independent_audit.py`: 다른 가격 방식 검증, `economics.py`: 원가·APR, `report.py`: 이 문서 생성, `verify.py`: 독립 스냅샷·Decimal 합계·산출물 검증. 모든 분석은 로컬 자료로 재실행된다.\n")
    tvl_note=HERE/'tvl_notes.md'
    if tvl_note.exists():
        add(tvl_note.read_text())
    counterpart=HERE/'counterparty_summary.json'
    if counterpart.exists():
        ct=json.loads(counterpart.read_text())
        matched=float(ct['single_sided_matched_notional_usd'])
        pt=matched/1e6*(m['rt_local_a2']+m['drop_volume_coeff'])
        add('## 후속 검토: A1/A3 체결 중복과 포인트 제외 가능성\n')
        add(f"원본 두 export를 Trade ID로 대조하면 {ct['shared_trade_id_count']}건이 겹치며, 시각·시장·가격·수량·금액도 모두 같다. 경제적 방향은 반대이고 A1 Maker / A3 Taker다. 체결금액을 양쪽 중복 없이 합치면 ${matched:,.6f}이며 A1 누적 거래대금의 {ct['matched_notional_share_of_lifetime_pct']['A1']:.6f}%, A3의 {ct['matched_notional_share_of_lifetime_pct']['A3']:.6f}%다. BTC/ETH/ZEC에서 9/22–23에 발생했고 SPY/QQQ에는 공통 ID가 없다.\n")
        add('이는 일부 주문이 서로 상대방이 된 체결이라는 강한 근거이지만, 의도적인 맞거래·공동 통제·보상 제재의 증거는 아니다. [공식 Trade 객체](https://apidocs.lighter.xyz/docs/websocket-reference)는 trade_id와 매수·매도 계정 ID를 함께 기록하는 구조다. 다만 제공 CSV에는 상대방 주소와 적격 여부 플래그가 없다. 상세 기록은 `analysis/counterparty_shared_ids.csv`에 있다.\n')
        add(f"위 소액 체결의 거래량 보상만 제외한다고 가정하면 기존 선형 계수로 계정당 약 {pt:.3f}pt, A1의 A3 추천분까지 포함해 약 {1.1*pt:.3f}pt다. 이는 조건부 산술이며 실제 제외가 있었는지는 모른다. 계정 전체에 대한 다른 할인·제외 규칙의 존재와 적용 여부도 이 자료로 확인할 수 없다.\n")
        add('SPY/QQQ의 같은 초 반대 방향 그룹은 모두 Taker/Taker로, 통상 서로 다른 resting order를 체결한 경우다. 방향이 반대라는 사실만으로 직접 체결이나 OI 상쇄를 추론하지 않는다. [기존 Retail 공식 문서](https://docs.lighter.xyz/points-program/retail)는 시빌·파밍 판별 지표를 공개하지 않는다고 설명한다. 이 문서만으로 Robinhood 프로그램에 특정 제재 기준을 적용할 수 없으며, 레퍼럴 연결과 반대 포지션만으로 자동 상쇄한다는 공개 규칙은 확인하지 못했다. 모델 잔차 역시 페널티 원장 대신 쓸 수 없다.\n')
    (HERE.parent/'RESULTS.md').write_text('\n'.join(out),encoding='utf-8')


if __name__=='__main__':
    run()
