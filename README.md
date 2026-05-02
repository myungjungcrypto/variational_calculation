# Variational 슬리피지 분석

`slippage.py` 는 Variational 체결 내역(`export-trades.csv`)을 같은 시각의 Binance 1분봉 가격과 비교해 **코인별 슬리피지 손실 (수수료 등가) 비율** 을 계산합니다.

## 사용

```bash
pip install pandas
python slippage.py
```

처음 실행하면 코인별로 Binance Futures(USDT-M) 또는 Spot 1분봉을 다운로드해서 `klines/` 디렉터리에 캐시합니다 (총 100MB 안팎). 두번째부터는 캐시를 사용해 즉시 계산.

## 출력 파일

- `slippage_per_coin.csv` — 코인별 요약
  - `total_notional_usdc` 총 거래대금
  - `raw_slip_usdc`, `raw_slip_pct` — Binance 마크가 그대로를 기준으로 한 슬리피지
  - `adj_slip_usdc`, `adj_slip_pct` — 코인별 중앙값 베이시스를 제거한 슬리피지 (Variational 의 구조적 디스카운트/프리미엄 영향 제거)
  - `median_basis_pct` — Variational 가 Binance 대비 평균 얼마나 싸게(혹은 비싸게) 거래되는지
  - `half_spread_usdc` — `|executed - adj_mark| * qty` 누적 (체결가가 공정가에서 벗어난 절대량)
  - `avg_half_spread_pct` — 거래대금 가중 평균 half-spread 비율 = `half_spread_usdc / total_notional_usdc`
  - `avg_full_spread_pct` — 추정 평균 bid-ask 스프레드 (= 2 × half-spread). 사용자가 항상 테이커였다고 가정한 상한선
  - `market` — futures / spot / unsupported
- `slippage_per_trade.csv` — 체결별 상세

## 슬리피지 계산식

체결마다:

```
side_sign      = +1 if buy else -1
slip_per_unit  = (executed_price - mark) * side_sign
slip_cost_usdc = slip_per_unit * qty   # 양수 = 손실, 음수 = 이득
```

`mark` 는 체결시각이 속한 1분봉의 `open` 과 다음 봉 `open` 사이를 선형보간한 값.

`adj_*` 는 `mark` 에서 코인별 중앙값 basis(Binance - Variational)를 빼서, 거래소간 구조적 가격차이를 제거한 후 같은 식을 적용한 값.
