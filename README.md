# Variational 슬리피지 분석

두 가지 분석 스크립트가 있습니다:

- **`slippage_tick.py` (권장)** — Binance USDT-Perp **aggTrades 틱** (~50ms 해상도) 기준. `data.binance.vision` 공개 아카이브 사용. 이게 정확함.
- **`slippage.py`** — Binance 1분봉 보간 기준. 빠르지만 ±5~10 bp 노이즈 있음.

## 사용

```bash
pip install pandas numpy
python slippage_tick.py                       # default: export-trades.csv
python slippage_tick.py export-trades_d.csv   # 다른 trades 파일

python trade_pnl.py                           # default
python trade_pnl.py export-trades_d.csv       # 다른 trades 파일
```

입력 파일이 `export-trades_d.csv` 면 출력은 `slippage_tick_per_coin_d.csv`,
`trade_pnl_d.csv` 처럼 자동으로 `_d` suffix 가 붙습니다.

## 틱 버전 (`slippage_tick.py`)

`Variationalca.xlsx` 의 audit 방법론 그대로:

1. 각 fill 의 `transact_time` 에 가장 가까운 Binance perp aggTrade 검색 (1초 이내 매칭)
2. signed slippage (bps): `sign × (fill_price − reference) / reference × 10000`, sign=+1(buy)/-1(sell), 양수=불리
3. 코인별/방향별/사이즈별 가중평균 + adverse 비율

출력:
- `slippage_tick_per_coin.csv` — 코인별 가중 bps + buy/sell bps + 매도 adverse 비율
- `slippage_tick_per_trade.csv` — 체결별 reference price, gap_ms, slip_bps, slip_usd
- `slippage_tick_asymmetry.csv` — 코인 × side 단위 비대칭 표
- `slippage_tick_size_buckets.csv` — `notional ≥ $200K` (큰 거래) 버킷 분리

처음 실행 시 daily aggTrades zip 다운로드 (수~수십 GB). `ticks/<SYMBOL>/<SYMBOL>-aggTrades-YYYY-MM-DD.csv.gz` 로 캐시.

## 1분봉 버전 (`slippage.py`)

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
