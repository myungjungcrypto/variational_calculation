"""
OKX 1초봉 크로스체크.

slippage_tick_per_trade<suffix>.csv 의 체결들을 OKX USDT-SWAP 1초봉과
재대조한다 (audit 의 Cross-Check 1 과 동일한 방법):

  reference = 체결 시각이 속한 1s bar 의 (high + low) / 2
  slip_bps  = sign * (fill - reference) / reference * 10000   (buy=+1, sell=-1)

Binance 틱 기준 결과와 나란히 출력해서 두 비너스가 같은 답을 주는지 확인.

candles 는 okx_cache/<INST>.csv.gz 에 캐시되어 재실행 시 API 호출 없음.

사용
----
$ python okx_crosscheck.py _btc_260803          # BTC (기본)
$ python okx_crosscheck.py _btc_260803 ETH      # 다른 코인
"""

from __future__ import annotations

import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
CACHE_DIR = ROOT / "okx_cache"
CACHE_DIR.mkdir(exist_ok=True)

API = "https://www.okx.com/api/v5/market/history-candles"
USER_AGENT = "Mozilla/5.0 okx-crosscheck"
BATCH = 100          # candles per request (API max for history-candles)
SLEEP_S = 0.15       # stay well under OKX public rate limits
BAR_MS = 1_000


def inst_for(coin: str) -> str:
    return f"{coin}-USDT-SWAP"


def _get(params: dict, retries: int = 5) -> list:
    url = f"{API}?{urllib.parse.urlencode(params)}"
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=30) as r:
                out = json.loads(r.read())
            if out.get("code") != "0":
                raise RuntimeError(f"okx error {out.get('code')}: {out.get('msg')}")
            return out["data"]
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 ** i)
    raise RuntimeError(f"okx request failed: {last}")


def fetch_bars(inst: str, need_ts: list[int], cache: dict[int, tuple[float, float]]) -> int:
    """Fetch 1s bars covering every needed bar-start timestamp (ms).
    Fills `cache` {bar_ts: (high, low)}. Returns number of API calls made."""
    missing = sorted({t for t in need_ts if t not in cache})
    calls = 0
    i = len(missing) - 1
    # walk from newest to oldest; each call covers up to BATCH seconds ending at anchor
    while i >= 0:
        anchor = missing[i]
        data = _get({"instId": inst, "bar": "1s",
                     "after": str(anchor + BAR_MS), "limit": str(BATCH)})
        calls += 1
        got = set()
        for row in data:  # newest first: [ts, o, h, l, c, ...]
            ts = int(row[0])
            cache[ts] = (float(row[2]), float(row[3]))
            got.add(ts)
        if not data:
            # no data at this anchor (delisted period etc.) - skip this second
            i -= 1
            continue
        oldest = min(got)
        # advance past every missing ts now covered
        while i >= 0 and missing[i] >= oldest:
            i -= 1
        time.sleep(SLEEP_S)
    return calls


def load_cache(path: Path) -> dict[int, tuple[float, float]]:
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    return {int(t): (h, l) for t, h, l in zip(df["ts"], df["high"], df["low"])}


def save_cache(path: Path, cache: dict[int, tuple[float, float]]) -> None:
    df = pd.DataFrame(
        {"ts": list(cache.keys()),
         "high": [v[0] for v in cache.values()],
         "low": [v[1] for v in cache.values()]}
    ).sort_values("ts")
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False, compression="gzip")
    tmp.replace(path)


def wavg(bps: pd.Series, w: pd.Series) -> float:
    if w.sum() == 0:
        return float("nan")
    return float((bps * w).sum() / w.sum())


def main() -> int:
    suffix = sys.argv[1] if len(sys.argv) > 1 else ""
    coin = sys.argv[2] if len(sys.argv) > 2 else "BTC"
    src = ROOT / f"slippage_tick_per_trade{suffix}.csv"
    if not src.exists():
        print(f"missing: {src}  (run slippage_tick.py first)")
        return 1

    df = pd.read_csv(src)
    df = df[df["underlying"] == coin].copy()
    if df.empty:
        print(f"no {coin} fills in {src.name}")
        return 1
    df["bar_ts"] = (df["ts_ms"] // BAR_MS) * BAR_MS

    inst = inst_for(coin)
    cache_path = CACHE_DIR / f"{inst}{suffix}.csv.gz"
    cache = load_cache(cache_path)
    print(f"{inst}: {len(df):,} fills, {df['bar_ts'].nunique():,} unique seconds, "
          f"{len(cache):,} bars cached")

    calls = fetch_bars(inst, df["bar_ts"].tolist(), cache)
    if calls:
        save_cache(cache_path, cache)
        print(f"fetched with {calls} API calls; cached {len(cache):,} bars")

    highs = df["bar_ts"].map(lambda t: cache.get(t, (np.nan, np.nan))[0])
    lows = df["bar_ts"].map(lambda t: cache.get(t, (np.nan, np.nan))[1])
    df["okx_ref"] = (highs + lows) / 2
    sign = np.where(df["side"] == "buy", 1, -1)
    df["okx_slip_bps"] = sign * (df["price"] - df["okx_ref"]) / df["okx_ref"] * 10_000
    df["okx_slip_usd"] = df["okx_slip_bps"] / 10_000 * df["notional"]

    ok = df[df["okx_ref"].notna()].copy()
    print(f"okx matched: {len(ok):,}/{len(df):,} "
          f"({100.0 * len(ok) / len(df):.1f}%)")

    out_csv = ROOT / f"okx_crosscheck{suffix}.csv"
    keep = ["id", "created_at", "underlying", "side", "qty", "price", "notional",
            "reference", "slip_bps", "slip_usd", "okx_ref", "okx_slip_bps", "okx_slip_usd"]
    df[[c for c in keep if c in df.columns]].to_csv(out_csv, index=False)

    # side-by-side summary on the SAME fills (matched by both venues)
    both = ok[ok["slip_bps"].notna()]
    print(f"\n=== {coin}: Binance tick vs OKX 1s mid (same {len(both):,} fills) ===")
    rows = []
    for venue, col in (("binance_tick", "slip_bps"), ("okx_1s_mid", "okx_slip_bps")):
        buys = both[both["side"] == "buy"]
        sells = both[both["side"] == "sell"]
        b = wavg(buys[col], buys["notional"])
        s = wavg(sells[col], sells["notional"])
        rows.append({
            "venue": venue,
            "buy_bps": round(b, 3),
            "sell_bps": round(s, 3),
            "mid_bps": round((b + s) / 2, 3),
            "weighted_bps": round(wavg(both[col], both["notional"]), 3),
            "net_usd": round(float((both[col] / 10_000 * both["notional"]).sum()), 2),
        })
    print(pd.DataFrame(rows).to_string(index=False))
    print(f"\nwrote: {out_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
