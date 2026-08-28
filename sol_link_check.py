"""
두 Solana 주소의 온체인 연결고리 분석.

확인 항목
---------
1. 직접 상호작용: 같은 트랜잭션 시그니처에 두 주소가 함께 등장하는지
2. 공통 자금원: 각 주소의 최초 트랜잭션들에서 SOL을 보낸 주소(펀더) 비교
3. 공통 상대방: 최근 트랜잭션에서 상호작용한 주소들의 교집합

사용
----
$ pip install requests
$ python sol_link_check.py <ADDR1> <ADDR2>
$ python sol_link_check.py <ADDR1> <ADDR2> --rpc https://your-rpc-url

공개 RPC(api.mainnet-beta.solana.com)는 레이트리밋이 빡빡해서 요청 사이에
딜레이를 둡니다. 429가 계속 나면 Helius/QuickNode 무료 키를 --rpc 로 넘기세요.
"""

from __future__ import annotations

import argparse
import sys
import time

import requests

DEFAULT_RPC = "https://api.mainnet-beta.solana.com"
SIG_LIMIT = 1000     # signatures to pull per address (max 1000 per call)
TX_SAMPLE = 25       # earliest/latest transactions to parse per address
SLEEP = 0.4          # seconds between RPC calls


def rpc(url: str, method: str, params: list, retries: int = 5):
    for i in range(retries):
        try:
            r = requests.post(url, json={"jsonrpc": "2.0", "id": 1,
                                         "method": method, "params": params},
                              timeout=30)
            if r.status_code == 429:
                time.sleep(2 ** i)
                continue
            r.raise_for_status()
            out = r.json()
            if "error" in out:
                raise RuntimeError(out["error"])
            return out["result"]
        except requests.RequestException as e:
            if i == retries - 1:
                raise
            time.sleep(2 ** i)
    raise RuntimeError("rpc retries exhausted")


def get_signatures(url: str, addr: str) -> list[dict]:
    """All (up to SIG_LIMIT pages... practically capped) signatures, newest first."""
    sigs = []
    before = None
    while True:
        params = [addr, {"limit": SIG_LIMIT}]
        if before:
            params[1]["before"] = before
        batch = rpc(url, "getSignaturesForAddress", params)
        if not batch:
            break
        sigs.extend(batch)
        if len(batch) < SIG_LIMIT or len(sigs) >= 5000:
            break
        before = batch[-1]["signature"]
        time.sleep(SLEEP)
    return sigs


def get_tx_accounts(url: str, sig: str):
    """Return (account_keys, pre/post SOL delta per account) for a tx."""
    tx = rpc(url, "getTransaction",
             [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
    if tx is None:
        return None
    keys = [k["pubkey"] for k in tx["transaction"]["message"]["accountKeys"]]
    pre = tx["meta"]["preBalances"]
    post = tx["meta"]["postBalances"]
    delta = {k: (post[i] - pre[i]) / 1e9 for i, k in enumerate(keys)}
    return keys, delta


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("addr1")
    ap.add_argument("addr2")
    ap.add_argument("--rpc", default=DEFAULT_RPC)
    args = ap.parse_args()
    url = args.rpc
    a1, a2 = args.addr1, args.addr2

    print(f"fetching signatures for {a1[:8]}... ", end="", flush=True)
    s1 = get_signatures(url, a1)
    print(f"{len(s1)} sigs")
    time.sleep(SLEEP)
    print(f"fetching signatures for {a2[:8]}... ", end="", flush=True)
    s2 = get_signatures(url, a2)
    print(f"{len(s2)} sigs")

    set1 = {s["signature"] for s in s1}
    set2 = {s["signature"] for s in s2}

    # 1. direct interaction
    common = set1 & set2
    print(f"\n[1] 공통 트랜잭션 (직접 상호작용): {len(common)}건")
    for sig in list(common)[:10]:
        print(f"    {sig}")
    if len(common) > 10:
        print(f"    ... and {len(common)-10} more")

    # 2. funding source: earliest few txs of each address
    def funders(addr, sigs):
        out = {}
        for s in sorted(sigs, key=lambda x: x.get("blockTime") or 0)[:TX_SAMPLE]:
            time.sleep(SLEEP)
            try:
                res = get_tx_accounts(url, s["signature"])
            except Exception as e:  # noqa: BLE001
                print(f"    [warn] {s['signature'][:16]}...: {e}")
                continue
            if res is None:
                continue
            keys, delta = res
            if delta.get(addr, 0) > 0:  # this address RECEIVED SOL
                # candidates: accounts that lost more than the fee
                for k, d in delta.items():
                    if k != addr and d < -0.000005:
                        out.setdefault(k, 0)
                        out[k] += -d
        return out

    print(f"\n[2] 최초 자금원 분석 (각 주소의 가장 오래된 {TX_SAMPLE}개 tx 중 SOL 수신분)")
    f1 = funders(a1, s1)
    f2 = funders(a2, s2)
    print(f"    {a1[:8]}... funders: {len(f1)}")
    for k, v in sorted(f1.items(), key=lambda x: -x[1])[:5]:
        print(f"      {k}  ({v:.4f} SOL)")
    print(f"    {a2[:8]}... funders: {len(f2)}")
    for k, v in sorted(f2.items(), key=lambda x: -x[1])[:5]:
        print(f"      {k}  ({v:.4f} SOL)")
    shared_funders = set(f1) & set(f2)
    print(f"    ==> 공통 자금원: {len(shared_funders)}")
    for k in shared_funders:
        print(f"      {k}  (sent {f1[k]:.4f} SOL to addr1, {f2[k]:.4f} SOL to addr2)")

    # 3. common counterparties in recent txs
    def counterparties(addr, sigs):
        out = set()
        for s in sigs[:TX_SAMPLE]:
            time.sleep(SLEEP)
            try:
                res = get_tx_accounts(url, s["signature"])
            except Exception:
                continue
            if res is None:
                continue
            keys, _ = res
            out |= set(keys)
        out.discard(addr)
        return out

    print(f"\n[3] 최근 {TX_SAMPLE}개 tx 상대방 교집합 (프로그램/시스템 계정 포함 주의)")
    c1 = counterparties(a1, s1)
    c2 = counterparties(a2, s2)
    shared = c1 & c2
    print(f"    addr1 counterparties: {len(c1)}, addr2: {len(c2)}, 교집합: {len(shared)}")
    for k in list(shared)[:15]:
        print(f"      {k}")

    print("\n해석 가이드:")
    print(" - [1] 공통 tx가 있으면 직접 전송/상호작용 확정")
    print(" - [2] 공통 자금원이 CEX 출금 지갑이면 같은 소유자일 가능성 높음")
    print(" - [3] 교집합은 프로그램 ID(예: Token Program)가 대부분이므로 지갑형 주소만 의미 있음")
    return 0


if __name__ == "__main__":
    sys.exit(main())
