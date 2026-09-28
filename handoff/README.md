# handoff/ — 다른 AI(Codex 등)에게 넘기는 패키지

이 폴더만 통째로 넘기면 됩니다. 시작점은 `TASK.md`.

```
handoff/
├─ TASK.md                 ← Codex에게 줄 임무 (목표, 데이터 설명, 산출물)
├─ AGENTS.md               ← Codex가 자동으로 읽는 진입점 (TASK.md로 안내)
├─ prior_results.md        ← 기존 분석 결과 (검증 대상, 정답 아님)
└─ data/
   ├─ lighter/
   │   ├─ a1_lighter_export_2026-09-27T15_00Z.csv       A1 전체 거래 (9/2~9/27)
   │   ├─ a2_lighter_export_2026-09-27T15_04Z.csv       A2 전체 거래 (9/4~9/23)
   │   ├─ a3_lighter_export_2026-09-27T10_40Z.csv.gz    A3 전체 거래 (9/3~9/27, gzip)
   │   ├─ points_snapshots.csv       포인트 스샷 전부 (UTC 시각, 무거래 여부 메모)
   │   ├─ positions_snapshots.csv    포지션 스샷 (마크가 OI, 펀딩, 청산가)
   │   ├─ leaderboard_snapshots.csv  리더보드 상위권 스샷
   │   ├─ referrals.csv              A1 레퍼럴 페이지 (A3 = 0xc2...9664)
   │   ├─ fee_tiers.csv              Standard/Premium 수수료표
   │   └─ program_facts.md           공개 룰 + 계정 사실관계
   └─ variational/                   (선택) Variational 쪽 데이터 일체
       ├─ *.csv, Variationalca.xlsx
       └─ points_observations.md     주간 포인트 분배표 4계정 + 플랫폼 볼륨/OI
```

Codex 실행 예시:
```
codex "Read handoff/AGENTS.md and complete handoff/TASK.md. Write RESULTS.md and analysis/ scripts."
```
