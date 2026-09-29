# Platform points: bounds and tail sensitivity

Only the bundled leaderboard and account point CSVs are used. Bounds assume per-account cumulative points never decrease or disappear and standard rank semantics; approximate screenshot times are accepted as supplied.

- By 2026-09-19 03:59 UTC: at least 275,441.128 points.
- By 2026-09-28 13:00 UTC: at least 337,899.979 points.
- Neither date has a finite total upper bound: account count and the unobserved tail are not supplied.

Each earlier (rank r, score p) reading implies at least r accounts still have at least p points at the later cutoff. Sum the envelope L(k)=max p over all observations with r>=k and time<=cutoff. Rank identities may change. The calculation uses pre-drop observations too; these are persistent lower constraints, not a synchronized cross-section. See platform_bound_segments.csv.

For a structural model, treat the 2026-09-19 02:35–03:03 readings as approximately synchronous. Top 10 sum=109,988.958. Piecewise straight lines in log(rank), log(points) through the top 10, rank 29, rank 115 and rank 1643 sum to 392,547.653 for ranks 1–1643. The monotone-only prefix range under that synchronization assumption is 241,547.373–1,215,074.409; it is not a confidence interval. Local slopes are in platform_local_slopes.csv.

Extrapolate beyond rank 1643 as p(r)=29.653493*(r/1643)^(-alpha). The rank 115–1643 fitted alpha is 1.110446. There were already at least 2,270 ranked accounts, so no finite-population scenario below 2,270 is used.

| Total ranked accounts N | Total points at fitted tail slope |
|---:|---:|
| 2,270 | 408,015 |
| 5,000 | 443,559 |
| 10,000 | 472,307 |
| 25,000 | 507,085 |
| 100,000 | 553,447 |
| Infinite | 833,643–833,673 |

The infinite-tail interval bounds numerical summation of that specific model only. It is not an empirical uncertainty interval. platform_tail_models.csv varies alpha over 0.8, 1.0, 1.110446, 1.25, 1.5, 2.0; alpha<=1 has a divergent infinite tail. An approximately 0.5M total is compatible with some assumptions, but is not inferred uniquely from the snapshots.

From 2026-09-19T03:59:00Z to 2026-09-28T03:42:00Z (8.988194 days), top-two total rose by 12,839.142, or 25.227% (19.148% per 7 days compounded). These are rank slots; account identities are absent. A3 rose by 875.695 in the same exact-endpoint interval and is distinct from both final leaders. Therefore platform growth is at least 13,714.837 over the interval, equivalent to 10,681.106 per 7 days averaged over it. No finite growth upper bound or positive percentage lower bound follows. Do not subtract the two total lower bounds to estimate growth.

The prior 100K–150K points/week extrapolation requires stable leaderboard share or a stable distribution, neither observed here. Program-end totals 1.5M/2.0M/3.6M/5.2M belong in reward-value scenarios, not in the measured-denominator estimate.

Most useful next measurement: synchronized full leaderboard exports (including ranked-account count and tail) on two consecutive Fridays, with screenshots immediately before and after the drop. A reported platform total would directly settle the denominator.
