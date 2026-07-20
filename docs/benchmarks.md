# Benchmarks

Targets are hypotheses until a dated result is recorded:

- 100–500 local dispatches/second
- API p95 below 150 ms at 100 requests/second
- Recovery within two lease intervals
- No permanently lost tasks in a 10,000-task worker-kill exercise

Run `k6 run tests/load/submissions.js`. Store raw JSON results under an ignored `benchmarks/results/` directory and record hardware, image SHAs, dataset, and date before publishing claims.
