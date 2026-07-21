# Superseded local benchmark — 2026-07-21T000619Z

Source commit: `9d14c59918978549001d9cd6ef14d302b0d196e4`

This corrected run passed all 15 blocking verification checks. It recorded
3,001 authenticated reads at 100.021 requests/second with 3.386 ms p95 and
4.966 ms p99, scheduler dispatch of 10,000 tasks at 414.042 tasks/second, and
10,000/10,000 successful tasks after terminating 20 workers with seven tasks
actively running. Seven leases were recovered and no task was permanently
lost.

It is retained as valid historical evidence but was superseded because a later
container-security fix changed the source commit. The latest documented run is
therefore tied to the patched code candidate. Raw JSON and `commands.sh` in
this directory have not been edited.
