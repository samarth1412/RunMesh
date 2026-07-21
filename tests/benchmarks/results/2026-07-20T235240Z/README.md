# Superseded local benchmark — 2026-07-20T235240Z

Source commit: `7eebd569e63768d853f37484ef121ed77de82234`

The blocking correctness verifier passed, including 10,000/10,000 successful
tasks, five recovered leases, zero permanent task loss, and records on all six
Kafka partitions. This run is retained as raw historical evidence but is not
used for README performance claims: its k6 summaries did not include p99 and the
worker topology was not normalized enough to produce representative partition
balance. The raw JSON and copied `commands.sh` have not been edited.
