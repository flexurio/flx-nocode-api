# Performance baselines

Each sub-directory `YYYYMMDD-HHMM/` holds the k6 `--summary-export` JSON of one
`make perf-baseline` run plus (optionally) a `metrics.txt` snapshot of `/metrics`.
Record the machine (CPU, DB, whether Redis was on) in a `NOTES.md` next to them.
The first directory is the pre-Phase-1 baseline; later runs are compared against it.
