# Upload benchmark

Reproducible load tests for `POST /api/upload/files`. They hit the real API
and make no changes to the application. Generated results live under
`benchmark/results/` (gitignored).

## What it tests

Two experiments run at each concurrency level, both uploading a deterministic
~8.8 MiB payload:

- `csv` — exercises file persistence **and** CSV preprocessing.
- `bin` — a same-size non-CSV file, exercising persistence only.

Comparing the two isolates how much of the cost is CSV parsing versus the
shared upload/persistence path. Filenames are unique per session, so the
duplicate-name 400 path is never hit (the happy path is what is measured).

## Why there are two scripts

The upload flow changed:

```
Original:        upload → save → preprocess → response

New:             upload → save → enqueue → response
                                     ↓
                               Celery workers
                                     ↓
                                preprocess
```

- `load_test.py` benchmarks the **original** synchronous flow, where CSV
  preprocessing happens inside the HTTP request.
- `benchmark_async.py` benchmarks the **new** Celery flow, where the request
  only persists and enqueues, and workers preprocess asynchronously.

Both use the same payload size, rows, columns, concurrency sweep
(`1 2 5 10 20`), repeat count, and shared-session structure, so their numbers
are directly comparable.

## Critical measurement distinction

The request-latency numbers in the two benchmarks measure different amounts of
work:

- **Baseline request latency** includes upload persistence **and** CSV
  preprocessing, because preprocessing happens before the HTTP response.
- **New request latency** measures persistence + enqueueing only, because
  preprocessing happens after the response, in the workers.

So a faster new request-latency number does not mean CSV parsing itself got
faster — the improvement comes from moving preprocessing out of the request
path and letting several worker processes handle files concurrently. To verify
the background work actually completes, `benchmark_async.py` separately
measures **end-to-end** time: from round start until every file reaches a
terminal state (`processed`/`failed`), by polling
`GET /api/upload/{session_id}/status`.

## Metrics

- **Request path** (`load_test.py`; `benchmark_async.py`): per-round wall time,
  per-request latency with p50/p95/max, throughput (`req/s`), and failed-request
  count.
- **End-to-end** (`benchmark_async.py` only): round wall time until every file
  is terminal, files `processed`/`failed`/`incomplete`, processing rate
  (`files/s`), and per-file processing time.
- **Observations**: Celery queue depth (via `--redis-url`), per-PID CPU%/RSS
  (`--pid ...`, Linux `/proc`), and post-round cache/session counts.

Results are written to `benchmark/results/<timestamp>[_<label>]/` as
`summary.csv`, `latencies.csv`, and `meta.json` (`benchmark_async.py` also
writes `terminal.csv`).

## How to run

Install the client dependencies once:

```bash
pip install -r benchmark/requirements.txt
```

### Baseline

Run the API the way it ran before the Celery change (single worker), then:

```bash
python3 benchmark/load_test.py --base-url http://localhost:8000
```

Useful variations:

```bash
# quick smoke test (rows drive parse cost)
python3 benchmark/load_test.py --rows 2000 --cols 10

# more samples per concurrency level for meaningful percentiles
python3 benchmark/load_test.py --repeat 5

# persistence-only experiment
python3 benchmark/load_test.py --file-type bin
```

### Celery benchmark

Start the API **and** the Celery workers, then sample their PIDs:

```bash
celery -A app.core.celery_app.celery_app worker --concurrency=4

python3 benchmark/benchmark_async.py --base-url http://localhost:8000 \
    --pid <api_pid> --pid <worker_pid>... \
    --redis-url redis://localhost:6379/0 --label celery4
```

`--pid` is repeatable (API process plus each worker, including prefork
children) and enables the CPU/RSS columns.

## Verified results

Measured 2026-08-15 against the Celery pipeline (4 worker processes) and the
same-day baseline, CSV experiment, 20,000×40 (~8.8 MiB) payload. Request-path
round wall times are medians across 3 runs × 3 rounds:

| Concurrency | Baseline (s) | New (s) | Speedup |
|------------:|-------------:|--------:|--------:|
| 1  | 0.447 | 0.115 | 3.9× |
| 2  | 0.771 | 0.205 | 3.8× |
| 5  | 1.671 | 0.447 | 3.7× |
| 10 | 2.698 | 0.690 | 3.9× |
| 20 | 4.683 | 1.479 | 3.2× |

End-to-end time (new flow, includes worker preprocessing): 0.755 s at
concurrency 1, rising to 2.972 s at concurrency 20 — faster than the baseline
request path at concurrency ≥ 5, and with **0 failed and 0 incomplete** files
across all rounds. Per-file processing median is ~0.43 s at low concurrency,
~0.73 s at concurrency 20; the four workers sustain ~6.7 files/s at
concurrency 20 (vs 1.3 files/s with a single request-path pipeline at
concurrency 1).

## Caveats

- **Same-machine measurement.** These runs put the client, the API, and the
  workers on one machine, so client-side CPU/memory and disk I/O contend with
  the server. The numbers above were captured under light additional load;
  treat them as indicative of the relative improvement, not absolute capacity.
- **Polling granularity.** `benchmark_async.py` polls status every 0.2 s by
  default, so end-to-end times at low concurrency include up to ~0.2 s of
  quantization (this is why e2e at concurrency 1 exceeds the old request time).
- **p95 with few samples.** With `--repeat 1` and small concurrency the p95 is
  effectively the max; use `--repeat` for meaningful percentiles.
- **Cleanup is best-effort.** Failed deletes leave files on disk and are
  recorded in `cleanup_fail`; they do not affect the measured numbers.