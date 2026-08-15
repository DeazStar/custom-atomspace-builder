#!/usr/bin/env python3
"""Load test for the Celery-backed upload pipeline.

Measures two dimensions with the same payload, concurrency sweep, and
shared-session structure as the baseline benchmark (load_test.py):

  A. REQUEST PATH  - how quickly the API accepts, persists, and enqueues uploads
                     (per-request latency + round wall time).
  B. END-TO-END    - time from round start until every file reaches a terminal
                     state (processed/failed), i.e. including the Celery
                     worker's CSV preprocessing.

Usage:
    python3 benchmark/benchmark_async.py --base-url http://localhost:8000 \
        --pid <api_pid> --pid <worker_pid>... [--redis-url redis://localhost:6379/0]
"""

import argparse
import asyncio
import csv
import io
import json
import math
import os
import platform
import statistics
import threading
import time
from datetime import datetime, timezone

import httpx

from load_test import make_csv_payload, make_bin_payload, percentile_nearest_rank

DEFAULT_BASE_URL = "http://localhost:8000"
DEFAULT_ROWS = 20_000
DEFAULT_COLS = 40
DEFAULT_POLL_INTERVAL = 0.2


class CpuSampler(threading.Thread):
    """Samples per-PID CPU% and RSS on an interval (Linux /proc only, best-effort)
    and records aggregate stats."""

    def __init__(self, pids, interval=0.5):
        super().__init__(daemon=True)
        self.pids = [int(p) for p in pids]
        self.interval = interval
        self._stop = threading.Event()
        self._per_pid = {pid: [] for pid in self.pids}
        self._t0 = time.monotonic()

    @staticmethod
    def _read_proc(pid):
        try:
            with open(f"/proc/{pid}/stat") as fh:
                parts = fh.read().split()
            utime = int(parts[13])
            stime = int(parts[14])
            rss_bytes = int(parts[23]) * os.sysconf("SC_PAGE_SIZE")
            return utime, stime, rss_bytes
        except Exception:  # noqa: BLE001 - best effort
            return None

    @staticmethod
    def _sys_ticks():
        try:
            with open("/proc/stat") as fh:
                for line in fh:
                    if line.startswith("cpu "):
                        return sum(int(x) for x in line.split()[1:])
        except Exception:  # noqa: BLE001
            return None
        return None

    def run(self):
        nproc = os.cpu_count() or 1
        prev = {pid: self._read_proc(pid) for pid in self.pids}
        prev_t = time.monotonic()
        prev_sys = self._sys_ticks()
        while not self._stop.is_set():
            time.sleep(self.interval)
            now_t = time.monotonic()
            sys = self._sys_ticks()
            for pid in self.pids:
                cur = self._read_proc(pid)
                old = prev.get(pid)
                if cur and old and sys and prev_sys is not None:
                    dt_proc = (cur[0] - old[0]) + (cur[1] - old[1])
                    dt_sys = (sys - prev_sys) or 1
                    # Percent of one core: a process pinned to one core reads
                    # dt_sys/nproc of the system ticks (the /proc/stat "cpu "
                    # line sums all cores).
                    pct = 100.0 * dt_proc * nproc / dt_sys
                    self._per_pid[pid].append((pct, cur[2]))
            prev = {pid: self._read_proc(pid) for pid in self.pids}
            prev_t = now_t
            prev_sys = sys

    def stop(self):
        self._stop.set()

    def summary(self):
        out = {}
        for pid in self.pids:
            samples = self._per_pid[pid]
            if not samples:
                out[pid] = None
                continue
            cpu = [s[0] for s in samples]
            rss = [s[1] for s in samples]
            out[pid] = {
                "cpu_max_pct": round(max(cpu), 1),
                "cpu_avg_pct": round(statistics.mean(cpu), 1),
                "rss_max_mb": round(max(rss) / 1048576, 1),
            }
        return out


def fmt_duration(value):
    return "n/a" if value is None else f"{value:.3f}s"


async def create_session(client, base_url):
    r = await client.post(f"{base_url}/api/upload/create-session")
    r.raise_for_status()
    return r.json()["session_id"]


async def upload_one(client, base_url, session_id, filename, payload, content_type):
    t0 = time.perf_counter()
    try:
        r = await client.post(
            f"{base_url}/api/upload/files",
            data={"session_id": session_id},
            files=[("files", (filename, payload, content_type))],
        )
        latency = time.perf_counter() - t0
        body = {}
        if r.status_code == 200:
            try:
                body = r.json()
            except Exception:  # noqa: BLE001
                body = {}
        return {
            "session_id": session_id,
            "filename": filename,
            "latency_s": latency,
            "status_code": r.status_code,
            "ok": r.status_code == 200,
            "error": None,
            "detail": r.text[:200] if r.status_code != 200 else "",
            "task_id": body.get("task_id"),
            "file_status": (body.get("file_statuses") or {}).get(filename),
            "response_total_files": body.get("total_files"),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "session_id": session_id,
            "filename": filename,
            "latency_s": time.perf_counter() - t0,
            "status_code": None,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "detail": "",
            "task_id": None,
            "file_status": None,
            "response_total_files": None,
        }


async def poll_until_terminal(client, base_url, session_id, filenames, start_t0,
                              timeout_s, poll_interval, redis_client, queue_key):
    """Poll GET /status until every file is terminal (processed/failed).

    Returns (first_terminal, incomplete, queue_depth_samples).  first_terminal
    maps filename -> {"status", "t_s"} where t_s is seconds since start_t0.
    """
    first_terminal = {}
    incomplete = set(filenames)
    queue_depths = []

    def sample_queue():
        if redis_client is None:
            return
        try:
            queue_depths.append(redis_client.llen(queue_key))
        except Exception:  # noqa: BLE001
            pass

    while incomplete and time.perf_counter() - start_t0 < timeout_s:
        sample_queue()
        try:
            r = await client.get(f"{base_url}/api/upload/{session_id}/status")
            if r.status_code == 200:
                statuses = r.json().get("file_statuses", {})
                now = time.perf_counter() - start_t0
                for fn in list(incomplete):
                    st = statuses.get(fn)
                    if st in ("processed", "failed"):
                        first_terminal[fn] = {"status": st, "t_s": now}
                        incomplete.discard(fn)
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(poll_interval)

    return first_terminal, incomplete, queue_depths


async def delete_file(client, base_url, session_id, filename):
    try:
        return (await client.delete(f"{base_url}/api/upload/{session_id}/files/{filename}")).status_code
    except Exception:  # noqa: BLE001
        return None


async def run_round(args, payload, content_type, extension, experiment_label,
                    concurrency, round_index, redis_client):
    queue_key = "celery"  # Celery default queue key in the Redis broker
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    timeout = httpx.Timeout(args.timeout)

    async with httpx.AsyncClient(base_url=args.base_url, timeout=timeout, limits=limits) as client:
        session_id = await create_session(client, args.base_url)
        filenames = [
            f"{experiment_label}_c{concurrency}_r{round_index}_{i:03d}.{extension}"
            for i in range(concurrency)
        ]

        upload_t0 = time.perf_counter()
        results = await asyncio.gather(
            *(upload_one(client, args.base_url, session_id, fn, payload, content_type)
              for fn in filenames)
        )
        upload_wall_s = time.perf_counter() - upload_t0

        # Dimension B: from round start until every file is terminal.
        e2e_t0 = upload_t0
        first_terminal, incomplete, queue_depths = await poll_until_terminal(
            client, args.base_url, session_id, filenames, e2e_t0,
            args.e2e_timeout, args.poll_interval, redis_client, queue_key)
        e2e_wall_s = time.perf_counter() - e2e_t0

        cleanup_failures = []
        if not args.no_cleanup:
            for res in results:
                status = await delete_file(client, args.base_url, session_id, res["filename"])
                if status != 200:
                    cleanup_failures.append({"filename": res["filename"], "delete_status": status})

    totals = [r["response_total_files"] for r in results if r.get("response_total_files") is not None]
    files_in_session = max(totals) if totals else None

    return {
        "experiment": experiment_label,
        "concurrency": concurrency,
        "round": round_index,
        "upload_wall_s": upload_wall_s,
        "e2e_wall_s": e2e_wall_s,
        "e2e_first_terminal": first_terminal,
        "e2e_incomplete": sorted(incomplete),
        "queue_depths": queue_depths,
        "results": results,
        "files_in_session": files_in_session,
        "cleanup_failures": cleanup_failures,
        "file_size_bytes": len(payload),
    }


def summarize_round(round_data):
    results = round_data["results"]
    ok = [r for r in results if r["ok"]]
    failed = [r for r in results if not r["ok"]]
    lats = sorted(r["latency_s"] for r in results)

    ft = round_data["e2e_first_terminal"]
    per_file_processing = []
    for r in results:
        if r["filename"] in ft:
            per_file_processing.append(ft[r["filename"]]["t_s"] - r["latency_s"])
    processed = [fn for fn, v in ft.items() if v["status"] == "processed"]
    failed_terminal = [fn for fn, v in ft.items() if v["status"] == "failed"]

    qd = round_data["queue_depths"]
    queue_max = max(qd) if qd else None
    queue_avg = round(statistics.mean(qd), 1) if qd else None

    return {
        "experiment": round_data["experiment"],
        "concurrency": round_data["concurrency"],
        "round": round_data["round"],
        "samples": len(results),
        "upload_ok": len(ok),
        "upload_failed": len(failed),
        "upload_wall_s": round_data["upload_wall_s"],
        "upload_req_s": round(round_data["concurrency"] / round_data["upload_wall_s"], 3)
        if round_data["upload_wall_s"] > 0 else None,
        "upload_p50_s": round(statistics.median(lats), 6) if lats else None,
        "upload_p95_s": round(percentile_nearest_rank(lats, 95), 6) if lats else None,
        "upload_max_s": round(lats[-1], 6) if lats else None,
        "upload_min_s": round(lats[0], 6) if lats else None,
        "e2e_wall_s": round(round_data["e2e_wall_s"], 6),
        "e2e_processed": len(processed),
        "e2e_failed": len(failed_terminal),
        "e2e_incomplete": len(round_data["e2e_incomplete"]),
        "processing_files_s": round(round_data["concurrency"] / round_data["e2e_wall_s"], 3)
        if round_data["e2e_wall_s"] > 0 else None,
        "e2e_p50_file_s": round(statistics.median(sorted(per_file_processing)), 6)
        if per_file_processing else None,
        "e2e_max_file_s": round(max(per_file_processing), 6) if per_file_processing else None,
        "queue_depth_max": queue_max,
        "queue_depth_avg": queue_avg,
        "files_in_session": round_data["files_in_session"],
        "cleanup_failures": len(round_data["cleanup_failures"]),
        "failures": [{"filename": r["filename"], "status": r["status_code"],
                      "error": r["error"], "detail": r["detail"]} for r in failed],
    }


def print_summary_table(rows):
    print("\n=== SUMMARY ===")
    columns = [
        ("experiment", "experiment"), ("conc", "concurrency"), ("r", "round"),
        ("n", "samples"), ("up_ok", "upload_ok"), ("up_fail", "upload_failed"),
        ("up_wall_s", "upload_wall_s"), ("up_req/s", "upload_req_s"),
        ("up_p50_s", "upload_p50_s"), ("up_p95_s", "upload_p95_s"),
        ("up_max_s", "upload_max_s"), ("e2e_wall_s", "e2e_wall_s"),
        ("e2e_ok", "e2e_processed"), ("e2e_fail", "e2e_failed"),
        ("inc", "e2e_incomplete"), ("proc/s", "processing_files_s"),
        ("q_max", "queue_depth_max"), ("q_avg", "queue_depth_avg"),
    ]
    widths = [len(h) for h, _ in columns]
    for i, (h, key) in enumerate(columns):
        for r in rows:
            v = r.get(key)
            s = f"{v:.3f}" if isinstance(v, float) else ("n/a" if v is None else str(v))
            widths[i] = max(widths[i], len(s))
    print("  ".join(h.ljust(w) for (h, _), w in zip(columns, widths)))
    for r in rows:
        cells = []
        for (h, key), w in zip(columns, widths):
            v = r.get(key)
            s = f"{v:.3f}" if isinstance(v, float) else ("n/a" if v is None else str(v))
            cells.append(s.ljust(w))
        print("  ".join(cells))


def main():
    parser = argparse.ArgumentParser(
        description="Load test for the Celery-backed upload pipeline (upload latency + e2e processing)."
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--concurrency", nargs="+", type=int, default=[1, 2, 5, 10, 20])
    parser.add_argument("--file-type", choices=["csv", "bin", "both"], default="both")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--cols", type=int, default=DEFAULT_COLS)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--e2e-timeout", type=float, default=600.0,
                        help="max seconds to wait for all files to reach terminal state")
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL)
    parser.add_argument("--redis-url", default=None,
                        help="sample the Celery Redis queue depth (e.g. redis://localhost:6379/0)")
    parser.add_argument("--pid", action="append", type=int, default=[],
                        help="PID to sample for CPU/RSS (repeatable: API + workers)")
    parser.add_argument("--cpu-interval", type=float, default=0.5)
    parser.add_argument("--output-dir", default=os.path.join(os.path.dirname(__file__), "results"))
    parser.add_argument("--label", default="")
    parser.add_argument("--no-cleanup", action="store_true")
    args = parser.parse_args()

    if any(c < 1 for c in args.concurrency):
        parser.error("concurrency values must be >= 1")

    csv_payload = make_csv_payload(args.rows, args.cols)
    bin_payload = make_bin_payload(len(csv_payload))

    experiments = []
    if args.file_type in ("csv", "both"):
        experiments.append(("csv", csv_payload, "text/csv", "csv", args.rows, args.cols))
    if args.file_type in ("bin", "both"):
        experiments.append(("bin", bin_payload, "application/octet-stream", "bin", 0, 0))

    redis_client = None
    if args.redis_url:
        import redis
        redis_client = redis.Redis.from_url(args.redis_url, decode_responses=True)

    sampler = CpuSampler(args.pid, interval=args.cpu_interval)
    sampler.start()
    try:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = f"_{args.label}" if args.label else ""
        out_dir = os.path.join(args.output_dir, f"{stamp}_async{suffix}")
        os.makedirs(out_dir, exist_ok=True)

        summary_rows = []
        latency_rows = []
        terminal_rows = []

        for label, payload, ctype, ext, rows, cols in experiments:
            print(f"[payload] {label}: {len(payload)} bytes ({len(payload)/1024/1024:.2f} MiB)"
                  + (f", rows={rows}, cols={cols}" if rows else ""))
            for conc in args.concurrency:
                for rnd in range(args.repeat):
                    print(f"[run] experiment={label} concurrency={conc} round={rnd+1}/{args.repeat} "
                          f"-> uploading {conc} x {len(payload)/1024/1024:.2f} MiB ...", flush=True)
                    round_data = asyncio.run(run_round(
                        args, payload, ctype, ext, label, conc, rnd, redis_client))
                    summary = summarize_round(round_data)
                    summary_rows.append(summary)

                    for res in round_data["results"]:
                        ft = round_data["e2e_first_terminal"].get(res["filename"])
                        latency_rows.append({
                            "experiment": label,
                            "concurrency": conc,
                            "round": rnd,
                            "filename": res["filename"],
                            "upload_latency_s": round(res["latency_s"], 6),
                            "status_code": res["status_code"],
                            "ok": res["ok"],
                            "error": res["error"] or res["detail"],
                            "task_id": res["task_id"],
                            "upload_file_status": res["file_status"],
                        })
                        terminal_rows.append({
                            "experiment": label,
                            "concurrency": conc,
                            "round": rnd,
                            "filename": res["filename"],
                            "terminal_status": ft["status"] if ft else "incomplete",
                            "terminal_t_s": round(ft["t_s"], 6) if ft else None,
                            "processing_s": round(ft["t_s"] - res["latency_s"], 6) if ft else None,
                        })

                    if summary["failures"]:
                        for f_ in summary["failures"]:
                            print(f"    ! upload failed: {f_['filename']} status={f_['status']} "
                                  f"error={f_['error']}")
                    if summary["e2e_incomplete"]:
                        print(f"    ! {summary['e2e_incomplete']} file(s) did not reach terminal state")
                    print(f"    upload wall={fmt_duration(summary['upload_wall_s'])} "
                          f"p50={fmt_duration(summary['upload_p50_s'])} "
                          f"p95={fmt_duration(summary['upload_p95_s'])} "
                          f"max={fmt_duration(summary['upload_max_s'])} | "
                          f"e2e wall={fmt_duration(summary['e2e_wall_s'])} "
                          f"ok={summary['e2e_processed']} fail={summary['e2e_failed']}", flush=True)

        print_summary_table(summary_rows)

        # Include CPU/RSS aggregate columns in the written summary
        cpu_summary = sampler.summary()
        for s in summary_rows:
            for pid, agg in cpu_summary.items():
                if agg:
                    s[f"cpu_max_pct_pid{pid}"] = agg["cpu_max_pct"]
                    s[f"cpu_avg_pct_pid{pid}"] = agg["cpu_avg_pct"]
                    s[f"rss_max_mb_pid{pid}"] = agg["rss_max_mb"]

        summary_path = os.path.join(out_dir, "summary.csv")
        lat_path = os.path.join(out_dir, "latencies.csv")
        term_path = os.path.join(out_dir, "terminal.csv")
        meta_path = os.path.join(out_dir, "meta.json")

        if summary_rows:
            with open(summary_path, "w", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
                w.writeheader()
                for s in summary_rows:
                    row = {k: (v if not isinstance(v, float) else round(v, 6)) for k, v in s.items()}
                    row["failures"] = json.dumps(s["failures"])
                    w.writerow(row)
        if latency_rows:
            with open(lat_path, "w", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=list(latency_rows[0].keys()))
                w.writeheader()
                w.writerows(latency_rows)
        if terminal_rows:
            with open(term_path, "w", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=list(terminal_rows[0].keys()))
                w.writeheader()
                w.writerows(terminal_rows)

        meta = {
            "timestamp": stamp,
            "mode": "async_celery",
            "base_url": args.base_url,
            "concurrency": args.concurrency,
            "file_type": args.file_type,
            "rows": args.rows,
            "cols": args.cols,
            "repeat": args.repeat,
            "timeout_s": args.timeout,
            "e2e_timeout_s": args.e2e_timeout,
            "poll_interval_s": args.poll_interval,
            "cleanup": not args.no_cleanup,
            "csv_payload_bytes": len(csv_payload),
            "bin_payload_bytes": len(bin_payload),
            "pids_sampled": args.pid,
            "cpu_summary": cpu_summary,
            "python": platform.python_version(),
            "platform": platform.platform(),
            "httpx_version": getattr(httpx, "__version__", "unknown"),
        }
        with open(meta_path, "w") as fh:
            json.dump(meta, fh, indent=2)

        print(f"\nResults written to: {out_dir}")
        print("  summary.csv, latencies.csv, terminal.csv, meta.json")
    finally:
        sampler.stop()


if __name__ == "__main__":
    main()
