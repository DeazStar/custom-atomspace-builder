#!/usr/bin/env python3
"""Deterministic load test for POST /api/upload/files.

Produces a reproducible baseline of the upload implementation so persistence
and CSV preprocessing can be compared. It makes no changes to the application.

Two experiments run per concurrency level:

  csv  - uploads a deterministic CSV; exercises persistence + full CSV
         preprocessing.
  bin  - uploads a same-size deterministic non-CSV; exercises persistence only.

Sessions are created through the real API and filenames are unique per session,
so the duplicate-name 400 path is never hit (this measures the happy path).
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
import time
from datetime import datetime, timezone

import httpx

DEFAULT_BASE_URL = "http://localhost:8000"
DEFAULT_ROWS = 20_000
DEFAULT_COLS = 40


def make_csv_payload(rows: int, cols: int) -> bytes:
    """Deterministic CSV: a header row plus `rows` data rows of `cols` cells.

    Cell content is a fixed formula (r<row>:06d c<col>:02d), so the same
    bytes are produced on every invocation.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow([f"col_{c}" for c in range(cols)])
    for r in range(rows):
        writer.writerow([f"r{r:06d}c{c:02d}" for c in range(cols)])
    return buf.getvalue().encode("utf-8")


def make_bin_payload(nbytes: int) -> bytes:
    """Deterministic non-CSV bytes of exactly `nbytes` length (same size as the CSV)."""
    pattern = (
        b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        b"!@#$%^&*()_+-=[]{};:<>?\n"
    )
    repeats = (nbytes // len(pattern)) + 1
    return (pattern * repeats)[:nbytes]


def percentile_nearest_rank(sorted_values, pct: float):
    """Nearest-rank percentile (0 < pct <= 100)."""
    n = len(sorted_values)
    if n == 0:
        return None
    idx = math.ceil(pct / 100.0 * n) - 1
    return sorted_values[max(0, min(idx, n - 1))]


def fmt_duration(value):
    if value is None:
        return "n/a"
    return f"{value:.3f}s"


async def create_session(client: httpx.AsyncClient, base_url: str) -> str:
    r = await client.post(f"{base_url}/api/upload/create-session")
    r.raise_for_status()
    return r.json()["session_id"]


async def upload_one(
    client: httpx.AsyncClient,
    base_url: str,
    session_id: str,
    filename: str,
    payload: bytes,
    content_type: str,
) -> dict:
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
            except Exception:
                body = {}
        return {
            "session_id": session_id,
            "filename": filename,
            "latency_s": latency,
            "status_code": r.status_code,
            "ok": r.status_code == 200,
            "error": None,
            "detail": r.text[:200] if r.status_code != 200 else "",
            # total_files is the session file count as seen by this request
            # after it committed.
            "response_total_files": body.get("total_files"),
            "response_files_in_session": body.get("files_in_session"),
        }
    except Exception as exc:  # network error, timeout, connection refused, ...
        latency = time.perf_counter() - t0
        return {
            "session_id": session_id,
            "filename": filename,
            "latency_s": latency,
            "status_code": None,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "detail": "",
            "response_total_files": None,
            "response_files_in_session": None,
        }


async def get_diagnostics(
    client: httpx.AsyncClient, base_url: str, session_id: str
) -> dict:
    """Post-round cache/session observations.

    - cached_files_count: entry count in the session's datasources cache, via
      GET /cache/stats (load_cache only, no re-processing).
    - datasources_count: count GET /datasources returns.

    These expose whether concurrent uploads lost or healed cache entries. They
    are recorded after the measured window, so they do not affect the
    latency/throughput numbers.
    """
    result = {"cached_files_count": None, "datasources_count": None, "error": None}
    try:
        r = await client.get(f"{base_url}/api/upload/{session_id}/cache/stats")
        if r.status_code == 200:
            result["cached_files_count"] = r.json().get("cache_stats", {}).get("cached_files_count")
        else:
            result["error"] = f"cache/stats {r.status_code}: {r.text[:200]}"
    except Exception as exc:
        result["error"] = f"cache/stats: {type(exc).__name__}: {exc}"
    try:
        r = await client.get(f"{base_url}/api/upload/{session_id}/datasources")
        if r.status_code == 200:
            result["datasources_count"] = len(r.json())
        else:
            result["error"] = (result["error"] or "") + f"; datasources {r.status_code}: {r.text[:200]}"
    except Exception as exc:
        result["error"] = (result["error"] or "") + f"; datasources: {type(exc).__name__}: {exc}"
    return result


async def delete_file(client: httpx.AsyncClient, base_url: str, session_id: str, filename: str) -> int:
    """Best-effort delete with one retry (a failed keep-alive request can leave
    the pooled connection broken). Returns the HTTP status, or None."""
    for attempt in range(2):
        try:
            r = await client.delete(f"{base_url}/api/upload/{session_id}/files/{filename}")
            return r.status_code
        except Exception:
            if attempt == 1:
                return None
            await asyncio.sleep(0.2)
    return None


async def run_round(
    args,
    payload: bytes,
    content_type: str,
    extension: str,
    prefix: str,
    concurrency: int,
    round_index: int,
    experiment_label: str,
):
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    timeout = httpx.Timeout(args.timeout)

    if args.sessions == "shared":
        async with httpx.AsyncClient(base_url=args.base_url, timeout=timeout, limits=limits) as client:
            session_id = await create_session(client, args.base_url)
            filenames = [f"{prefix}_c{concurrency}_r{round_index}_{i:03d}.{extension}" for i in range(concurrency)]

            wall_t0 = time.perf_counter()
            results = await asyncio.gather(
                *(upload_one(client, args.base_url, session_id, fn, payload, content_type)
                  for fn in filenames)
            )
            wall_s = time.perf_counter() - wall_t0

            # Diagnostics run before cleanup: they read session/cache state
            # that deletion would destroy.
            diag = await get_diagnostics(client, args.base_url, session_id)

            cleanup_failures = []
            if not args.no_cleanup:
                for res in results:
                    status = await delete_file(client, args.base_url, session_id, res["filename"])
                    if status != 200:
                        cleanup_failures.append({"filename": res["filename"], "delete_status": status})

        # files_in_session is the largest total_files any request observed after
        # commit.
        totals = [r["response_total_files"] for r in results if r.get("response_total_files") is not None]
        files_in_session = max(totals) if totals else None
        diag = {**diag, "files_in_session": files_in_session, "cleanup_failures": cleanup_failures}
    else:  # per-request: one fresh session per upload
        async with httpx.AsyncClient(base_url=args.base_url, timeout=timeout, limits=limits) as client:
            async def one(i: int):
                filename = f"{prefix}_c{concurrency}_r{round_index}_{i:03d}.{extension}"
                sid = await create_session(client, args.base_url)
                res = await upload_one(client, args.base_url, sid, filename, payload, content_type)
                if not args.no_cleanup:
                    await delete_file(client, args.base_url, sid, filename)
                return res

            wall_t0 = time.perf_counter()
            results = await asyncio.gather(*(one(i) for i in range(concurrency)))
            wall_s = time.perf_counter() - wall_t0
            diag = {
                "files_in_session": 1,
                "datasources_count": None,
                "error": "n/a (per-request sessions)",
                "cleanup_failures": [],
            }

    return {
        "experiment": experiment_label,
        "concurrency": concurrency,
        "round": round_index,
        "wall_s": wall_s,
        "results": results,
        "diagnostic": diag,
        "file_size_bytes": len(payload),
    }


def summarize_round(round_data: dict, row_count: int, col_count: int) -> dict:
    results = round_data["results"]
    ok = [r for r in results if r["ok"]]
    failed = [r for r in results if not r["ok"]]
    lats = sorted(r["latency_s"] for r in results)

    return {
        "experiment": round_data["experiment"],
        "concurrency": round_data["concurrency"],
        "round": round_data["round"],
        "samples": len(results),
        "ok": len(ok),
        "failed": len(failed),
        "wall_s": round_data["wall_s"],
        "throughput_req_s": round_data["concurrency"] / round_data["wall_s"] if round_data["wall_s"] > 0 else None,
        "bytes_total": round_data["concurrency"] * round_data["file_size_bytes"],
        "mb_per_s": (
            (round_data["concurrency"] * round_data["file_size_bytes"]) / (1024 * 1024) / round_data["wall_s"]
            if round_data["wall_s"] > 0 else None
        ),
        "p50_s": statistics.median(lats) if lats else None,
        "p95_s": percentile_nearest_rank(lats, 95),
        "max_s": lats[-1] if lats else None,
        "min_s": lats[0] if lats else None,
        "file_size_bytes": round_data["file_size_bytes"],
        "row_count": row_count,
        "col_count": col_count,
        "files_in_session": round_data["diagnostic"].get("files_in_session"),
        "cached_files_count": round_data["diagnostic"].get("cached_files_count"),
        "datasources_count": round_data["diagnostic"].get("datasources_count"),
        "cleanup_failures": len(round_data["diagnostic"].get("cleanup_failures") or []),
        "diag_error": round_data["diagnostic"].get("error"),
        "failures": [{"filename": r["filename"], "status": r["status_code"], "error": r["error"], "detail": r["detail"]} for r in failed],
    }


def print_summary_table(rows):
    print("\n=== SUMMARY ===")
    columns = [
        ("experiment", "experiment"),
        ("conc", "concurrency"),
        ("round", "round"),
        ("n", "samples"),
        ("ok", "ok"),
        ("fail", "failed"),
        ("wall_s", "wall_s"),
        ("req/s", "throughput_req_s"),
        ("MB/s", "mb_per_s"),
        ("p50_s", "p50_s"),
        ("p95_s", "p95_s"),
        ("max_s", "max_s"),
        ("min_s", "min_s"),
        ("size_bytes", "file_size_bytes"),
        ("rows", "row_count"),
        ("cols", "col_count"),
        ("files_in_session", "files_in_session"),
        ("cached_ds", "cached_files_count"),
        ("datasources", "datasources_count"),
        ("cleanup_fail", "cleanup_failures"),
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
        description="Deterministic load test for POST /api/upload/files (baseline only, no app changes)."
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL (default: %(default)s)")
    parser.add_argument(
        "--concurrency", nargs="+", type=int, default=[1, 2, 5, 10, 20],
        help="concurrency levels to test (default: 1 2 5 10 20)",
    )
    parser.add_argument(
        "--file-type", choices=["csv", "bin", "both"], default="both",
        help="csv = persistence + preprocessing; bin = persistence only; both = both experiments (default: both)",
    )
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS, help="CSV data rows (drives CSV parse cost)")
    parser.add_argument("--cols", type=int, default=DEFAULT_COLS, help="CSV columns")
    parser.add_argument("--repeat", type=int, default=1, help="rounds per (experiment, concurrency) pair (default: 1)")
    parser.add_argument("--timeout", type=float, default=600.0, help="per-request HTTP timeout in seconds (default: 600)")
    parser.add_argument(
        "--sessions", choices=["shared", "per-request"], default="shared",
        help="shared = all concurrent uploads into one session; per-request = a fresh session per upload (default: shared)",
    )
    parser.add_argument("--output-dir", default=os.path.join(os.path.dirname(__file__), "results"))
    parser.add_argument("--label", default="", help="optional run label appended to the results directory name")
    parser.add_argument("--no-cleanup", action="store_true", help="do not delete uploaded files after each round")
    args = parser.parse_args()

    if any(c < 1 for c in args.concurrency):
        parser.error("concurrency values must be >= 1")
    if args.rows < 1 or args.cols < 1:
        parser.error("rows and cols must be >= 1")

    csv_payload = make_csv_payload(args.rows, args.cols)
    bin_payload = make_bin_payload(len(csv_payload))

    experiments = []
    if args.file_type in ("csv", "both"):
        experiments.append(("csv", csv_payload, "text/csv", "csv", args.rows, args.cols))
    if args.file_type in ("bin", "both"):
        experiments.append(("bin", bin_payload, "application/octet-stream", "bin", 0, 0))

    for label, payload, ctype, ext, rows, cols in experiments:
        print(
            f"[payload] {label}: {len(payload)} bytes ({len(payload)/1024/1024:.2f} MiB)"
            + (f", rows={rows}, cols={cols}" if rows else "")
        )

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = f"_{args.label}" if args.label else ""
    out_dir = os.path.join(args.output_dir, f"{stamp}{suffix}")
    os.makedirs(out_dir, exist_ok=True)

    summary_rows = []
    latency_rows = []

    for label, payload, ctype, ext, rows, cols in experiments:
        for conc in args.concurrency:
            for rnd in range(args.repeat):
                print(
                    f"[run] experiment={label} concurrency={conc} round={rnd + 1}/{args.repeat} "
                    f"-> uploading {conc} x {len(payload)/1024/1024:.2f} MiB ...",
                    flush=True,
                )
                round_data = asyncio.run(run_round(args, payload, ctype, ext, label, conc, rnd, label))
                summary = summarize_round(round_data, rows, cols)
                summary_rows.append(summary)

                for res in round_data["results"]:
                    latency_rows.append({
                        "experiment": label,
                        "concurrency": conc,
                        "round": rnd,
                        "filename": res["filename"],
                        "latency_s": res["latency_s"],
                        "status_code": res["status_code"],
                        "ok": res["ok"],
                        "error": res["error"] or res["detail"],
                    })

                if summary["failures"]:
                    for f_ in summary["failures"]:
                        print(f"    ! failed: {f_['filename']} status={f_['status']} error={f_['error']}")
                print(
                    f"    wall={summary['wall_s']:.3f}s ok={summary['ok']}/{summary['samples']} "
                    f"p50={fmt_duration(summary['p50_s'])} p95={fmt_duration(summary['p95_s'])} max={fmt_duration(summary['max_s'])}",
                    flush=True,
                )

    print_summary_table(summary_rows)

    summary_csv_path = os.path.join(out_dir, "summary.csv")
    lat_csv_path = os.path.join(out_dir, "latencies.csv")
    meta_path = os.path.join(out_dir, "meta.json")

    if summary_rows:
        with open(summary_csv_path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
            w.writeheader()
            for s in summary_rows:
                row = {k: (v if not isinstance(v, float) else round(v, 6)) for k, v in s.items()}
                row["failures"] = json.dumps(s["failures"])
                w.writerow(row)
        with open(lat_csv_path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(latency_rows[0].keys()))
            w.writeheader()
            for r in latency_rows:
                w.writerow(r)

    import httpx as _httpx
    meta = {
        "timestamp": stamp,
        "base_url": args.base_url,
        "concurrency": args.concurrency,
        "file_type": args.file_type,
        "rows": args.rows,
        "cols": args.cols,
        "repeat": args.repeat,
        "timeout_s": args.timeout,
        "sessions": args.sessions,
        "cleanup": not args.no_cleanup,
        "csv_payload_bytes": len(csv_payload),
        "bin_payload_bytes": len(bin_payload),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "httpx_version": getattr(_httpx, "__version__", "unknown"),
    }
    with open(meta_path, "w") as fh:
        json.dump(meta, fh, indent=2)

    print(f"\nResults written to: {out_dir}")
    print(f"  summary.csv, latencies.csv, meta.json")


if __name__ == "__main__":
    main()
