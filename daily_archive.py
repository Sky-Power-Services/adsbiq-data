#!/usr/bin/env python3
"""ADSBiq daily history archiver — adsb.lol-style free GitHub data drop.

Reads ONE day's partition of the append-only aircraft-diffs lake and packages it
as a single zstd-compressed Parquet plus a JSON manifest, ready to publish as a
GitHub Release asset.

Design notes
------------
* Runs on GitHub Actions runners (free, public repos = unlimited minutes), NOT
  on the production feeder box. Zero load on the real-time decoders.
* The source lake is APPEND-ONLY and partitioned by ``date`` (YYYY-MM-DD), so a
  day's data is exactly ``<source-base>/date=<DATE>/*.parquet``. Because nothing
  is ever deleted/updated, we read those parquet files directly.
* DuckDB streams the source -> local parquet, so memory stays flat regardless of
  day size (important for the small GitHub runner).
* Idempotent by construction: this script just produces files; the workflow
  decides whether to create or overwrite the day's release.

Configuration (no locations are hard-coded)
--------------------------------------------
The source location is supplied at runtime, never embedded in this file:
    SOURCE_S3_BASE   the partition root, e.g. via a repo *secret*  (required)
    --source         same thing, as a CLI override (for local runs)
AWS credentials come from the standard chain (env vars / OIDC role) — DuckDB's
credential_chain provider picks them up automatically. Optionally cap CPU with
    DUCKDB_THREADS   integer thread limit (keeps shared runners/boxes friendly)

Usage
-----
    SOURCE_S3_BASE=... python daily_archive.py                    # yesterday (UTC)
    python daily_archive.py --source ... --date 2026-06-27        # specific day
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
import sys


def _yesterday_utc() -> str:
    return (_dt.datetime.now(_dt.timezone.utc).date() - _dt.timedelta(days=1)).isoformat()


def _valid_date(s: str) -> str:
    try:
        _dt.date.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a YYYY-MM-DD date: {s!r}")
    return s


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build(date: str, source_base: str, region: str, out_dir: str) -> dict:
    import duckdb

    if not source_base:
        raise SystemExit("source location not configured — set SOURCE_S3_BASE (or pass --source).")

    os.makedirs(out_dir, exist_ok=True)
    out_parquet = os.path.join(out_dir, f"aircraft_diffs_{date}.parquet")
    out_manifest = os.path.join(out_dir, f"manifest_{date}.json")

    # date= partition dir; hive_partitioning reconstructs the `date` column that
    # the lake stores only in the path (append-only table -> every file is live).
    src_glob = f"{source_base.rstrip('/')}/date={date}/*.parquet"

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    if region:
        con.execute(f"SET s3_region='{region}';")
    threads = os.environ.get("DUCKDB_THREADS")
    if threads:
        con.execute(f"PRAGMA threads={int(threads)};")
    # Pull creds from the standard AWS chain (env vars / OIDC / instance role).
    con.execute("CREATE SECRET aws (TYPE S3, PROVIDER credential_chain);")

    # Fail loudly if the partition is empty/missing rather than publish a 0-row drop.
    (row_count,) = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?, hive_partitioning=1)", [src_glob]
    ).fetchone()
    if row_count == 0:
        raise SystemExit(f"No rows found for {date} — refusing to publish an empty archive.")

    # NOTE: inline src_glob (our own validated config — no injection vector) and
    # keep a single `?` for the TO destination. DuckDB mis-binds COPY when both
    # the subquery and the TO target use positional params.
    con.execute(
        f"""
        COPY (
            SELECT * FROM read_parquet('{src_glob}', hive_partitioning=1) ORDER BY ts
        ) TO ? (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)
        """,
        [out_parquet],
    )

    size = os.path.getsize(out_parquet)
    digest = _sha256(out_parquet)

    manifest = {
        "dataset": "adsbiq-aircraft-diffs",
        "date": date,
        "rows": int(row_count),
        "file": os.path.basename(out_parquet),
        "bytes": size,
        "sha256": digest,
        "compression": "zstd",
        "source": "ADSBiq feeder network",
        "schema_note": "append-only ADS-B aircraft state diffs; is_snapshot=true marks a full row, "
                       "else only changed fields are populated. See repo README for column reference.",
        "license": "ODbL-1.0",
        "generated_by": "adsbiq daily_archive.py",
    }
    with open(out_manifest, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"[ok] {date}: {row_count:,} rows -> {out_parquet} ({size/1e6:.1f} MB, sha256={digest[:12]}...)")

    # Emit machine-readable outputs for the GitHub Actions step.
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as f:
            f.write(f"parquet={out_parquet}\n")
            f.write(f"manifest={out_manifest}\n")
            f.write(f"rows={row_count}\n")
            f.write(f"tag={date}\n")
    return manifest


def main(argv=None):
    ap = argparse.ArgumentParser(description="ADSBiq daily history archiver")
    ap.add_argument("--date", type=_valid_date, default=_yesterday_utc(),
                    help="UTC day to archive (YYYY-MM-DD). Default: yesterday.")
    ap.add_argument("--source", default=os.environ.get("SOURCE_S3_BASE", ""),
                    help="Partition root for the source lake (or set SOURCE_S3_BASE).")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    ap.add_argument("--out-dir", default="dist")
    args = ap.parse_args(argv)
    build(args.date, args.source, args.region, args.out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
