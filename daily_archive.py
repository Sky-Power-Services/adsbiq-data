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


# Adaptive-thinning thresholds (adsb.lol trace style). Env-overridable.
# A point is kept on a real maneuver OR a heartbeat OR a state change, so straight
# cruise collapses to one point per heartbeat while turns/climbs stay full-detail.
_THIN_HEARTBEAT_S = int(os.environ.get("THIN_HEARTBEAT_S", "30"))   # min one point / aircraft / Ns
_THIN_TURN_DEG    = float(os.environ.get("THIN_TURN_DEG", "5"))     # keep if heading moved >= this
_THIN_VERT_FT     = int(os.environ.get("THIN_VERT_FT", "80"))       # keep if altitude moved >= this
# Columns that are integers by nature but may be stored as float — cast to INT
# (smaller + more correct; lat/lon stay as degrees for consumer friendliness).
_INT_FIELDS = {"alt_baro", "alt_geom", "gs", "ias", "tas", "baro_rate", "geom_rate"}


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
    # Memory safety: the adaptive-thinning windows materialize a lot, so cap RAM
    # and give DuckDB a disk temp dir to spill into. Keeps a small runner from
    # OOMing on a big day (prime directive). Defaults left to DuckDB if unset.
    mem = os.environ.get("DUCKDB_MEMORY_LIMIT")
    if mem:
        con.execute(f"PRAGMA memory_limit='{mem}'")
    tmp = os.environ.get("DUCKDB_TEMP_DIR")
    if tmp:
        os.makedirs(tmp, exist_ok=True)
        con.execute(f"PRAGMA temp_directory='{tmp}'")
    # Pull creds from the standard AWS chain (env vars / OIDC / instance role).
    con.execute("CREATE SECRET aws (TYPE S3, PROVIDER credential_chain);")

    src = f"read_parquet('{src_glob}', hive_partitioning=1)"

    # Fail loudly if the partition is empty/missing rather than publish a 0-row drop.
    (row_count,) = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()
    if row_count == 0:
        raise SystemExit(f"No rows found for {date} — refusing to publish an empty archive.")

    # Build the column projection (cast obviously-integer fields to INT).
    src_cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
    proj = ", ".join(f"CAST({c} AS INTEGER) AS {c}" if c in _INT_FIELDS else c for c in src_cols)

    # Adaptive thinning (adsb.lol trace style): keep a point when the aircraft does
    # something real (turn/climb), at a state change, or at a heartbeat — otherwise
    # drop redundant cruise points. Circular track diff handles the 0/360 wrap.
    #
    # Done as TWO PASSES so memory stays bounded on a small runner (prime directive):
    #   Pass 1 windows over ONLY the ~8 decision columns (parquet column-projection
    #          means S3 ships just those) -> the set of (hex, ts) keys to keep.
    #   Pass 2 reads the full rows, keeps only those keys, sorts by (hex, ts).
    # This avoids materialising all ~50 columns through the window/sort.
    hb, turn, vert = _THIN_HEARTBEAT_S, _THIN_TURN_DEG, _THIN_VERT_FT
    con.execute(f"""
        CREATE TEMP TABLE _kept AS
        WITH slim AS (
            SELECT hex, ts, track, alt_baro, squawk, flight, is_snapshot, is_removed FROM {src}
        ),
        base AS (
            SELECT *,
                lag(track)    OVER w AS _p_trk,
                lag(alt_baro) OVER w AS _p_alt,
                lag(squawk)   OVER w AS _p_sq,
                lag(flight)   OVER w AS _p_fl,
                row_number()  OVER w AS _rn_first,
                row_number()  OVER (PARTITION BY hex ORDER BY ts DESC) AS _rn_last,
                row_number()  OVER (PARTITION BY hex, CAST(epoch(ts)/{hb} AS BIGINT) ORDER BY ts) AS _rn_hb
            FROM slim WINDOW w AS (PARTITION BY hex ORDER BY ts)
        )
        SELECT DISTINCT hex, ts FROM base
        WHERE _rn_first = 1 OR _rn_last = 1 OR _rn_hb = 1
           OR LEAST(abs(track - _p_trk), 360 - abs(track - _p_trk)) >= {turn}
           OR abs(alt_baro - _p_alt) >= {vert}
           OR squawk IS DISTINCT FROM _p_sq
           OR flight IS DISTINCT FROM _p_fl
           OR is_snapshot OR is_removed
    """)
    # Pass 2: full rows for the chosen keys, sorted for compression locality.
    # NOTE: inline src (our own validated config — no injection vector); single `?`
    # for the TO destination (DuckDB mis-binds COPY with two positional params).
    con.execute(
        f"COPY (SELECT {proj} FROM {src} WHERE (hex, ts) IN (SELECT hex, ts FROM _kept) "
        f"ORDER BY hex, ts) TO ? "
        "(FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 6, ROW_GROUP_SIZE 1000000)",
        [out_parquet],
    )
    con.execute("DROP TABLE _kept")
    (kept_rows,) = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [out_parquet]).fetchone()

    size = os.path.getsize(out_parquet)
    digest = _sha256(out_parquet)

    manifest = {
        "dataset": "adsbiq-aircraft-diffs",
        "date": date,
        "rows": int(kept_rows),
        "raw_points": int(row_count),
        "file": os.path.basename(out_parquet),
        "bytes": size,
        "sha256": digest,
        "compression": "zstd",
        "source": "ADSBiq feeder network",
        "thinning": f"adaptive trace: a point is kept on a heading change >= {turn} deg, an "
                    f"altitude change >= {vert} ft, a {hb}s heartbeat, or a state change "
                    f"(squawk/callsign/snapshot/removal). Cruise collapses to the heartbeat; "
                    f"turns and climbs stay full-detail.",
        "schema_note": "append-only ADS-B aircraft state diffs, sorted by (hex, ts); is_snapshot=true "
                       "marks a full row, else only changed fields are populated. lat/lon are degrees; "
                       "alt/speed fields are integers. See repo README for column reference.",
        "license": "ODbL-1.0",
        "generated_by": "adsbiq daily_archive.py",
    }
    with open(out_manifest, "w") as f:
        json.dump(manifest, f, indent=2)

    pct = 100.0 * kept_rows / row_count if row_count else 0.0
    print(f"[ok] {date}: {row_count:,} raw -> {kept_rows:,} kept ({pct:.1f}%) -> {out_parquet} "
          f"({size/1e6:.1f} MB, sha256={digest[:12]}...)")

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
