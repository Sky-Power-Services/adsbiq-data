# ADSBiq history — daily ADS-B aircraft data

[![Latest release](https://img.shields.io/github/v/release/Sky-Power-Services/adsbiq-data?display_name=tag&label=latest%20day&color=brightgreen)](https://github.com/Sky-Power-Services/adsbiq-data/releases/latest)
[![Updated daily](https://img.shields.io/badge/updated-daily%20~00%3A30%20UTC-blue)](https://github.com/Sky-Power-Services/adsbiq-data/releases)
[![License: ODbL-1.0](https://img.shields.io/badge/license-ODbL--1.0-lightgrey)](LICENSE)

Free, open daily snapshots of aircraft seen by the [ADSBiq](https://adsbiq.com)
feeder network. One [Release](../../releases) per UTC day, generated
automatically. Inspired by [adsb.lol](https://github.com/adsblol)'s open-data
drops.

## What's in each release
| Asset | Description |
|---|---|
| `aircraft_diffs_<date>.parquet` | That day's ADS-B aircraft state, as append-only **diffs** (zstd Parquet) |
| `manifest_<date>.json` | `rows`, `bytes`, `sha256`, schema note, license |

## Latest — permanent download link

The newest day is always at a fixed URL (no date needed):

```bash
curl -L -o aircraft_diffs_latest.parquet \n  https://github.com/Sky-Power-Services/adsbiq-data/releases/latest/download/aircraft_diffs_latest.parquet
```

Or query it in place — DuckDB reads the release URL directly:

```sql
SELECT * FROM 'https://github.com/Sky-Power-Services/adsbiq-data/releases/latest/download/aircraft_diffs_latest.parquet' LIMIT 10;
```

A committed **`sample.parquet`** (200 rows) at the repo root lets you peek at the schema without a download.

## Data model — append-only diffs
Rows are time-ordered state changes, not full snapshots every tick (keeps files
small):
- `is_snapshot = true` → a **full** row: every known field for that aircraft at that moment.
- `is_snapshot = false` → a **sparse diff**: only the fields that *changed* since the prior row are populated; everything else is null. Carry forward the last value per `hex`.
- `is_removed = true` → the aircraft dropped off coverage.

To reconstruct full state at any time, forward-fill per `hex` ordered by `ts`.

## Resolution — adaptive trace thinning
Rows are **adaptively thinned** (adsb.lol trace style), not raw 1 Hz. A point is
kept when the aircraft *does something* — a heading change ≥ 5°, an altitude change
≥ 80 ft, or a state change (squawk/callsign/snapshot/removal) — plus a **30 s
heartbeat** so straight cruise still has a point at least every 30 s. The result:
turns and climbs keep full detail, while redundant cruise points are dropped
(~10× smaller files). The exact thresholds are recorded in each `manifest.json`
under `thinning`, with `raw_points` vs kept `rows`.

Files are sorted by `(hex, ts)`. `lat`/`lon` are degrees; `alt_*`/`gs`/`ias`/`tas`
and the rate fields are integers.

### Key columns
`ts` (UTC timestamp), `date`, `is_snapshot`, `is_removed`, `hex` (ICAO 24-bit),
`src`, `flight`, `lat`, `lon`, `alt_baro`, `alt_geom`, `gs`, `track`, `squawk`,
`type`, `category`, plus the full readsb field set (nav_*, nic/nac/sil quality,
winds `wd`/`ws`, `oat`/`tat`, etc.). See `manifest.json` and the Parquet schema
for the complete list.

## Loading
```python
import duckdb
duckdb.sql("SELECT hex, ts, lat, lon, alt_baro FROM 'aircraft_diffs_2026-06-27.parquet' WHERE is_snapshot LIMIT 10")
```
```python
import pandas as pd
df = pd.read_parquet("aircraft_diffs_2026-06-27.parquet")
```

## Related

- [vdliq-data](https://github.com/Sky-Power-Services/vdliq-data) — VDL2/ACARS datalink message data
- [adsbiq-feeder](https://github.com/adsbiq/adsbiq-feeder) — join the network: open-source feeder install scripts
- [ADSBiq](https://adsbiq.com) · [become a feeder](https://adsbiq.com/join) · open data at [adsbiq.com/data](https://adsbiq.com/data)

## License
Open Database License (**ODbL-1.0**). Attribution: "ADSBiq feeder network".
