#!/usr/bin/env python3
"""
inject_injuries.py — pull nflverse weekly NFL injuries and load Postgres.

Inserts rows into public.player_injury_status_weekly, then runs
public.derive_injury_events() to refresh the events table, then
public.derive_injuries_from_reserve() to restore the severities that the
report structurally cannot see. Unmatched gsis_ids land in
public.sync_unresolved_injury_ids for review.

The second derivation is NOT optional: derive_injury_events() overwrites
severity from the injury report on every run, and the report goes blank the
moment a player is placed on injured reserve. Dropping that call silently
returns player_injuries to zero season-ending injuries.

Usage:
    .venv/bin/python scripts/inject_injuries.py --seasons 2024
    .venv/bin/python scripts/inject_injuries.py --seasons 2009-2025

Env:
    DATABASE_URL   Postgres connection string (sslmode=require ok)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.error
from datetime import date
from typing import Any

import psycopg2
import psycopg2.extras

try:
    import nfl_data_py as nfl
except ImportError as e:
    print(f"[inject_injuries] missing dep: {e}. Install with: pip install nfl_data_py", file=sys.stderr)
    sys.exit(2)


# sync_runs step name. Must stay in step with MONITORED_STEPS in
# commish/backend/scripts/lib/syncHealth.mjs — this is a cross-repo, cross-language
# literal, so renaming one side alone stops the watchdog silently instead of
# failing anything. Both sides pin it in a test.
SYNC_STEP = 'injury_weekly_ingest'

_TERMINAL_STATUSES = ('ok', 'partial', 'failed')


def start_sync_run(conn, step: str = SYNC_STEP) -> str:
    """Open a sync_runs row and return its id.

    Must be on an AUTOCOMMIT connection separate from the ingest transaction:
    main() rolls the whole ingest back on error, and a shared connection would
    roll this row back with it — the failure would erase its own evidence. It
    also has to be visible immediately, because a run killed mid-flight (SIGTERM
    at TimeoutStartSec, OOM, reboot) never reaches the UPDATE, and health-sync
    reads that stranded 'running' row as the fingerprint of a kill.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO sync_runs (status, step) VALUES ('running', %s) RETURNING id",
            (step,),
        )
        (run_id,) = cur.fetchone()
    return str(run_id)


def finish_sync_run(conn, run_id: str, status: str, notes: str | None = None,
                    rows_affected: int | None = None) -> None:
    """Close a sync_runs row out. `status` is CHECK-constrained in the table;
    rejecting a bad one here beats a constraint violation that would take down
    the very write we came to make."""
    if status not in _TERMINAL_STATUSES:
        raise ValueError(f'status must be one of {_TERMINAL_STATUSES}, got {status!r}')
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE sync_runs
                  SET status = %s, finished_at = now(), notes = %s, rows_affected = %s
                WHERE id = %s""",
            (status, (notes[:2000] if notes else None), rows_affected, run_id),
        )


def parse_seasons(arg: str) -> list[int]:
    if '-' in arg:
        a, b = arg.split('-', 1)
        return list(range(int(a), int(b) + 1))
    return [int(s) for s in arg.split(',')]


def _is_missing_file_error(exc: Exception) -> bool:
    """Whether a fetch exception means the season's parquet simply isn't there.

    nflverse creates a season's injuries file on demand at ~Week 1; until then
    the URL 404s. pandas surfaces that as a urllib HTTPError 404 today, but a
    different read backend could raise FileNotFoundError or wrap the 404 in
    another type, so we also match well-anchored 'not found' phrasings. A non-404
    HTTP error (e.g. a 5xx outage) and generic parse errors (e.g. a truncated
    parquet's 'magic bytes not found') are deliberately NOT treated as missing,
    so a genuine in-season data problem still surfaces loudly.
    """
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code == 404
    if isinstance(exc, FileNotFoundError):
        return True
    msg = str(exc).lower()
    return (
        'error 404' in msg          # urllib: "HTTP Error 404: Not Found"
        or '404 client error' in msg  # requests
        or '404, message' in msg      # aiohttp / fsspec
        or 'no such file' in msg      # local / fsspec file-not-found text
    )


def fetch_injuries(seasons: list[int], today: date | None = None):
    """Pull nflverse weekly injuries season-by-season, tolerating a season whose
    file nflverse has not published yet.

    The scheduled job runs with the current season, whose injuries parquet does
    not exist until ~Week 1 (early September) — until then the fetch 404s. Before
    this fix that 404 was uncaught and crashed the job on every timer fire from
    March until Week 1. Here a 'file not found' for the current (or a future)
    season is treated as 'nothing to ingest yet' and skipped, so the run exits
    cleanly for the whole offseason including the Sept-1-to-Week-1 gap. A failure
    for a PAST season (its file must already exist) or any non-404 error for the
    current season (a real outage worth seeing) is re-raised so genuine breakage
    still surfaces. Returns a pandas DataFrame (possibly empty).
    """
    import pandas as pd

    current_year = (today or date.today()).year
    frames = []
    for season in seasons:
        try:
            frames.append(nfl.import_injuries([season]))
        except Exception as exc:
            if season >= current_year and _is_missing_file_error(exc):
                print(f'[inject_injuries] season {season} not published by nflverse yet '
                      f'({exc}) — nothing to ingest, skipping')
                continue
            raise
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def hash_row(row: dict[str, Any], source: str) -> str:
    fields = (
        source, row.get('gsis_id'), row.get('season'), row.get('week'),
        row.get('report_status'), row.get('practice_status'),
        row.get('report_primary_injury'), row.get('report_secondary_injury'),
        row.get('practice_primary_injury'), row.get('practice_secondary_injury'),
    )
    return hashlib.sha256('|'.join(str(f) for f in fields).encode()).hexdigest()


def row_to_record(row: dict[str, Any], source: str) -> dict[str, Any]:
    return {
        'gsis_id':                   row['gsis_id'],
        'season':                    int(row['season']),
        'week':                      int(row['week']),
        'report_status':             row.get('report_status') or None,
        'practice_status':           row.get('practice_status') or None,
        'report_primary_injury':     row.get('report_primary_injury') or None,
        'report_secondary_injury':   row.get('report_secondary_injury') or None,
        # nflverse carries the body part here when a player practiced but got no
        # game-status designation (report_primary_injury NULL). Without this the
        # derived body_part defaults to 'undisclosed' for ~36% of rows.
        'practice_primary_injury':   row.get('practice_primary_injury') or None,
        'practice_secondary_injury': row.get('practice_secondary_injury') or None,
        'team':                      row.get('team') or None,
        'position':                  row.get('position') or None,
        'source':                    source,
        'source_row_hash':           hash_row(row, source),
    }


def fetch_player_id_map(conn, gsis_ids: set[str]) -> dict[str, str]:
    """Return {gsis_id: player_uuid} by checking player_external_ids source='gsis'
    first, then falling back to a cross-ref through sleeper IDs via the nflverse
    ID mapping table. Populates player_external_ids source='gsis' as a side effect
    so subsequent calls are fast."""
    if not gsis_ids:
        return {}

    # 1. Direct lookup — already-cached gsis entries.
    result: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(
            """SELECT source_id, player_id::text
                 FROM player_external_ids
                WHERE source = 'gsis'
                  AND source_id = ANY(%s)""",
            (list(gsis_ids),),
        )
        for row in cur.fetchall():
            result[row[0]] = row[1]

    missing = gsis_ids - set(result)
    if not missing:
        return result

    # 2. Cross-reference: nflverse gsis→sleeper mapping, then look up sleeper rows.
    try:
        import nfl_data_py as nfl
        id_df = nfl.import_ids()
        # Build gsis→sleeper map for only the missing gsis_ids
        # sleeper_id comes back as float (e.g. 4046.0) — cast to int string
        gsis_to_sleeper: dict[str, str] = {}
        for _, row in id_df.iterrows():
            g = row.get('gsis_id')
            s = row.get('sleeper_id')
            if g and g in missing and s and str(s) != 'nan':
                gsis_to_sleeper[g] = str(int(float(s)))
    except Exception as exc:
        print(f'[inject_injuries] nflverse id mapping failed: {exc}', file=sys.stderr)
        gsis_to_sleeper = {}

    if gsis_to_sleeper:
        sleeper_ids = list(gsis_to_sleeper.values())
        with conn.cursor() as cur:
            cur.execute(
                """SELECT source_id, player_id::text
                     FROM player_external_ids
                    WHERE source = 'sleeper'
                      AND source_id = ANY(%s)""",
                (sleeper_ids,),
            )
            sleeper_to_pid: dict[str, str] = {r[0]: r[1] for r in cur.fetchall()}

        # Populate gsis results and back-fill player_external_ids
        new_gsis_entries: list[tuple[str, str]] = []
        for gsis, sleeper in gsis_to_sleeper.items():
            pid = sleeper_to_pid.get(sleeper)
            if pid:
                result[gsis] = pid
                new_gsis_entries.append((gsis, pid))

        if new_gsis_entries:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO player_external_ids (source, source_id, player_id)
                       VALUES %s
                       ON CONFLICT (source, source_id) DO NOTHING""",
                    [('gsis', g, p) for g, p in new_gsis_entries],
                )
            print(f'[inject_injuries] back-filled {len(new_gsis_entries)} gsis entries into player_external_ids')

    return result


def upsert_weekly(
    conn, records: list[dict[str, Any]], id_map: dict[str, str], full_names: dict[str, str]
) -> tuple[int, int]:
    """Bulk-upsert via execute_values. Was per-row; over a 150k-row 2009-2025
    backfill against Supabase's pooler that meant 150k×~50ms round trips =
    multi-hour stall. Batches of 1000 cuts that to seconds."""
    # Dedupe within batch on the conflict-key tuples — Postgres rejects
    # ON CONFLICT DO UPDATE if the same conflict key appears twice in one
    # statement. nflverse occasionally lists a player twice in the same
    # week with different injury notes; "last seen wins" matches the prior
    # per-row INSERT semantics.
    matched_by_key: dict[tuple, tuple] = {}
    unresolved_by_gsis: dict[str, tuple] = {}
    for r in records:
        pid = id_map.get(r['gsis_id'])
        if pid:
            key = (r['gsis_id'], r['season'], r['week'], r['source'])
            matched_by_key[key] = (
                pid, r['gsis_id'], r['season'], r['week'],
                r['report_status'], r['practice_status'],
                r['report_primary_injury'], r['report_secondary_injury'],
                r['practice_primary_injury'], r['practice_secondary_injury'],
                r['team'], r['position'], r['source'], r['source_row_hash'],
            )
        else:
            unresolved_by_gsis[r['gsis_id']] = (
                r['gsis_id'], full_names.get(r['gsis_id']),
                r['team'], r['position'],
            )

    matched_rows = list(matched_by_key.values())
    unresolved_rows = list(unresolved_by_gsis.values())

    print(f'[inject_injuries] bulk-upserting {len(matched_rows)} matched + {len(unresolved_rows)} unresolved rows')

    with conn.cursor() as cur:
        if matched_rows:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO player_injury_status_weekly (
                     player_id, gsis_id, season, week, report_status, practice_status,
                     report_primary_injury, report_secondary_injury,
                     practice_primary_injury, practice_secondary_injury, team, position,
                     source, source_row_hash
                   ) VALUES %s
                   ON CONFLICT (gsis_id, season, week, source) DO UPDATE SET
                     report_status             = EXCLUDED.report_status,
                     practice_status           = EXCLUDED.practice_status,
                     report_primary_injury     = EXCLUDED.report_primary_injury,
                     report_secondary_injury   = EXCLUDED.report_secondary_injury,
                     practice_primary_injury   = EXCLUDED.practice_primary_injury,
                     practice_secondary_injury = EXCLUDED.practice_secondary_injury,
                     source_row_hash           = EXCLUDED.source_row_hash,
                     ingested_at               = now()
                   WHERE player_injury_status_weekly.source_row_hash <> EXCLUDED.source_row_hash""",
                matched_rows,
                page_size=1000,
            )
        if unresolved_rows:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO sync_unresolved_injury_ids
                     (gsis_id, full_name, team, position)
                   VALUES %s
                   ON CONFLICT (gsis_id) DO UPDATE SET
                     last_seen_at = now(),
                     seen_count   = sync_unresolved_injury_ids.seen_count + 1,
                     full_name    = COALESCE(EXCLUDED.full_name, sync_unresolved_injury_ids.full_name),
                     team         = COALESCE(EXCLUDED.team, sync_unresolved_injury_ids.team),
                     position     = COALESCE(EXCLUDED.position, sync_unresolved_injury_ids.position)""",
                unresolved_rows,
                page_size=1000,
            )

    return len(matched_rows), len(unresolved_rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--seasons', required=True, help='e.g. 2024 or 2009-2025 or 2023,2024')
    ap.add_argument('--auto-create-unknown', action='store_true', help='reserved for v1.5')
    args = ap.parse_args()

    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print('[inject_injuries] DATABASE_URL missing', file=sys.stderr)
        return 2

    seasons = parse_seasons(args.seasons)

    # The run log opens BEFORE the nflverse fetch, on its own autocommit
    # connection. Both details matter: the fetch is the slow part where a
    # TimeoutStartSec kill lands, and a stranded 'running' row is what tells
    # health-sync the process was killed rather than merely quiet.
    log_conn = psycopg2.connect(db_url)
    log_conn.autocommit = True
    run_id = start_sync_run(log_conn)
    try:
        print(f'[inject_injuries] pulling nflverse injuries for seasons {seasons}')

        df = fetch_injuries(seasons)
        if df.empty:
            # Correctly doing nothing — nflverse has not published the season yet
            # (the whole offseason plus the Sept-1-to-Week-1 gap). This used to
            # return before anything was recorded, which made a quiet March
            # indistinguishable from a timer that had stopped firing. Recording
            # the skip costs one row and makes silence mean exactly one thing.
            print('[inject_injuries] no rows from nflverse')
            finish_sync_run(log_conn, run_id, 'ok', rows_affected=0,
                            notes=f'skipped=no_rows_from_nflverse seasons={seasons}')
            return 0

        raw_rows = df.to_dict(orient='records')
        records = [row_to_record(r, source='nflverse') for r in raw_rows]

        # Carry full_name keyed by gsis_id (for the unresolved audit table)
        full_names = {r.get('gsis_id'): r.get('full_name') for r in raw_rows if r.get('gsis_id')}

        conn = psycopg2.connect(db_url)
        try:
            gsis = {r['gsis_id'] for r in records if r.get('gsis_id')}
            id_map = fetch_player_id_map(conn, gsis)
            matched, unresolved = upsert_weekly(conn, records, id_map, full_names)

            print(f'[inject_injuries] matched={matched} unresolved={unresolved}')

            with conn.cursor() as cur:
                cur.execute('SELECT public.derive_injury_events()')
                (result,) = cur.fetchone()
                print(f'[inject_injuries] derive_injury_events → {json.dumps(result)}')

                # ORDER IS LOAD-BEARING, and this call is not optional.
                #
                # derive_injury_events() rebuilds player_injuries from the weekly
                # injury report, and its upsert sets `severity = EXCLUDED.severity`
                # unconditionally. The report cannot see injured reserve at all — a
                # player placed on IR leaves the active roster and stops appearing
                # in it — so every run above pushes a real severity 5 back down to
                # whatever the report last said, usually 1.
                #
                # derive_injuries_from_reserve() restores it from
                # player_reserve_weeks, merging into the same rows. It must run
                # after EVERY derive, not merely after the roster ingest: the
                # roster step lives in commish's sync-players-weekly (Tue + Sat)
                # while this job runs five days a week, so pairing them there would
                # leave severity 5 destroyed for most of the week.
                #
                # It reads player_reserve_weeks, which is populated independently,
                # so running it here is correct even when no new roster data has
                # landed. Cheap and idempotent by construction.
                cur.execute('SELECT public.derive_injuries_from_reserve()')
                (reserve_result,) = cur.fetchone()
                print(f'[inject_injuries] derive_injuries_from_reserve → {json.dumps(reserve_result)}')

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        finish_sync_run(log_conn, run_id, 'ok', rows_affected=matched,
                        notes=f'seasons={seasons} matched={matched} unresolved={unresolved}')
        return 0
    except Exception as exc:
        # Re-raised: a failed ingest must still exit non-zero so systemd records
        # it. The row just means the watchdog sees the reason too.
        finish_sync_run(log_conn, run_id, 'failed', notes=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        log_conn.close()


if __name__ == '__main__':
    sys.exit(main())
