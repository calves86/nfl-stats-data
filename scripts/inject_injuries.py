#!/usr/bin/env python3
"""
inject_injuries.py — pull nflverse weekly NFL injuries and load Postgres.

Inserts rows into public.player_injury_status_weekly, then runs
public.derive_injury_events() to refresh the events table. Unmatched
gsis_ids land in public.sync_unresolved_injury_ids for review.

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
from typing import Any

import psycopg2
import psycopg2.extras

try:
    import nfl_data_py as nfl
except ImportError as e:
    print(f"[inject_injuries] missing dep: {e}. Install with: pip install nfl_data_py", file=sys.stderr)
    sys.exit(2)


def parse_seasons(arg: str) -> list[int]:
    if '-' in arg:
        a, b = arg.split('-', 1)
        return list(range(int(a), int(b) + 1))
    return [int(s) for s in arg.split(',')]


def hash_row(row: dict[str, Any], source: str) -> str:
    fields = (
        source, row.get('gsis_id'), row.get('season'), row.get('week'),
        row.get('report_status'), row.get('practice_status'),
        row.get('report_primary_injury'), row.get('report_secondary_injury'),
    )
    return hashlib.sha256('|'.join(str(f) for f in fields).encode()).hexdigest()


def row_to_record(row: dict[str, Any], source: str) -> dict[str, Any]:
    return {
        'gsis_id':                 row['gsis_id'],
        'season':                  int(row['season']),
        'week':                    int(row['week']),
        'report_status':           row.get('report_status') or None,
        'practice_status':         row.get('practice_status') or None,
        'report_primary_injury':   row.get('report_primary_injury') or None,
        'report_secondary_injury': row.get('report_secondary_injury') or None,
        'team':                    row.get('team') or None,
        'position':                row.get('position') or None,
        'source':                  source,
        'source_row_hash':         hash_row(row, source),
    }


def fetch_player_id_map(conn, gsis_ids: set[str]) -> dict[str, str]:
    if not gsis_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """SELECT gsis_id, id::text
                 FROM players
                WHERE gsis_id = ANY(%s)""",
            (list(gsis_ids),),
        )
        return {r[0]: r[1] for r in cur.fetchall()}


def upsert_weekly(
    conn, records: list[dict[str, Any]], id_map: dict[str, str], full_names: dict[str, str]
) -> tuple[int, int]:
    matched, unresolved = 0, 0
    with conn.cursor() as cur:
        for r in records:
            pid = id_map.get(r['gsis_id'])
            if pid:
                matched += 1
                cur.execute(
                    """INSERT INTO player_injury_status_weekly (
                         player_id, gsis_id, season, week, report_status, practice_status,
                         report_primary_injury, report_secondary_injury, team, position,
                         source, source_row_hash
                       ) VALUES (
                         %(pid)s, %(gsis_id)s, %(season)s, %(week)s, %(report_status)s,
                         %(practice_status)s, %(report_primary_injury)s,
                         %(report_secondary_injury)s, %(team)s, %(position)s,
                         %(source)s, %(source_row_hash)s
                       )
                       ON CONFLICT (gsis_id, season, week, source) DO UPDATE SET
                         report_status           = EXCLUDED.report_status,
                         practice_status         = EXCLUDED.practice_status,
                         report_primary_injury   = EXCLUDED.report_primary_injury,
                         report_secondary_injury = EXCLUDED.report_secondary_injury,
                         source_row_hash         = EXCLUDED.source_row_hash,
                         ingested_at             = now()
                       WHERE player_injury_status_weekly.source_row_hash <> EXCLUDED.source_row_hash""",
                    {**r, 'pid': pid},
                )
            else:
                unresolved += 1
                cur.execute(
                    """INSERT INTO sync_unresolved_injury_ids
                         (gsis_id, full_name, team, position)
                       VALUES (%(gsis_id)s, %(full_name)s, %(team)s, %(position)s)
                       ON CONFLICT (gsis_id) DO UPDATE SET
                         last_seen_at = now(),
                         seen_count   = sync_unresolved_injury_ids.seen_count + 1,
                         full_name    = COALESCE(EXCLUDED.full_name, sync_unresolved_injury_ids.full_name),
                         team         = COALESCE(EXCLUDED.team, sync_unresolved_injury_ids.team),
                         position     = COALESCE(EXCLUDED.position, sync_unresolved_injury_ids.position)""",
                    {**r, 'full_name': full_names.get(r['gsis_id'])},
                )
    return matched, unresolved


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
    print(f'[inject_injuries] pulling nflverse injuries for seasons {seasons}')

    df = nfl.import_injuries(seasons)
    if df.empty:
        print('[inject_injuries] no rows from nflverse')
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

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return 0


if __name__ == '__main__':
    sys.exit(main())
