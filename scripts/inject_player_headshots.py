#!/usr/bin/env python3
"""
inject_player_headshots.py — fill missing players.photo_url from nflverse rosters.

Sleeper's player meta (the original photo source via
backend/scripts/backfill-players-from-meta.mjs) has sparse coverage for
defensive players (LB / DB / CB / S / DL / etc.), so the IDPs surfaced by
the Injury Tool and NGS leaderboards render with no headshot. nflverse's
seasonal rosters carry headshot_url for ~97% of IDP players. This script
fills the gap idempotently — only writes when photo_url IS NULL, so existing
Sleeper photos are never overwritten.

Mirror of inject_injuries.py for env/DB/ID-resolution patterns. The
fetch_player_id_map helper is duplicated rather than shared because there
are only two callers; extract to a sibling module if a third one shows up.

Usage:
    .venv/bin/python scripts/inject_player_headshots.py --seasons 2024-2026
    .venv/bin/python scripts/inject_player_headshots.py --seasons 2025

Env:
    DATABASE_URL   Postgres connection string (sslmode=require ok)
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Any

import psycopg2
import psycopg2.extras

try:
    import nfl_data_py as nfl
except ImportError as e:
    print(f"[inject_player_headshots] missing dep: {e}. Install with: pip install nfl_data_py", file=sys.stderr)
    sys.exit(2)


def parse_seasons(arg: str) -> list[int]:
    if '-' in arg:
        a, b = arg.split('-', 1)
        return list(range(int(a), int(b) + 1))
    return [int(s) for s in arg.split(',')]


def fetch_player_id_map(conn, gsis_ids: set[str]) -> dict[str, str]:
    """Return {gsis_id: player_uuid}. Same algorithm as inject_injuries.py:
    direct lookup against player_external_ids source='gsis', then nflverse
    gsis→sleeper crosswalk → source='sleeper' lookup, with back-fill of the
    new gsis entries so future runs are fast."""
    if not gsis_ids:
        return {}

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

    try:
        id_df = nfl.import_ids()
        gsis_to_sleeper: dict[str, str] = {}
        for _, row in id_df.iterrows():
            g = row.get('gsis_id')
            s = row.get('sleeper_id')
            if g and g in missing and s and str(s) != 'nan':
                gsis_to_sleeper[g] = str(int(float(s)))
    except Exception as exc:
        print(f'[inject_player_headshots] nflverse id mapping failed: {exc}', file=sys.stderr)
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
            print(f'[inject_player_headshots] back-filled {len(new_gsis_entries)} gsis entries into player_external_ids')

    return result


def collect_headshots(seasons: list[int]) -> dict[str, str]:
    """Pull seasonal rosters for the given seasons and return {gsis_id:
    headshot_url}. When the same gsis appears across multiple seasons, the
    most recent non-null headshot wins (rookies sometimes get an updated
    photo year-over-year)."""
    df = nfl.import_seasonal_rosters(seasons)
    if df.empty:
        return {}
    df = df[df['headshot_url'].notna() & df['player_id'].notna() & (df['player_id'] != '')]
    df = df.sort_values('season')  # ascending → later overwrites earlier
    headshots: dict[str, str] = {}
    for _, row in df.iterrows():
        headshots[row['player_id']] = row['headshot_url']
    return headshots


def update_photos(conn, photo_rows: list[tuple[str, str]]) -> int:
    """Bulk UPDATE players SET photo_url = v.headshot_url WHERE photo_url IS NULL.
    Returns count of rows actually updated (Postgres reports affected rowcount)."""
    if not photo_rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """UPDATE players p
                  SET photo_url = v.headshot_url
                 FROM (VALUES %s) AS v(player_id, headshot_url)
                WHERE p.id = v.player_id::uuid
                  AND p.photo_url IS NULL""",
            photo_rows,
            template='(%s, %s)',
            page_size=1000,
        )
        return cur.rowcount


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--seasons', default='2024-2026', help='e.g. 2025 or 2024-2026 or 2023,2024')
    ap.add_argument('--dry-run', action='store_true', help='resolve IDs but skip the UPDATE')
    args = ap.parse_args()

    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print('[inject_player_headshots] DATABASE_URL missing', file=sys.stderr)
        return 2

    seasons = parse_seasons(args.seasons)
    print(f'[inject_player_headshots] pulling nflverse seasonal rosters for {seasons}')

    headshots = collect_headshots(seasons)
    print(f'[inject_player_headshots] collected {len(headshots)} unique gsis→headshot mappings')

    if not headshots:
        return 0

    conn = psycopg2.connect(db_url)
    try:
        id_map = fetch_player_id_map(conn, set(headshots.keys()))
        print(f'[inject_player_headshots] resolved {len(id_map)}/{len(headshots)} gsis ids to player uuids')

        rows = [(pid, headshots[gsis]) for gsis, pid in id_map.items()]

        if args.dry_run:
            print(f'[inject_player_headshots] dry-run: would attempt UPDATE on {len(rows)} rows')
            print('[inject_player_headshots] (only rows where photo_url IS NULL would actually change)')
            return 0

        updated = update_photos(conn, rows)
        print(f'[inject_player_headshots] UPDATE players SET photo_url … → {updated} rows changed (NULLs filled)')
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return 0


if __name__ == '__main__':
    sys.exit(main())
