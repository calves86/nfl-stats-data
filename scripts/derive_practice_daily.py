#!/usr/bin/env python3
"""
derive_practice_daily.py — fan nflverse weekly practice rows into 3 daily rows.

Reads `player_injury_status_weekly` (source='nflverse') for one (season, week)
and writes 3 daily rows (Wed/Thu/Fri) per player into `player_practice_daily`
with source='nflverse'. Sleeper rows always win in `player_practice_latest`
because of source-priority ordering in the view.

Run weekly after `inject_injuries.py` ingests the official report.

Usage:
    .venv/bin/python scripts/derive_practice_daily.py --season 2026 --week 1
    .venv/bin/python scripts/derive_practice_daily.py --season 2025 --week 14

Env:
    DATABASE_URL   Postgres connection string (sslmode=require ok)
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from typing import Optional

import psycopg2
import psycopg2.extras

_STATUS_MAP = {
    'Did Not Participate In Practice':   'DNP',
    'Limited Participation in Practice': 'Limited',
    'Full Participation in Practice':    'Full',
}


def normalize_nflverse_status(raw: Optional[str]) -> Optional[str]:
    """Map verbose nflverse practice strings to CHECK-constrained literals.

    Returns None for blank/missing values (rows with no status are skipped).
    """
    if not raw:
        return None
    return _STATUS_MAP.get(raw)


def week_wednesday(season: int, week: int) -> dt.date:
    """Return the Wednesday on or after Sept 3 of `season`, offset by (week-1) weeks.

    Approximation — real NFL practice-report Wednesday is the one immediately
    before each week's Thursday slate. The Sept 3 + first-Wednesday rule lines
    up correctly with 2024/2025/2026 calendars.

    Examples:
        week_wednesday(2026, 1) → date(2026, 9, 9)   # week before Sept 10 opener
        week_wednesday(2026, 2) → date(2026, 9, 16)
    """
    seed = dt.date(season, 9, 3)
    while seed.weekday() != 2:  # 2 == Wednesday
        seed += dt.timedelta(days=1)
    return seed + dt.timedelta(days=(week - 1) * 7)


def fan_out_to_daily(weekly: dict, week_wed: dt.date) -> list[dict]:
    """Expand one weekly row into 3 daily rows (Wed/Thu/Fri).

    Returns an empty list if the weekly row has no mappable practice_status
    (blank or unknown strings are skipped rather than written with NULL status).
    """
    status = normalize_nflverse_status(weekly.get('practice_status'))
    if not status:
        return []
    out = []
    for offset in (0, 1, 2):  # Wed, Thu, Fri
        day = week_wed + dt.timedelta(days=offset)
        out.append({
            'player_id':   weekly['player_id'],
            'report_date': day.isoformat(),
            'source':      'nflverse',
            'status':      status,
            'description': weekly.get('report_primary_injury'),
            'nfl_team':    weekly.get('team'),
            'position':    weekly.get('position'),
        })
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Fan nflverse weekly practice rows into 3 daily rows per player.',
    )
    parser.add_argument('--season', type=int, required=True, help='NFL season year, e.g. 2026')
    parser.add_argument('--week',   type=int, required=True, help='NFL week number, e.g. 1')
    args = parser.parse_args()

    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print('[derive_practice_daily] DATABASE_URL required', file=sys.stderr)
        return 2

    week_wed = week_wednesday(args.season, args.week)
    print(f'[derive_practice_daily] season={args.season} week={args.week} week_wednesday={week_wed}')

    conn = psycopg2.connect(db_url)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT player_id, practice_status, report_primary_injury, team, position
            FROM player_injury_status_weekly
            WHERE season = %s AND week = %s AND source = 'nflverse' AND player_id IS NOT NULL
            """,
            (args.season, args.week),
        )
        weekly_rows = [
            {
                'player_id':             r[0],
                'practice_status':       r[1],
                'report_primary_injury': r[2],
                'team':                  r[3],
                'position':              r[4],
            }
            for r in cur.fetchall()
        ]

        daily: list[dict] = []
        for w in weekly_rows:
            daily.extend(fan_out_to_daily(w, week_wed))

        upserted = 0
        if daily:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO player_practice_daily
                  (player_id, report_date, source, status, description, nfl_team, position)
                VALUES %s
                ON CONFLICT (player_id, report_date, source)
                DO UPDATE SET
                  status      = EXCLUDED.status,
                  description = EXCLUDED.description,
                  nfl_team    = EXCLUDED.nfl_team,
                  position    = EXCLUDED.position,
                  ingested_at = now()
                """,
                [
                    (
                        row['player_id'], row['report_date'], row['source'],
                        row['status'], row['description'], row['nfl_team'], row['position'],
                    )
                    for row in daily
                ],
                page_size=1000,
            )
            upserted = len(daily)

        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    result = {
        'summary':            'ok',
        'season':             args.season,
        'week':               args.week,
        'weekly_input_rows':  len(weekly_rows),
        'daily_upserted':     upserted,
    }
    print(json.dumps(result))
    return 0


if __name__ == '__main__':
    sys.exit(main())
