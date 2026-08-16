from __future__ import annotations
import os
import sys
import unittest
import urllib.error
from datetime import date
from unittest import mock

import pandas as pd

import inject_injuries
from inject_injuries import (
    row_to_record, hash_row, fetch_injuries,
    SYNC_STEP, start_sync_run, finish_sync_run,
)

SAMPLE = {
    'season': 2024, 'week': 5, 'team': 'PHI',
    'gsis_id': '00-0036389', 'full_name': 'Jalen Hurts', 'position': 'QB',
    'report_status': 'Questionable',
    'practice_status': 'Limited',
    'report_primary_injury': 'Knee',
    'report_secondary_injury': None,
    'practice_primary_injury': 'Hamstring',
    'practice_secondary_injury': None,
}

class TestRowToRecord(unittest.TestCase):
    def test_extracts_all_fields(self):
        r = row_to_record(SAMPLE, source='nflverse')
        self.assertEqual(r['gsis_id'], '00-0036389')
        self.assertEqual(r['season'], 2024)
        self.assertEqual(r['week'], 5)
        self.assertEqual(r['report_status'], 'Questionable')
        self.assertEqual(r['report_primary_injury'], 'Knee')
        self.assertEqual(r['source'], 'nflverse')

    def test_captures_practice_report_injury(self):
        # The body part for players with no game-status designation lives here.
        r = row_to_record(SAMPLE, source='nflverse')
        self.assertEqual(r['practice_primary_injury'], 'Hamstring')
        self.assertIsNone(r['practice_secondary_injury'])

    def test_hash_is_stable(self):
        h1 = hash_row(SAMPLE, source='nflverse')
        h2 = hash_row(dict(SAMPLE), source='nflverse')
        self.assertEqual(h1, h2)

    def test_hash_changes_when_status_changes(self):
        h1 = hash_row(SAMPLE, source='nflverse')
        s2 = dict(SAMPLE); s2['report_status'] = 'Out'
        self.assertNotEqual(h1, hash_row(s2, source='nflverse'))

    def test_hash_changes_when_practice_injury_changes(self):
        # Re-ingest must overwrite existing rows (which were hashed without the
        # practice fields), so the hash has to fold practice injuries in.
        h1 = hash_row(SAMPLE, source='nflverse')
        s2 = dict(SAMPLE); s2['practice_primary_injury'] = 'Ankle'
        self.assertNotEqual(h1, hash_row(s2, source='nflverse'))

    def test_handles_missing_optional_fields(self):
        thin = {'season':2024,'week':5,'team':'PHI','gsis_id':'00-0036389',
                'full_name':'X','position':'QB'}
        r = row_to_record(thin, source='nflverse')
        self.assertIsNone(r['report_status'])
        self.assertIsNone(r['report_primary_injury'])
        self.assertIsNone(r['practice_primary_injury'])
        self.assertIsNone(r['practice_secondary_injury'])

def _http_error(code, url='https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_2026.parquet'):
    return urllib.error.HTTPError(url, code, 'err', {}, None)


def _http_404():
    return _http_error(404)


class TestIsMissingFileError(unittest.TestCase):
    """Only a genuine 'file not found' counts as 'not published yet'; anything
    else (a 5xx outage, a connection reset) must be treated as a real failure."""

    def test_http_404_is_missing(self):
        self.assertTrue(inject_injuries._is_missing_file_error(_http_404()))

    def test_http_503_is_not_missing(self):
        self.assertFalse(inject_injuries._is_missing_file_error(_http_error(503)))

    def test_file_not_found_is_missing(self):
        self.assertTrue(inject_injuries._is_missing_file_error(FileNotFoundError('injuries_2026.parquet')))

    def test_wrapped_404_message_is_missing(self):
        # A future pandas/fsspec backend may wrap the 404 in another type.
        self.assertTrue(inject_injuries._is_missing_file_error(Exception('HTTP Error 404: Not Found')))

    def test_requests_style_404_message_is_missing(self):
        self.assertTrue(inject_injuries._is_missing_file_error(
            Exception('404 Client Error: Not Found for url: https://.../injuries_2026.parquet')))

    def test_parse_error_mentioning_not_found_is_not_missing(self):
        # A truncated/corrupt parquet in-season must surface, not be swallowed.
        self.assertFalse(inject_injuries._is_missing_file_error(
            Exception('ArrowInvalid: Parquet magic bytes not found in footer')))

    def test_connection_error_is_not_missing(self):
        self.assertFalse(inject_injuries._is_missing_file_error(ConnectionError('reset')))


class TestFetchInjuries(unittest.TestCase):
    """The scheduled job must exit cleanly when nflverse hasn't published the
    current season's file yet — for the WHOLE offseason including the Sept-1 to
    Week-1 gap — but a failure for a season that should already exist, or a
    non-404 outage for the current season, must still surface loudly."""

    def test_current_season_404_offseason_returns_empty(self):
        # July: the actual ongoing production incident — must not crash.
        with mock.patch.object(inject_injuries.nfl, 'import_injuries', side_effect=_http_404()):
            df = fetch_injuries([2026], today=date(2026, 7, 24))
        self.assertTrue(df.empty)

    def test_current_season_404_early_september_returns_empty(self):
        # Sept 1 -> Week 1 (~Sept 10): file still absent. Regression guard for the
        # boundary bug the calendar-cutoff version had (crashed this window).
        with mock.patch.object(inject_injuries.nfl, 'import_injuries', side_effect=_http_404()):
            df = fetch_injuries([2026], today=date(2026, 9, 3))
        self.assertTrue(df.empty)

    def test_past_season_404_reraises(self):
        # A 404 for a season whose file must exist is real breakage.
        with mock.patch.object(inject_injuries.nfl, 'import_injuries', side_effect=_http_404()):
            with self.assertRaises(urllib.error.HTTPError):
                fetch_injuries([2024], today=date(2026, 9, 3))

    def test_current_season_non_404_reraises(self):
        # An in-season 5xx outage must NOT be swallowed as 'not published'.
        with mock.patch.object(inject_injuries.nfl, 'import_injuries', side_effect=_http_error(503)):
            with self.assertRaises(urllib.error.HTTPError):
                fetch_injuries([2026], today=date(2026, 11, 15))

    def test_current_season_connection_error_reraises(self):
        # Only 'file not found' is tolerated; a transient network error surfaces.
        with mock.patch.object(inject_injuries.nfl, 'import_injuries', side_effect=ConnectionError('reset')):
            with self.assertRaises(ConnectionError):
                fetch_injuries([2026], today=date(2026, 7, 24))

    def test_future_season_missing_file_returns_empty(self):
        # Defensive: NFL_CURRENT_SEASON accidentally set ahead of the calendar.
        with mock.patch.object(inject_injuries.nfl, 'import_injuries', side_effect=FileNotFoundError('injuries_2027.parquet')):
            df = fetch_injuries([2027], today=date(2026, 7, 24))
        self.assertTrue(df.empty)

    def test_success_returns_rows(self):
        frame = pd.DataFrame([SAMPLE])
        with mock.patch.object(inject_injuries.nfl, 'import_injuries', return_value=frame):
            df = fetch_injuries([2024], today=date(2026, 7, 24))
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['gsis_id'], '00-0036389')

    def test_backfill_skips_unpublished_keeps_published(self):
        # Multi-season backfill: 2024/2025 return rows, 2026 404s and is skipped.
        def side_effect(years):
            (y,) = years
            if y == 2026:
                raise _http_404()
            return pd.DataFrame([dict(SAMPLE, season=y)])
        with mock.patch.object(inject_injuries.nfl, 'import_injuries', side_effect=side_effect):
            df = fetch_injuries([2024, 2025, 2026], today=date(2026, 7, 24))
        self.assertEqual(sorted(df['season'].tolist()), [2024, 2025])

    def test_each_season_fetched_individually(self):
        calls = []
        def side_effect(years):
            calls.append(list(years))
            return pd.DataFrame([dict(SAMPLE, season=years[0])])
        with mock.patch.object(inject_injuries.nfl, 'import_injuries', side_effect=side_effect):
            fetch_injuries([2023, 2024], today=date(2026, 7, 24))
        self.assertEqual(calls, [[2023], [2024]])


class FakeCursor:
    """psycopg2 cursors are context managers; record what gets executed."""

    def __init__(self, conn):
        self.conn = conn
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.calls.append((' '.join(sql.split()), params))
        lowered = sql.lower()
        if 'returning id' in lowered:
            self._row = ('run-uuid-1',)
        elif 'derive_injury_events' in lowered:
            self._row = ({'events': 0},)

    def fetchone(self):
        return self._row


class FakeConn:
    def __init__(self):
        self.calls = []
        self.autocommit = False
        self.closed = False
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class TestSyncRunLogging(unittest.TestCase):
    """The weekly ingest wrote nothing to sync_runs, so health-sync could only
    watch the matview refreshes downstream — and those succeed happily over
    frozen source data."""

    def test_step_name_matches_the_watchdog(self):
        # Mirrors MONITORED_STEPS in commish/backend/scripts/lib/syncHealth.mjs.
        # Cross-repo, so both sides pin the literal; changing one alone silently
        # stops the monitoring rather than failing anything.
        self.assertEqual(SYNC_STEP, 'injury_weekly_ingest')

    def test_start_inserts_a_running_row_and_returns_its_id(self):
        conn = FakeConn()
        run_id = start_sync_run(conn)
        self.assertEqual(run_id, 'run-uuid-1')
        sql, params = conn.calls[0]
        self.assertIn('INSERT INTO sync_runs', sql)
        self.assertIn("'running'", sql)
        self.assertEqual(params, (SYNC_STEP,))

    def test_finish_records_ok_with_counts_and_notes(self):
        conn = FakeConn()
        finish_sync_run(conn, 'run-uuid-1', 'ok', notes='matched=12', rows_affected=12)
        sql, params = conn.calls[0]
        self.assertIn('UPDATE sync_runs', sql)
        self.assertIn('finished_at = now()', sql)
        self.assertEqual(params, ('ok', 'matched=12', 12, 'run-uuid-1'))

    def test_finish_truncates_a_huge_failure_message(self):
        conn = FakeConn()
        finish_sync_run(conn, 'run-uuid-1', 'failed', notes='x' * 5000)
        _, params = conn.calls[0]
        self.assertEqual(len(params[1]), 2000)

    def test_finish_rejects_a_status_the_check_constraint_would_reject(self):
        # sync_runs.status is CHECK-constrained; catching it here beats a
        # constraint violation that rolls back the audit row we came to write.
        conn = FakeConn()
        with self.assertRaises(ValueError):
            finish_sync_run(conn, 'run-uuid-1', 'success')


class TestMainRecordsEveryOutcome(unittest.TestCase):
    """Every path out of main() must leave a terminal sync_runs row behind —
    silence is what made the pipeline invisible in the first place."""

    def _run_main(self, fetch_result=None, fetch_error=None, upsert=None, conns=None):
        """Run main() against fakes. `conns` collects every connection opened, so
        the audit trail is still inspectable when main() raises."""
        opened = conns if conns is not None else []
        fetch = (mock.Mock(side_effect=fetch_error) if fetch_error
                 else mock.Mock(return_value=fetch_result))

        def connect(*_a, **_k):
            opened.append(FakeConn())
            return opened[-1]

        with mock.patch.dict(os.environ, {'DATABASE_URL': 'postgresql://localhost/x'}), \
             mock.patch.object(inject_injuries.psycopg2, 'connect', connect), \
             mock.patch.object(inject_injuries, 'fetch_injuries', fetch), \
             mock.patch.object(inject_injuries, 'fetch_player_id_map', return_value={}), \
             mock.patch.object(inject_injuries, 'upsert_weekly', return_value=(upsert or (0, 0))), \
             mock.patch.object(sys, 'argv', ['inject_injuries.py', '--seasons', '2026']):
            rc = inject_injuries.main()
        return rc, opened[0]

    @staticmethod
    def _terminal(log_conn):
        """The single UPDATE that closes the run out."""
        updates = [c for c in log_conn.calls if 'UPDATE sync_runs' in c[0]]
        assert len(updates) == 1, f'expected exactly one terminal update, got {updates}'
        return updates[0][1]

    def test_offseason_empty_frame_records_ok_not_silence(self):
        # The whole point of item 4: nflverse hasn't published 2026 yet, so the
        # job correctly does nothing. It must still leave an `ok` row, or a
        # quiet March is indistinguishable from a timer that stopped firing.
        rc, log_conn = self._run_main(fetch_result=pd.DataFrame())
        self.assertEqual(rc, 0)
        status, notes, rows, _ = self._terminal(log_conn)
        self.assertEqual(status, 'ok')
        self.assertEqual(rows, 0)
        self.assertIn('skipped=', notes)

    def test_successful_ingest_records_ok_with_row_count(self):
        rc, log_conn = self._run_main(fetch_result=pd.DataFrame([SAMPLE]), upsert=(1, 0))
        self.assertEqual(rc, 0)
        status, notes, rows, _ = self._terminal(log_conn)
        self.assertEqual(status, 'ok')
        self.assertEqual(rows, 1)
        self.assertIn('matched=1', notes)

    def test_a_crash_records_failed_with_the_reason_and_still_raises(self):
        # Loud failure AND an audit row: the run must not be swallowed, and the
        # watchdog must be able to see why it died.
        conns = []
        with self.assertRaises(ConnectionError):
            self._run_main(fetch_error=ConnectionError('nflverse unreachable'), conns=conns)
        status, notes, _, _ = self._terminal(conns[0])
        self.assertEqual(status, 'failed')
        self.assertIn('nflverse unreachable', notes)

    def test_the_running_row_is_committed_before_the_slow_work_starts(self):
        # A SIGTERM during the nflverse fetch must STRAND a visible 'running'
        # row — that is the fingerprint health-sync keys on. If the insert were
        # only visible at final commit, a killed job would leave nothing at all.
        conns, seen = [], {}

        def fetch(*_a, **_k):
            seen['autocommit'] = conns[0].autocommit
            seen['calls'] = list(conns[0].calls)
            return pd.DataFrame()

        with mock.patch.dict(os.environ, {'DATABASE_URL': 'postgresql://localhost/x'}), \
             mock.patch.object(inject_injuries.psycopg2, 'connect',
                               lambda *_a, **_k: conns.append(FakeConn()) or conns[-1]), \
             mock.patch.object(inject_injuries, 'fetch_injuries', mock.Mock(side_effect=fetch)), \
             mock.patch.object(sys, 'argv', ['inject_injuries.py', '--seasons', '2026']):
            inject_injuries.main()

        self.assertTrue(seen['autocommit'], 'the log connection must autocommit')
        self.assertTrue(any('INSERT INTO sync_runs' in c[0] for c in seen['calls']),
                        'the running row must already be written when the slow fetch begins')

    def test_logging_uses_a_connection_separate_from_the_data_transaction(self):
        # inject_injuries rolls the whole ingest back on error. Sharing that
        # connection would roll the audit row back with it — the failure would
        # erase its own evidence.
        #
        # ⚠️ This assertion used to be `assertFalse(any('player_injury_status_weekly'
        # in sql ...))`, which was VACUOUS: _run_main mocks out fetch_player_id_map
        # and upsert_weekly, so that table never appears in any statement on any
        # connection and the check passed even with `conn = log_conn`. Assert the
        # split positively instead — two distinct connections, and nothing but
        # sync_runs travelling on the audit one.
        conns = []
        rc, _ = self._run_main(fetch_result=pd.DataFrame([SAMPLE]), upsert=(1, 0), conns=conns)
        self.assertEqual(rc, 0)

        self.assertEqual(len(conns), 2, 'the ingest must open its own connection')
        self.assertIsNot(conns[0], conns[1], 'the audit log must not share the ingest connection')

        log_conn = conns[0]
        self.assertTrue(log_conn.autocommit)
        for sql, _params in log_conn.calls:
            self.assertIn('sync_runs', sql, f'non-audit statement on the log connection: {sql[:80]}')
        self.assertEqual(log_conn.commits, 0, 'an autocommit connection never needs commit()')


if __name__ == '__main__':
    unittest.main()
