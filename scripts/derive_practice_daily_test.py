from __future__ import annotations
import datetime as dt
import unittest

from derive_practice_daily import normalize_nflverse_status, fan_out_to_daily, week_wednesday


class TestNormalizeNflverseStatus(unittest.TestCase):
    def test_normalize_dnp(self):
        assert normalize_nflverse_status('Did Not Participate In Practice') == 'DNP'

    def test_normalize_limited(self):
        assert normalize_nflverse_status('Limited Participation in Practice') == 'Limited'

    def test_normalize_full(self):
        assert normalize_nflverse_status('Full Participation in Practice') == 'Full'

    def test_normalize_blank(self):
        assert normalize_nflverse_status('') is None
        assert normalize_nflverse_status(None) is None


class TestFanOutToDaily(unittest.TestCase):
    def test_fan_out_three_rows_per_week(self):
        weekly = {
            'player_id': 'uuid-1',
            'practice_status': 'Did Not Participate In Practice',
            'report_primary_injury': 'hamstring',
            'team': 'KC',
            'position': 'QB',
        }
        week_start_wed = dt.date(2026, 9, 9)  # Wednesday of NFL Week 1 (Sept 10 2026 is Thursday)
        rows = fan_out_to_daily(weekly, week_start_wed)
        assert len(rows) == 3
        assert [r['report_date'] for r in rows] == [
            '2026-09-09', '2026-09-10', '2026-09-11',
        ]
        assert all(r['status'] == 'DNP' for r in rows)
        assert all(r['source'] == 'nflverse' for r in rows)
        assert all(r['description'] == 'hamstring' for r in rows)

    def test_fan_out_skips_no_status(self):
        weekly = {
            'player_id': 'uuid-1',
            'practice_status': '',  # no nflverse status
            'report_primary_injury': None,
            'team': 'KC',
            'position': 'QB',
        }
        rows = fan_out_to_daily(weekly, dt.date(2026, 9, 9))
        assert rows == []


class TestWeekWednesday(unittest.TestCase):
    def test_week_wednesday_approximation(self):
        # Week 1 2026: Sept 10 is Thursday opener (Eagles vs Cowboys).
        # Wednesday before = Sept 9.
        assert week_wednesday(2026, 1) == dt.date(2026, 9, 9)
        # Week 2 = next Wednesday (Sept 16)
        assert week_wednesday(2026, 2) == dt.date(2026, 9, 16)


if __name__ == '__main__':
    unittest.main()
