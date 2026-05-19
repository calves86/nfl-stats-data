from __future__ import annotations
import unittest
from datetime import datetime, timezone

from inject_player_headshots import default_seasons, parse_seasons


def _utc(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=timezone.utc)


class TestDefaultSeasons(unittest.TestCase):
    """4-year sliding window ending in today's calendar year — chosen
    so post-draft rookies enter the window the moment nflverse publishes
    the new season's roster (typically May-July), without operator action."""

    def test_2026_spring_after_draft(self):
        # Matches the original `--seasons 2023-2026` runbook value.
        self.assertEqual(default_seasons(_utc(2026, 5, 18)), [2023, 2024, 2025, 2026])

    def test_2026_in_season(self):
        # Same range still applies once regular-season games start.
        self.assertEqual(default_seasons(_utc(2026, 10, 12)), [2023, 2024, 2025, 2026])

    def test_2026_january_post_super_bowl(self):
        # Jan/Feb 2026: still 4-year window ending in 2026.
        self.assertEqual(default_seasons(_utc(2026, 1, 15)), [2023, 2024, 2025, 2026])

    def test_rolls_into_2027(self):
        # First January 1st of next year — the window slides forward.
        self.assertEqual(default_seasons(_utc(2027, 1, 1)), [2024, 2025, 2026, 2027])

    def test_rolls_into_2028(self):
        self.assertEqual(default_seasons(_utc(2028, 6, 1)), [2025, 2026, 2027, 2028])


class TestParseSeasons(unittest.TestCase):
    def test_range_form(self):
        self.assertEqual(parse_seasons('2024-2026'), [2024, 2025, 2026])

    def test_single_form(self):
        self.assertEqual(parse_seasons('2025'), [2025])

    def test_comma_form(self):
        self.assertEqual(parse_seasons('2023,2024'), [2023, 2024])


if __name__ == '__main__':
    unittest.main()
