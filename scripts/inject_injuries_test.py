from __future__ import annotations
import unittest
from inject_injuries import row_to_record, hash_row

SAMPLE = {
    'season': 2024, 'week': 5, 'team': 'PHI',
    'gsis_id': '00-0036389', 'full_name': 'Jalen Hurts', 'position': 'QB',
    'report_status': 'Questionable',
    'practice_status': 'Limited',
    'report_primary_injury': 'Knee',
    'report_secondary_injury': None,
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

    def test_hash_is_stable(self):
        h1 = hash_row(SAMPLE, source='nflverse')
        h2 = hash_row(dict(SAMPLE), source='nflverse')
        self.assertEqual(h1, h2)

    def test_hash_changes_when_status_changes(self):
        h1 = hash_row(SAMPLE, source='nflverse')
        s2 = dict(SAMPLE); s2['report_status'] = 'Out'
        self.assertNotEqual(h1, hash_row(s2, source='nflverse'))

    def test_handles_missing_optional_fields(self):
        thin = {'season':2024,'week':5,'team':'PHI','gsis_id':'00-0036389',
                'full_name':'X','position':'QB'}
        r = row_to_record(thin, source='nflverse')
        self.assertIsNone(r['report_status'])
        self.assertIsNone(r['report_primary_injury'])

if __name__ == '__main__':
    unittest.main()
