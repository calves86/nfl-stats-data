# NFL Stats Data

NFL player season totals (2007–2025) scraped from CBS Sports Fantasy.

Covers QB, RB, WR, TE, DST, and K for all seasons.

Used by PHFL and SBT fantasy football dashboards.

## Files

`nfl_stats_{year}.json` — one file per season (2007–2025)

## Schema

```json
[
  {"name":"Christian McCaffrey","pos":"RB","nfl_team":"SF","year":2025,
   "pass_att":0,"pass_comp":0,"pass_yds":0,"pass_td":0,"pass_int":0,
   "rush_att":272,"rush_yds":1459,"rush_td":14,
   "rec_tar":85,"rec_rec":67,"rec_yds":564,"rec_td":7,
   "fumbles_lost":2,"fpts":456.2}
]
```
