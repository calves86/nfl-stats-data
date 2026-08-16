"""Wall-clock bounds for the nflverse network fetches.

Why this exists
---------------
`inject_injuries.py` runs on injury-weekly-ingest.service, which allows
`TimeoutStartSec=600`. It opens a `sync_runs` row BEFORE the nflverse fetch (on
its own autocommit connection, deliberately) and closes it in a `finally`. That
works for every failure the process can observe — but not for a hang: systemd
sends SIGTERM at the 600s boundary, no `except` can absorb that, and the row
stays `'running'` forever. A stranded row pins the health watchdog red for 30
days until the reaper sweeps it.

`nfl_data_py` exposes no timeout parameter, so the fetch has to be bounded from
the outside.

Why signal.alarm rather than a socket timeout
---------------------------------------------
`nfl_data_py`'s transport is an implementation detail — pandas hands https URLs
to fsspec, which uses aiohttp, and `socket.setdefaulttimeout()` does not reach
asyncio at all. A SIGALRM whose handler raises interrupts the call whatever the
transport: verified against both a blocked socket read and a bare `time.sleep`.
It also bounds the WHOLE fetch rather than a single read, which is what the unit
budget actually cares about.

The socket default is still set as defence in depth, for the urllib/requests
paths where it does apply.

⚠️ Main thread only. `signal.signal` raises ValueError anywhere else, so never
arm this inside a worker thread.
"""
from __future__ import annotations

import contextlib
import signal
import socket

# Half of injury-weekly-ingest.service's TimeoutStartSec=600, leaving the other
# half for the DB work and for closing the sync_runs row. Enormous headroom: the
# nflverse files are small (injuries_2025.parquet 97KB, players.parquet 3.4MB,
# roster_weekly_2025.parquet 851KB — all sub-second, measured 2026-08-16). The
# budget exists to catch a HANG, not to pace a healthy download.
FETCH_DEADLINE_SECONDS = 300

# Per-read bound for the transports where it applies. Well inside the wall-clock
# budget so a single stalled read fails long before the whole-fetch deadline.
SOCKET_READ_TIMEOUT_SECONDS = 60


class FetchDeadlineExceeded(RuntimeError):
    """A network fetch outran its wall-clock budget.

    Raised (not SIGTERM'd), so the caller's existing handler can close its
    sync_runs row as 'failed' with a real reason instead of stranding it.
    """


def install_socket_default_timeout(seconds: int = SOCKET_READ_TIMEOUT_SECONDS) -> None:
    """Bound any single blocking socket read. Defence in depth only — this does
    NOT reach asyncio, which is why the wall-clock guard below is the primary."""
    socket.setdefaulttimeout(seconds)


@contextlib.contextmanager
def fetch_deadline(seconds: int = FETCH_DEADLINE_SECONDS, what: str = 'network fetch'):
    """Fail the wrapped call after `seconds`, whatever it is blocked on.

    `what` is echoed in the exception because that string ends up in
    sync_runs.notes, where "something timed out" is useless.
    """
    def _on_alarm(signum, frame):  # noqa: ARG001
        raise FetchDeadlineExceeded(f'{what} exceeded its {seconds}s deadline')

    previous_handler = signal.signal(signal.SIGALRM, _on_alarm)
    signal.alarm(seconds)
    try:
        yield
    finally:
        # Cancel BEFORE restoring: a leaked alarm would fire later, during the DB
        # write, and be misattributed to the fetch.
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)
