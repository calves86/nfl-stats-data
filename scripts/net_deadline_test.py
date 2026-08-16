import signal
import socket
import time
import unittest

from net_deadline import (
    FetchDeadlineExceeded,
    SOCKET_READ_TIMEOUT_SECONDS,
    fetch_deadline,
    install_socket_default_timeout,
)


class FetchDeadlineTest(unittest.TestCase):
    """The point of this guard is that the ingest FAILS ITSELF before systemd
    kills it. injury-weekly-ingest.service allows TimeoutStartSec=600; a SIGTERM
    at that boundary cannot be caught, so the sync_runs row opened before the
    fetch stays 'running' forever and pins the health watchdog red until the
    reaper sweeps it. Raising instead lets the existing `except` close the row.
    """

    def test_raises_after_the_deadline_naming_the_operation(self):
        started = time.time()
        with self.assertRaises(FetchDeadlineExceeded) as ctx:
            with fetch_deadline(1, 'nflverse injuries fetch'):
                time.sleep(30)
        elapsed = time.time() - started
        self.assertLess(elapsed, 10, f'did not interrupt the call (took {elapsed:.1f}s)')
        # The message lands in sync_runs.notes, so it has to say what stalled.
        self.assertIn('nflverse injuries fetch', str(ctx.exception))
        self.assertIn('1', str(ctx.exception))

    def test_does_not_fire_when_the_work_finishes_in_time(self):
        with fetch_deadline(5, 'quick fetch'):
            result = 'done'
        self.assertEqual(result, 'done')

    def test_cancels_the_alarm_on_success(self):
        # A leaked alarm would fire later, in the middle of the DB write, and be
        # misreported as a fetch timeout.
        with fetch_deadline(1, 'quick fetch'):
            pass
        time.sleep(1.5)  # would have fired by now if it were still armed

    def test_cancels_the_alarm_when_the_body_raises_something_else(self):
        with self.assertRaises(ValueError):
            with fetch_deadline(1, 'failing fetch'):
                raise ValueError('nflverse 404')
        time.sleep(1.5)  # a leaked alarm would surface here instead

    def test_restores_the_previous_handler(self):
        sentinel = lambda signum, frame: None  # noqa: E731
        previous = signal.signal(signal.SIGALRM, sentinel)
        try:
            with fetch_deadline(5, 'fetch'):
                pass
            self.assertIs(signal.getsignal(signal.SIGALRM), sentinel)
        finally:
            signal.signal(signal.SIGALRM, previous)

    def test_socket_default_timeout_is_installed_as_defence_in_depth(self):
        previous = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(None)
            install_socket_default_timeout()
            self.assertEqual(socket.getdefaulttimeout(), SOCKET_READ_TIMEOUT_SECONDS)
            # It must stay well inside the wall-clock budget, or it is decorative.
            self.assertLess(SOCKET_READ_TIMEOUT_SECONDS, 300)
        finally:
            socket.setdefaulttimeout(previous)


if __name__ == '__main__':
    unittest.main()
