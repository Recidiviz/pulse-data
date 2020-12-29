# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Implements test for the RepeatedTimer class."""
import time
from typing import Optional
from unittest import TestCase, mock

from recidiviz.utils.timer import RepeatedTimer


TEST_MIN_SLEEP_TIME = 0.000001


class TestRepeatedTimer(TestCase):
    """Implements test cases for RepeatedTimer."""

    def setUp(self) -> None:
        self.test_fn = mock.Mock()
        self.timer: Optional[RepeatedTimer] = None

    def tearDown(self) -> None:
        if self.timer is not None:
            self.timer.stop_timer()

    def test_doesnt_run_immediately(self) -> None:
        self.timer = RepeatedTimer(5, self.test_fn)
        self.timer.start()
        self.timer.stop_timer()
        self.timer.join()
        self.test_fn.assert_not_called()

    def test_runs_immediately(self) -> None:
        self.timer = RepeatedTimer(5, self.test_fn, run_immediately=True)
        self.timer.start()
        self.timer.stop_timer()
        self.timer.join()
        self.test_fn.assert_called()

    def test_timer_stops(self) -> None:
        self.timer = RepeatedTimer(5, self.test_fn)
        start_time = time.time()
        self.timer.start()
        self.timer.stop_timer()
        self.timer.join()
        end_time = time.time()

        # 2 seconds > total time to stop
        self.assertGreater(2, end_time - start_time)

    def test_timer_called_multiple_times(self) -> None:
        self.timer = RepeatedTimer(0.1, self.test_fn)
        self.timer.start()
        time.sleep(0.5)
        self.timer.stop_timer()
        self.timer.join()

        # Make sure it was called at least 3 times in 0.5 seconds
        self.assertGreater(self.test_fn.call_count, 3)

    def test_stop_timer_within_callback(self) -> None:
        self.timer = RepeatedTimer(0.1, self.test_fn)
        self.test_fn.side_effect = self.timer.stop_timer

        self.timer.start()
        time.sleep(0.5)
        self.timer.join()

        self.assertEqual(self.test_fn.call_count, 1)
