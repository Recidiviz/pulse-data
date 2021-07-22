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
"""
Implements the RepeatedTimer class which can be used to invoke functions on a
repeated basis.
"""
import threading
from typing import Callable


class RepeatedTimer(threading.Thread):
    """Starts a background thread that repeatedly calls the passed in callback with
    the specified delay time in between."""

    def __init__(
        self,
        seconds_to_wait: float,
        callback: Callable[[], None],
        run_immediately: bool = False,
        run_immediately_synchronously: bool = False,
    ):
        threading.Thread.__init__(self)
        self.seconds_to_wait = seconds_to_wait
        self.callback = callback
        self.run_immediately = run_immediately

        if run_immediately_synchronously:
            self.callback()

        self.daemon = True
        self.stop_event = threading.Event()

    def run(self) -> None:
        if self.run_immediately:
            self.callback()
        while True:
            # Will be true if we didn't wait for the full timeout (e.g stop_timer() is called)
            is_set = self.stop_event.wait(timeout=self.seconds_to_wait)
            if is_set:
                break
            self.callback()

    def stop_timer(self) -> None:
        self.stop_event.set()
