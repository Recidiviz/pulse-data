# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Parses emulator logs and prints stats"""
import re

import attr


@attr.s(auto_attribs=True, frozen=True)
class RequestEntry:
    """Data object for requests"""

    method: str
    path: str
    duration_ms: float


class BigQueryEmulatorLogParser:
    """Parses emulator logs and prints stats"""

    duration_pattern = re.compile(r"([A-Z]+) (/[^\s]+) took ([\d.]+)(ms|s|µs|µ)")

    def __init__(self) -> None:
        self.requests: list[RequestEntry] = []

    def parse_line(self, line: str) -> None:
        match = self.duration_pattern.search(line)
        if match:
            method, path, value, unit = match.groups()
            duration_ms = self._normalize_duration(float(value), unit)
            self.requests.append(
                RequestEntry(method=method, path=path, duration_ms=duration_ms)
            )

    def _normalize_duration(self, value: float, unit: str) -> float:
        if unit == "s":
            return value * 1000
        if unit in ["µs", "µ"]:
            return value / 1000
        return value  # ms

    def parse_logs(self, log_text: str) -> None:
        for line in log_text.strip().splitlines():
            self.parse_line(line)

    def get_total_time(self) -> float:
        return sum(request.duration_ms for request in self.requests)

    def get_top_n_requests(self, n: int = 10) -> list[RequestEntry]:
        return sorted(self.requests, key=lambda x: x.duration_ms, reverse=True)[:n]

    def print_stats(self, n: int = 10) -> None:
        top_requests = self.get_top_n_requests(n)

        print(f"Total time spent in emulator: {self.get_total_time():.3f}ms")
        print(f"\nTop {n} most expensive requests in emulator:")
        for i, req in enumerate(top_requests, 1):
            print(f"{i:>2}. {req.method} {req.path} took {req.duration_ms:.3f}ms")
