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
"""Unit tests for remove_bad_bytes"""
from io import BytesIO
from unittest import TestCase

from recidiviz.tools.ingest.one_offs.identify_and_purge_bad_bytes import (
    remove_bad_bytes,
)


class TestRemoveBadBytes(TestCase):
    """Unit tests for remove_bad_bytes"""

    @staticmethod
    def _run_test(
        *,
        input_bytes: bytes,
        expected_output: bytes | None,
        encoding: str,
    ) -> None:
        input_io = BytesIO(input_bytes)
        output_io = BytesIO()
        remove_bad_bytes(input_io=input_io, output_io=output_io, encoding=encoding)

        assert expected_output == output_io.getvalue()

    def test_empty(self) -> None:
        self._run_test(input_bytes=b"", expected_output=b"", encoding="windows-1252")

    def test_no_problem(self) -> None:
        self._run_test(
            input_bytes=b"abc123def456",
            expected_output=b"abc123def456",
            encoding="windows-1252",
        )

    def test_with_problem(self) -> None:
        self._run_test(
            input_bytes=b"abc123\x9ddef456",
            expected_output=b"abc123def456",
            encoding="windows-1252",
        )

    def test_with_problem_end(self) -> None:
        self._run_test(
            input_bytes=b"abc123def456\x9d",
            expected_output=b"abc123def456",
            encoding="windows-1252",
        )

    def test_with_problem_start(self) -> None:
        self._run_test(
            input_bytes=b"\x9dabc123def456",
            expected_output=b"abc123def456",
            encoding="windows-1252",
        )

    def test_with_problems_everywhere(self) -> None:
        self._run_test(
            input_bytes=b"\x9d\x9d\x9dabc\x9d\x9d123\x9d\x9d\x9d\x9ddef456\x9d\x9d\x9d\x9d\x9d\x9d\x9d\x9d\x9d",
            expected_output=b"abc123def456",
            encoding="windows-1252",
        )
