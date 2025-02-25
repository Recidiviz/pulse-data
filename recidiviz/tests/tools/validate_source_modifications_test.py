# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

"""Tests the validate_source_modifications script."""
import unittest
from typing import FrozenSet, List, Tuple

from recidiviz.tools.validate_source_modifications import (
    BUILD_INFRA_KEY,
    INGEST_DOCS_KEY,
    PIPFILE_KEY,
    check_assertions,
)


class CheckAssertionsTest(unittest.TestCase):
    """Tests for the check_assertions function."""

    def test_copy_bara_happy(self) -> None:
        modified_files = [
            "mirror/copy.bara.sky",
            "Dockerfile.case-triage-pathways.dockerignore",
            "Dockerfile.justice-counts.dockerignore",
        ]
        self._run_test(modified_files, [], [])

    def test_copy_bara_unhappy(self) -> None:
        modified_files = [
            "mirror/copy.bara.sky",
        ]
        expected_failures: List[Tuple[FrozenSet[str], FrozenSet[str], str]] = [
            (
                frozenset({"mirror/copy.bara.sky"}),
                frozenset(
                    {
                        "Dockerfile.case-triage-pathways.dockerignore",
                        "Dockerfile.justice-counts.dockerignore",
                    }
                ),
                BUILD_INFRA_KEY,
            )
        ]

        self._run_test(modified_files, expected_failures, [])

    def test_copy_bara_skipped(self) -> None:
        modified_files = ["mirror/copy.bara.sky"]

        self._run_test(modified_files, [], [BUILD_INFRA_KEY])

    def test_pipfile_happy(self) -> None:
        modified_files = ["Pipfile", "Pipfile.lock"]

        self._run_test(modified_files, [], [])

    def test_pipfile_unhappy(self) -> None:
        modified_files = ["Pipfile"]
        expected_failures: List[Tuple[FrozenSet[str], FrozenSet[str], str]] = [
            (frozenset(["Pipfile"]), frozenset(["Pipfile.lock"]), "pipfile")
        ]

        self._run_test(modified_files, expected_failures, [])

    def test_pipfile_skipped(self) -> None:
        modified_files = ["Pipfile"]

        self._run_test(modified_files, [], [PIPFILE_KEY])

    def test_ingest_docs_happy(self) -> None:
        modified_files = [
            "recidiviz/ingest/direct/regions/us_nd/raw_data/whatever.yaml",
            "docs/ingest/us_nd/raw_data.md",
        ]

        self._run_test(modified_files, [], [])

    def test_ingest_docs_happy_multiple_regions(self) -> None:
        modified_files = [
            "recidiviz/ingest/direct/regions/us_nd/raw_data/whatever.yaml",
            "docs/ingest/us_nd/raw_data.md",
            "recidiviz/ingest/direct/regions/us_mo/us_mo_controller.py",
            "docs/ingest/us_mo/us_mo.md",
        ]

        self._run_test(modified_files, [], [])

    def test_ingest_docs_unhappy(self) -> None:
        modified_files = [
            "recidiviz/ingest/direct/regions/us_nd/raw_data/whatever.yaml",
            "recidiviz/ingest/direct/regions/us_nd/raw_data/something_else.yaml",
        ]
        expected_failures: List[Tuple[FrozenSet[str], FrozenSet[str], str]] = [
            (
                frozenset(["recidiviz/ingest/direct/regions/us_nd/raw_data/"]),
                frozenset(["docs/ingest/us_nd/"]),
                "ingest_docs",
            )
        ]

        self._run_test(modified_files, expected_failures, [])

    def test_ingest_docs_unhappy_multiple_regions(self) -> None:
        modified_files = [
            "recidiviz/ingest/direct/regions/us_nd/raw_data/whatever.yaml",
            "docs/ingest/us_nd/raw_data.md",
            "recidiviz/ingest/direct/regions/us_mo/raw_data/whatever.yaml",
        ]
        expected_failures: List[Tuple[FrozenSet[str], FrozenSet[str], str]] = [
            (
                frozenset(["recidiviz/ingest/direct/regions/us_mo/raw_data/"]),
                frozenset(["docs/ingest/us_mo/"]),
                "ingest_docs",
            )
        ]

        self._run_test(modified_files, expected_failures, [])

    def test_ingest_docs_skipped(self) -> None:
        modified_files = ["../../ingest/direct/regions/us_nd/raw_data/whatever.yaml"]

        self._run_test(modified_files, [], [INGEST_DOCS_KEY])

    def test_endpoint_docs_happy(self) -> None:
        modified_files = [
            "recidiviz/ingest/direct/direct_ingest_control.py",
            "docs/endpoints/direct/handle_new_files.md",
        ]
        self._run_test(modified_files, [], [])

    def test_endpoint_docs_unhappy(self) -> None:
        modified_files = [
            "recidiviz/ingest/direct/direct_ingest_control.py",
        ]
        expected_failures: List[Tuple[FrozenSet[str], FrozenSet[str], str]] = [
            (
                frozenset(["recidiviz/ingest/direct/direct_ingest_control.py"]),
                frozenset(["docs/endpoints/direct"]),
                "endpoints_docs",
            ),
        ]
        self._run_test(modified_files, expected_failures, [])

    # TODO(#8217) excluding the admin panel from endpoint documentation,
    #  write new test to test the admin panel routes

    def _run_test(
        self,
        modified_files: List[str],
        expected_failures: List[Tuple[FrozenSet[str], FrozenSet[str], str]],
        skipped_assertion_keys: List[str],
    ) -> None:
        failed_assertions = check_assertions(
            frozenset(modified_files), frozenset(skipped_assertion_keys)
        )
        self.assertEqual(expected_failures, failed_assertions)
