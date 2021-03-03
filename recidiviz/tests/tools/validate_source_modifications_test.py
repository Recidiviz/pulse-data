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
import os
import unittest
from typing import FrozenSet, List, Tuple

from recidiviz.ingest.models import ingest_info, ingest_info_pb2
from recidiviz.persistence.database.migrations.state import versions as state_versions
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.tools.validate_source_modifications import (
    check_assertions,
    INGEST_KEY,
    PIPFILE_KEY,
    STATE_KEY,
    INGEST_DOCS_KEY,
)


class CheckAssertionsTest(unittest.TestCase):
    """Tests for the check_assertions function."""

    def test_ingest_info_happy(self) -> None:
        modified_files = [
            os.path.relpath(ingest_info.__file__),
            os.path.relpath(ingest_info.__file__)[:-2] + "proto",
            os.path.relpath(ingest_info_pb2.__file__),
            os.path.relpath(ingest_info_pb2.__file__) + "i",
        ]

        self._run_test(modified_files, [], [])

    def test_ingest_info_unhappy(self) -> None:
        modified_files = [
            os.path.relpath(ingest_info.__file__),
            os.path.relpath(ingest_info.__file__)[:-2] + "proto",
        ]
        expected_failures: List[Tuple[FrozenSet[str], FrozenSet[str]]] = [
            (
                frozenset(
                    (
                        os.path.relpath(ingest_info.__file__),
                        os.path.relpath(ingest_info.__file__)[:-2] + "proto",
                    )
                ),
                frozenset(
                    (
                        os.path.relpath(ingest_info_pb2.__file__),
                        os.path.relpath(ingest_info_pb2.__file__) + "i",
                    )
                ),
            )
        ]

        self._run_test(modified_files, expected_failures, [])

    def test_ingest_info_skipped(self) -> None:
        modified_files = [
            os.path.relpath(ingest_info.__file__),
            os.path.relpath(ingest_info.__file__)[:-2] + "proto",
        ]

        self._run_test(modified_files, [], [INGEST_KEY])

    def test_pipfile_happy(self) -> None:
        modified_files = ["Pipfile", "Pipfile.lock"]

        self._run_test(modified_files, [], [])

    def test_pipfile_unhappy(self) -> None:
        modified_files = ["Pipfile"]
        expected_failures: List[Tuple[FrozenSet[str], FrozenSet[str]]] = [
            (
                frozenset(["Pipfile"]),
                frozenset(["Pipfile.lock"]),
            )
        ]

        self._run_test(modified_files, expected_failures, [])

    def test_pipfile_skipped(self) -> None:
        modified_files = ["Pipfile"]

        self._run_test(modified_files, [], [PIPFILE_KEY])

    def test_state_schema_happy(self) -> None:
        modified_files = [
            os.path.relpath(state_schema.__file__),
            os.path.relpath(state_versions.__file__[: -len("__init__.py")]),
        ]

        self._run_test(modified_files, [], [])

    def test_state_schema_unhappy(self) -> None:
        modified_files = [os.path.relpath(state_schema.__file__)]
        expected_failures: List[Tuple[FrozenSet[str], FrozenSet[str]]] = [
            (
                frozenset([os.path.relpath(state_schema.__file__)]),
                frozenset(
                    [os.path.relpath(state_versions.__file__[: -len("__init__.py")])]
                ),
            )
        ]

        self._run_test(modified_files, expected_failures, [])

    def test_state_schema_skipped(self) -> None:
        modified_files = [os.path.relpath(state_schema.__file__)]

        self._run_test(modified_files, [], [STATE_KEY])

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
        expected_failures: List[Tuple[FrozenSet[str], FrozenSet[str]]] = [
            (
                frozenset(["recidiviz/ingest/direct/regions/us_nd/"]),
                frozenset(["docs/ingest/us_nd/"]),
            )
        ]

        self._run_test(modified_files, expected_failures, [])

    def test_ingest_docs_unhappy_multiple_regions(self) -> None:
        modified_files = [
            "recidiviz/ingest/direct/regions/us_nd/raw_data/whatever.yaml",
            "docs/ingest/us_nd/raw_data.md",
            "recidiviz/ingest/direct/regions/us_mo/raw_data/whatever.yaml",
        ]
        expected_failures: List[Tuple[FrozenSet[str], FrozenSet[str]]] = [
            (
                frozenset(["recidiviz/ingest/direct/regions/us_mo/"]),
                frozenset(["docs/ingest/us_mo/"]),
            )
        ]

        self._run_test(modified_files, expected_failures, [])

    def test_ingest_docs_skipped(self) -> None:
        modified_files = ["../../ingest/direct/regions/us_nd/raw_data/whatever.yaml"]

        self._run_test(modified_files, [], [INGEST_DOCS_KEY])

    def _run_test(
        self,
        modified_files: List[str],
        expected_failures: List[Tuple[FrozenSet[str], FrozenSet[str]]],
        skipped_assertion_keys: List[str],
    ) -> None:
        failed_assertions = check_assertions(
            frozenset(modified_files), frozenset(skipped_assertion_keys)
        )
        self.assertEqual(expected_failures, failed_assertions)
