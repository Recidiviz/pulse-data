# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Custom configuration for how pytest should run."""
import os
from typing import List

import pytest
from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.mark import deselect_by_mark

import recidiviz

# Register fixtures
pytest_plugins = ("recidiviz.tests.fixtures",)


def pytest_configure(config: Config) -> None:
    recidiviz.called_from_test = True
    config.addinivalue_line(
        "markers", "uses_db: for tests that spin up a new database."
    )


def pytest_unconfigure() -> None:
    del recidiviz.called_from_test


def pytest_addoption(parser: Parser) -> None:
    parser.addoption("--test-set", type=str, choices=["parallel", "not-parallel"])

    parser.addoption(
        "-E",
        "--with-emulator",
        action="store_true",
        help="run tests that require the datastore emulator.",
    )

    group = parser.getgroup("split your tests into evenly sized suites")
    group.addoption(
        "--suite-count",
        dest="suite_count",
        type=int,
        help="The total number of suites to split",
    )
    group.addoption(
        "--suite",
        dest="suite",
        type=int,
        help="The suite to be executed",
    )


def pytest_runtest_setup(item: pytest.Item) -> None:
    test_set = item.config.getoption("test_set", default=None)

    if item.config.getoption("with_emulator", default=None):
        if "emulator" not in item.fixturenames:
            pytest.skip("requires datastore emulator")
    else:
        if "emulator" in item.fixturenames:
            pytest.skip("requires datastore emulator")

    if item.get_closest_marker("uses_db") is not None:
        if test_set == "parallel":
            pytest.skip("[parallel tests] skipping because test requires database")
    else:
        if test_set == "not-parallel":
            pytest.skip(
                "[not-parallel tests] skipping because test does not require database or emulator"
            )


def get_suite(
    items: List[pytest.Item], suite_count: int, suite_id: int
) -> List[pytest.Item]:
    """Get the items from the passed in suite based on suite count."""
    if not 1 <= suite_id <= suite_count:
        raise ValueError(
            f"Invalid suite id (suite_count={suite_count}, suite_id={suite_id})"
        )

    start = suite_id - 1
    return items[start : len(items) : suite_count]


def pytest_collection_modifyitems(config: Config, items: List[pytest.Item]) -> None:
    """Hook for selecting which tests should be executed
    Selects the tests for specified suite if using the `--suite` and `--suite-count` options
    """
    suite_count = config.getoption("suite_count")
    suite_id = config.getoption("suite")

    if not suite_count or not suite_id:
        return

    deselect_by_mark(items, config)

    items[:] = get_suite(items, suite_count, suite_id)


def get_worker_id() -> int:
    """Retrieves the worker number from the appropriate environment variable
    https://github.com/pytest-dev/pytest-xdist#identifying-the-worker-process-during-a-test
    """
    return int(os.environ.get("PYTEST_XDIST_WORKER", "gw0")[2:])
