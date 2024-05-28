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
from typing import List

from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.mark import deselect_by_mark
from _pytest.nodes import Item

import recidiviz

# Register fixtures
pytest_plugins = ("recidiviz.tests.fixtures",)


def pytest_configure(config: Config) -> None:
    recidiviz.called_from_test = True
    config.addinivalue_line(
        "markers", "uses_db: for tests that spin up a new database."
    )
    config.addinivalue_line(
        "markers", "uses_bq_emulator: for tests that use the BigQuery emulator"
    )
    config.addinivalue_line(
        "markers", "view_graph_validation: special marker for long-running test"
    )


def pytest_unconfigure() -> None:
    del recidiviz.called_from_test


def pytest_addoption(parser: Parser) -> None:
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


def get_suite(items: List[Item], suite_count: int, suite_id: int) -> List[Item]:
    """Get the items from the passed in suite based on suite count."""
    if not 1 <= suite_id <= suite_count:
        raise ValueError(
            f"Invalid suite id (suite_count={suite_count}, suite_id={suite_id})"
        )

    start = suite_id - 1
    return items[start : len(items) : suite_count]


def pytest_collection_modifyitems(config: Config, items: List[Item]) -> None:
    """Hook for selecting which tests should be executed
    Selects the tests for specified suite if using the `--suite` and `--suite-count` options
    """
    suite_count = config.getoption("suite_count")
    suite_id = config.getoption("suite")

    if not suite_count or not suite_id:
        return

    deselect_by_mark(items, config)

    items[:] = get_suite(items, suite_count, suite_id)
