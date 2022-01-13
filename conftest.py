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

import pytest
from mock import patch

import recidiviz

# Register fixtures
pytest_plugins = ("recidiviz.tests.fixtures",)


def pytest_configure(config) -> None:
    recidiviz.called_from_test = True
    config.addinivalue_line(
        "markers", "uses_db: for tests that spin up a new database."
    )


def pytest_unconfigure() -> None:
    del recidiviz.called_from_test


def pytest_addoption(parser) -> None:
    parser.addoption("--test-set", type=str, choices=["parallel", "not-parallel"])

    parser.addoption(
        "-E",
        "--with-emulator",
        action="store_true",
        help="run tests that require the datastore emulator.",
    )


def pytest_runtest_setup(item: pytest.Item) -> None:
    test_set = item.config.getoption("test_set", default=None)

    if item.config.getoption("with_emulator", default=None):
        if "emulator" not in item.fixturenames:
            pytest.skip("requires datastore emulator")
    else:
        # For tests without the emulator, prevent them from trying to create google cloud clients.
        item.google_auth_patcher = patch("google.auth.default")
        mock_google_auth = item.google_auth_patcher.start()
        mock_google_auth.side_effect = AssertionError(
            "Unit test may not instantiate a Google client. Please mock the appropriate client class inside this test "
            " (e.g. `patch('google.cloud.bigquery.Client')`)."
        )

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


def pytest_runtest_teardown(item: pytest.Item) -> None:
    if hasattr(item, "google_auth_patcher") and item.google_auth_patcher is not None:
        item.google_auth_patcher.stop()
