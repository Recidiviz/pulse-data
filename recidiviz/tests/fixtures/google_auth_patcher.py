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
""" Fixture for ensuring our tests don't reach out to GCP """

from typing import Generator
from unittest.mock import patch

import pytest
from _pytest.config import Config
from _pytest.fixtures import FixtureRequest


@pytest.fixture(autouse=True)
def google_auth_patcher(
    pytestconfig: Config, request: FixtureRequest  # pylint: disable=unused-argument
) -> Generator[None, None, None]:
    """Prevent tests from trying to create google cloud clients."""

    patcher = patch("google.auth.default")
    mock_google_auth = patcher.start()
    mock_google_auth.side_effect = AssertionError(
        "Unit test may not instantiate a Google client. Please mock the appropriate client class inside this test "
        " (e.g. `patch('google.cloud.bigquery.Client')`)."
    )

    yield

    patcher.stop()
