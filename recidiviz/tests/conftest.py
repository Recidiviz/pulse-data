# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
from typing import Any

import pytest
from snapshottest.pytest import PyTestSnapshotTest


# In order to update snapshots, run pytest <test-path> --snapshot-update
# Implemented as per https://github.com/syrusakbary/snapshottest/issues/53
@pytest.fixture(scope="class")
def snapshottest_snapshot(request: Any) -> None:
    """
    Wraps snapshot fixture to provide instance snapshot property for
    unittest.TestCase tests
    """
    request.cls.snapshot = PyTestSnapshotTest(request)
