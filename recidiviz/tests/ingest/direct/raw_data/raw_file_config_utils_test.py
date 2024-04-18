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
"""Tests for the raw file config utils file."""
import unittest

from recidiviz.ingest.direct.raw_data.raw_file_config_utils import (
    raw_file_tags_referenced_downstream,
)
from recidiviz.tests.ingest.direct import fake_regions


class TestRawFileConfigUtils(unittest.TestCase):
    """Tests for the raw file config utils file."""

    def setUp(self) -> None:
        self.region_module = fake_regions

    def test_used_downstream_invalid_region(self) -> None:
        with self.assertRaises(FileNotFoundError):
            raw_file_tags_referenced_downstream("us_not_a_region")

    def test_used_downstream_no_ingest_views(self) -> None:
        assert raw_file_tags_referenced_downstream("us_ww", self.region_module) == set()

    def test_used_downstream_for_empty_config_one(self) -> None:
        assert raw_file_tags_referenced_downstream("us_xx", self.region_module) == {
            "tagBasicData",
            "tagMoreBasicData",
        }
