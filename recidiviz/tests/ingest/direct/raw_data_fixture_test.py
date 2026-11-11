# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for RawDataFixture."""
import datetime
import os
import tempfile
import unittest
from unittest.mock import patch

from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tests.ingest.direct.raw_data_fixture import RawDataFixture


class RawDataFixtureTest(unittest.TestCase):
    """Tests for RawDataFixture."""

    def setUp(self) -> None:
        # enterContext closes the directory at cleanup, which pylint doesn't model.
        temp_dir = self.enterContext(
            tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        )
        self.fixture_path = os.path.join(temp_dir, "tagBasicData.csv")
        self.enterContext(
            patch(
                "recidiviz.tests.ingest.direct.raw_data_fixture."
                "fixture_path_for_raw_file_config",
                return_value=self.fixture_path,
            )
        )
        self.fixture = RawDataFixture(
            DirectIngestRegionRawFileConfig(
                region_code="us_xx", region_module=fake_regions_module
            ).raw_file_configs["tagBasicData"]
        )

    def _write_fixture(self, contents: str) -> None:
        with open(self.fixture_path, "w", encoding="utf-8") as f:
            f.write(contents)

    def test_max_update_datetime_across_date_and_datetime_formats(self) -> None:
        self._write_fixture(
            "COL1,COL2,COL3,file_id,update_datetime,is_deleted\n"
            "A100,Main St,Active,1,2025-01-15,False\n"
            "A101,Oak Ave,Inactive,2,2025-02-11 21:00:14.954652,False\n"
            "A102,Elm Rd,Active,3,2024-12-30T08:15:00,False\n"
        )
        self.assertEqual(
            datetime.datetime(2025, 2, 11, 21, 0, 14, 954652),
            self.fixture.max_update_datetime("some_characteristic"),
        )

    def test_max_update_datetime_date_only_value_is_midnight(self) -> None:
        self._write_fixture(
            "COL1,COL2,COL3,file_id,update_datetime,is_deleted\n"
            "A100,Main St,Active,1,2025-01-15,False\n"
        )
        self.assertEqual(
            datetime.datetime(2025, 1, 15, 0, 0, 0),
            self.fixture.max_update_datetime("some_characteristic"),
        )

    def test_max_update_datetime_unpadded_hour(self) -> None:
        self._write_fixture(
            "COL1,COL2,COL3,file_id,update_datetime,is_deleted\n"
            "A100,Main St,Active,1,2024-01-05 0:00:00,False\n"
        )
        self.assertEqual(
            datetime.datetime(2024, 1, 5, 0, 0, 0),
            self.fixture.max_update_datetime("some_characteristic"),
        )

    def test_max_update_datetime_offset_is_converted_to_naive_utc(self) -> None:
        self._write_fixture(
            "COL1,COL2,COL3,file_id,update_datetime,is_deleted\n"
            "A100,Main St,Active,1,2024-12-30T08:15:00-05:00,False\n"
        )
        self.assertEqual(
            datetime.datetime(2024, 12, 30, 13, 15, 0),
            self.fixture.max_update_datetime("some_characteristic"),
        )

    def test_max_update_datetime_empty_fixture_is_none(self) -> None:
        self._write_fixture("COL1,COL2,COL3,file_id,update_datetime,is_deleted\n")
        self.assertIsNone(self.fixture.max_update_datetime("some_characteristic"))
