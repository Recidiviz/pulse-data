#   Recidiviz - a data platform for criminal justice reform
#   Copyright (C) 2021 Recidiviz, Inc.
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <https://www.gnu.org/licenses/>.
#   =============================================================================
#
"""Tests for create_ingest_config_skeleton.py"""
import os
import shutil
import unittest

from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tools.ingest.development.create_ingest_config_skeleton import (
    create_ingest_config_skeleton,
    make_config_directory,
)

INPUT_TABLE = "raw_table_1"

FAKE_STATE = "us_xx"


class CreateIngestConfigSkeletonTest(unittest.TestCase):
    def tearDown(self) -> None:
        shutil.rmtree(os.path.split(make_config_directory(FAKE_STATE))[0])

    def test_create_structure_and_check(self) -> None:
        """Create a raw file config from a test input, try to ingest it, and check the results."""
        state_code = FAKE_STATE
        delimiter = "|"
        allow_overwrite = False
        initialize_state = True

        input_path = os.path.join(
            os.path.dirname(__file__),
            "create_ingest_config_skeleton_test_fixtures",
            INPUT_TABLE + ".txt",
        )

        create_ingest_config_skeleton(
            [input_path], state_code, delimiter, allow_overwrite, initialize_state
        )

        config = DirectIngestRegionRawFileConfig(region_code=state_code)

        self.assertIsNotNone(config)

        table_config = config.raw_file_configs[INPUT_TABLE]
        self.assertEqual(table_config.file_tag, INPUT_TABLE)
        self.assertEqual(
            [field.name for field in table_config.columns],
            ["field1", "field2", "field3"],
        )
