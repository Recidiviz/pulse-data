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
import tempfile
import types
import unittest
from unittest.mock import patch

from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
)
from recidiviz.tools.docs.utils import PLACEHOLDER_TO_DO_STRING
from recidiviz.tools.ingest.development.create_ingest_config_skeleton import (
    create_ingest_config_skeleton,
)

INPUT_TABLE = "raw_table_1"
HIDDEN_INPUT_TABLE = ".raw_table_1"
FIXTURE_DIR_NAME = "create_ingest_config_skeleton_test_fixtures"

FAKE_STATE = "us_xx"


class CreateIngestConfigSkeletonTest(unittest.TestCase):
    """Tests for create_ingest_config_skeleton.py"""

    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp()
        self.input_path = os.path.join(
            os.path.dirname(__file__),
            FIXTURE_DIR_NAME,
            INPUT_TABLE + ".txt",
        )
        self.state_code = FAKE_STATE
        self.delimiter = "|"
        self.encoding = "utf-8"
        self.allow_overwrite = False
        self.initialize_state = True
        self.add_description_placeholders = False
        self.custom_line_terminator = None

        # Create the mock config directory structure
        self.mock_config_dir = os.path.join(self.test_dir, FAKE_STATE, "raw_data")
        os.makedirs(self.mock_config_dir, exist_ok=True)

        self.get_config_dir_patcher = patch(
            "recidiviz.tools.ingest.development.create_ingest_config_skeleton.get_config_directory",
            side_effect=self._mock_get_config_directory,
        )
        self.get_config_dir_patcher.start()

        self.mock_region_module = types.ModuleType("mock_regions")
        self.mock_region_module.__file__ = os.path.join(self.test_dir, "__init__.py")

    def tearDown(self) -> None:
        self.get_config_dir_patcher.stop()
        shutil.rmtree(self.test_dir)

    def _mock_get_config_directory(self, state_code: str) -> str:
        """Mock function to return test directory instead of real config directory."""
        return os.path.join(self.test_dir, state_code, "raw_data")

    def test_create_structure_custom_line_terminator(self) -> None:
        file_tag = "raw_table_2"
        create_ingest_config_skeleton(
            raw_table_paths=[
                os.path.join(
                    os.path.dirname(__file__),
                    FIXTURE_DIR_NAME,
                    f"{file_tag}.txt",
                )
            ],
            state_code=self.state_code,
            delimiter="‡",
            encoding=self.encoding,
            data_classification=RawDataClassification.SOURCE,
            allow_overwrite=self.allow_overwrite,
            initialize_state=self.initialize_state,
            add_description_placeholders=self.add_description_placeholders,
            custom_line_terminator="†",
        )

        config = DirectIngestRegionRawFileConfig(
            region_code=self.state_code, region_module=self.mock_region_module
        )

        self.assertIsNotNone(config)

        table_config = config.raw_file_configs[file_tag]
        self.assertEqual(table_config.file_tag, file_tag)
        self.assertEqual(table_config.data_classification, RawDataClassification.SOURCE)
        self.assertEqual(
            [field.name for field in table_config.current_columns],
            ["field1", "field2", "field3", "field_4"],
        )
        self.assertEqual(
            [field.description for field in table_config.current_columns],
            [None, None, None, None],
        )

    def test_create_structure_and_check(self) -> None:
        """Create a raw file config from a test input, try to ingest it, and check the results."""
        create_ingest_config_skeleton(
            [self.input_path],
            self.state_code,
            self.delimiter,
            self.encoding,
            RawDataClassification.SOURCE,
            self.allow_overwrite,
            self.initialize_state,
            self.add_description_placeholders,
            self.custom_line_terminator,
        )

        config = DirectIngestRegionRawFileConfig(
            region_code=self.state_code, region_module=self.mock_region_module
        )

        self.assertIsNotNone(config)

        table_config = config.raw_file_configs[INPUT_TABLE]
        self.assertEqual(table_config.file_tag, INPUT_TABLE)
        self.assertEqual(table_config.data_classification, RawDataClassification.SOURCE)
        self.assertEqual(
            [field.name for field in table_config.current_columns],
            ["field1", "field2", "field3", "field_4"],
        )
        self.assertEqual(
            [field.description for field in table_config.current_columns],
            [None, None, None, None],
        )

    def test_create_ingest_config_skeleton_description_placeholders(self) -> None:
        """Create a raw file config with description placeholders"""
        create_ingest_config_skeleton(
            [self.input_path],
            self.state_code,
            self.delimiter,
            self.encoding,
            RawDataClassification.SOURCE,
            self.allow_overwrite,
            self.initialize_state,
            add_description_placeholders=True,
            custom_line_terminator=self.custom_line_terminator,
        )

        config = DirectIngestRegionRawFileConfig(
            region_code=self.state_code, region_module=self.mock_region_module
        )
        table_config = config.raw_file_configs[INPUT_TABLE]

        self.assertIsNotNone(config)

        self.assertEqual(
            [field.description for field in table_config.current_columns],
            [
                PLACEHOLDER_TO_DO_STRING,
                PLACEHOLDER_TO_DO_STRING,
                PLACEHOLDER_TO_DO_STRING,
                PLACEHOLDER_TO_DO_STRING,
            ],
        )

    def test_create_ingest_config_skeleton_hidden_file(self) -> None:
        """Skips creating ingest config for a hidden file"""
        hidden_input_path = os.path.join(
            os.path.dirname(__file__),
            "create_ingest_config_skeleton_test_fixtures",
            HIDDEN_INPUT_TABLE + ".txt",
        )
        create_ingest_config_skeleton(
            [hidden_input_path],
            self.state_code,
            self.delimiter,
            self.encoding,
            RawDataClassification.SOURCE,
            self.allow_overwrite,
            self.initialize_state,
            add_description_placeholders=True,
            custom_line_terminator=self.custom_line_terminator,
        )

        config = DirectIngestRegionRawFileConfig(
            region_code=self.state_code, region_module=self.mock_region_module
        )
        self.assertEqual(config.raw_file_configs, {})
