# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Provides the base setup for unit testing any raw data LookML generation"""
import abc
import difflib
import filecmp
import os
import tempfile
import unittest
from types import ModuleType
from typing import Any, Callable, List
from unittest.mock import patch

from recidiviz.common.constants.encoding import UTF_8
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tests.ingest.direct import fake_regions


class RawDataPersonDetailsLookMLGeneratorTest(unittest.TestCase):
    """
    Base class for helping to test raw data LookML generation.
    """

    @classmethod
    @abc.abstractmethod
    def generator_modules(cls) -> List[ModuleType]:
        """Return a list of modules to patch out"""

    def setUp(self) -> None:
        # Only generate LookML files for US_LL
        self.get_states_patchers = [
            patch(
                f"{module.__name__}.get_existing_direct_ingest_states",
                return_value=[StateCode.US_LL],
            )
            for module in self.generator_modules()
            if "get_existing_direct_ingest_states" in dir(module)
        ]

        # Use fake regions for states
        def mock_config_constructor(
            *, region_code: str
        ) -> DirectIngestRegionRawFileConfig:
            return DirectIngestRegionRawFileConfig(
                region_code=region_code,
                region_module=fake_regions,
            )

        self.region_config_patchers = [
            patch(
                f"{module.__name__}.DirectIngestRegionRawFileConfig",
                side_effect=mock_config_constructor,
            )
            for module in self.generator_modules()
            if "DirectIngestRegionRawFileConfig" in dir(module)
        ]

        for patcher in self.get_states_patchers + self.region_config_patchers:
            patcher.start()

    def tearDown(self) -> None:
        for patcher in self.get_states_patchers + self.region_config_patchers:
            patcher.stop()

    def generate_files(
        self,
        *,
        function_to_test: Callable[[str], Any],
        filename_filter: str,
    ) -> None:
        """
        Calls the provided function on a temporary directory and compares the
        result with the fixtures directory, filtering by names ending with
        filename_filter.

        The mocks should be patched in for each test case using this base class.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            function_to_test(tmp_dir)
            fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")

            for fixtures_path, _, filenames in os.walk(fixtures_dir):
                # Get the fixtures inner directory corresponding to the temp inner directory
                relpath = os.path.relpath(fixtures_path, start=fixtures_dir)
                tmp_path = os.path.join(tmp_dir, relpath)

                # Ensure every .lkml file in the fixture directory is equal
                # byte-by-byte to the one in the temp directory
                lkml_filenames = filter(
                    lambda name: name.endswith(filename_filter), filenames
                )
                _, mismatch, errors = filecmp.cmpfiles(
                    tmp_path, fixtures_path, lkml_filenames, shallow=False
                )
                for filename in mismatch:
                    expected_file = os.path.join(fixtures_path, filename)
                    actual_file = os.path.join(tmp_path, filename)

                    with open(expected_file, "r", encoding=UTF_8) as f1, open(
                        actual_file, "r", encoding=UTF_8
                    ) as f2:
                        expected_lines = f1.readlines()
                        actual_lines = f2.readlines()

                    diff = difflib.unified_diff(
                        expected_lines,
                        actual_lines,
                        fromfile=f"expected/{filename}",
                        tofile=f"actual/{filename}",
                    )
                    diff_output = "".join(diff)
                    self.fail(
                        "File mismatch - you may need to update the following file in the fixtures/ directory: "
                        f"{filename}\n{diff_output}"
                    )

                if errors:
                    self.fail(f"Error comparing files: {errors}")
