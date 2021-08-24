# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for RawDataConfigWriter."""
import os
import tempfile
import unittest

from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tests.ingest.direct.direct_ingest_util import PLACEHOLDER_TO_DO_STRING
from recidiviz.tools.ingest.development.raw_data_config_writer import (
    RawDataConfigWriter,
)


class RawDataConfigWriterTest(unittest.TestCase):
    """Tests for DirectIngestDocumentationGenerator."""

    def setUp(self) -> None:
        self.maxDiff = None

    def test_output_to_file(
        self,
    ) -> None:

        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )

        with tempfile.TemporaryDirectory() as tmpdirname:
            for file_tag, config in region_config.raw_file_configs.items():
                test_output_path = os.path.join(tmpdirname, f"{file_tag}.yaml")
                config_writer = RawDataConfigWriter()
                config_writer.output_to_file(
                    default_encoding="UTF-8",
                    default_separator=",",
                    default_ignore_quotes=False,
                    output_path=test_output_path,
                    raw_file_config=config,
                )
                with open(test_output_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                with open(test_output_path, "w", encoding="utf-8") as f:
                    for line in lines:
                        if PLACEHOLDER_TO_DO_STRING not in line:
                            f.write(line)

                with open(config.file_path, "r", encoding="utf-8") as f:
                    expected_contents = f.read()

                with open(test_output_path, "r", encoding="utf-8") as f:
                    written_contents = f.read()
                self.assertEqual(expected_contents, written_contents)
