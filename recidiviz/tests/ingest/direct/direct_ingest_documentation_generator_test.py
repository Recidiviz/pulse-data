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
"""Tests for DirectIngestDocumentationGenerator."""

import unittest

import pytest
from mock import patch, MagicMock

from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import DirectIngestRegionRawFileConfig
from recidiviz.ingest.direct.direct_ingest_documentation_generator import DirectIngestDocumentationGenerator
from recidiviz.tests.ingest import fixtures


class DirectIngestDocumentationGeneratorTest(unittest.TestCase):
    """Tests for DirectIngestDocumentationGenerator."""

    @patch('recidiviz.ingest.direct.direct_ingest_documentation_generator.DirectIngestRegionRawFileConfig')
    def test_generate_raw_file_docs_for_region(self, mock_raw_config: MagicMock) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code='us_doc',
            yaml_config_file_dir=fixtures.as_filepath('us_doc'),
        )
        mock_raw_config.return_value = region_config

        documentation_generator = DirectIngestDocumentationGenerator()
        documentation = documentation_generator.generate_raw_file_docs_for_region('us_doc')

        expected_documentation = """## multiLineDescription

First raw file.

|       Column        |                                                                      Column Description                                                                       | Part of Primary Key? |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------|
| col_name_1a         | First column.                                                                                                                                                 | YES                  |
| col_name_1b         | A column description that is long enough to take up multiple lines. This text block will be interpreted literally and trailing/leading whitespace is removed. | YES                  |
| undocumented_column |                                                                                                                                                               |                      |


## tagColumnsMissing

tagColumnsMissing file description

| Column | Column Description | Part of Primary Key? |
|--------|--------------------|----------------------|


## tagPrimaryKeyColsMissing

tagPrimaryKeyColsMissing file description

|  Column  |  Column Description  | Part of Primary Key? |
|----------|----------------------|----------------------|
| column_1 | column_1 description |                      |
"""

        self.assertIsNotNone(documentation)
        self.assertEqual(expected_documentation, documentation)

    def test_generate_raw_file_docs_for_region_region_not_found(self) -> None:
        documentation_generator = DirectIngestDocumentationGenerator()

        with pytest.raises(ValueError) as error:
            documentation_generator.generate_raw_file_docs_for_region('US_NOT_REAL')
            self.assertEqual(error.value, "Missing raw data configs for region: US_NOT_REAL")
