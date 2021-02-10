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
import importlib
import unittest

import pytest
from mock import patch, MagicMock

from recidiviz.common.constants import states
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import DirectIngestRegionRawFileConfig
from recidiviz.ingest.direct.direct_ingest_documentation_generator import DirectIngestDocumentationGenerator
from recidiviz.tests.ingest import fixtures


class DirectIngestDocumentationGeneratorTest(unittest.TestCase):
    """Tests for DirectIngestDocumentationGenerator."""

    @patch('recidiviz.ingest.direct.direct_ingest_documentation_generator.DirectIngestRegionRawFileConfig')
    def test_generate_raw_file_docs_for_region(self, mock_raw_config: MagicMock) -> None:
        importlib.reload(states)
        region_code = states.StateCode.US_XX.value.lower()

        region_config = DirectIngestRegionRawFileConfig(
            region_code=region_code,
            yaml_config_file_dir=fixtures.as_filepath(region_code),
        )
        mock_raw_config.return_value = region_config

        documentation_generator = DirectIngestDocumentationGenerator()
        documentation = documentation_generator.generate_raw_file_docs_for_region(region_code)

        expected_documentation = """
# Test State Raw Data Description

All raw data can be found in append-only tables in the dataset `us_xx_raw_data`. Views on the raw data
table that show the latest state of this table (i.e. select the most recently received row for each primary key) can be
found in `us_xx_raw_data_up_to_date_views`.

## Table of Contents

The statuses below are defined as:
- RECEIVED: the file has been sent to Recidiviz at least once but is not kept up-to-date in any way as of yet
- IMPORTED TO BQ: the file is kept up-to-date in its raw form in our data warehouse without any formal ingest processing
- ON DECK: the Recidiviz team is actively planning to ingest the file in the near future but has not done so yet
- INGESTED: the file is being actively ingested with regular data transfers from the source system

|                       **Table**                       | **Status** | **Last Updated** | **Updated By** |
|-------------------------------------------------------|------------|------------------|----------------|
| [multiLineDescription](#multiLineDescription)         |            |                  |                |
| [tagColumnsMissing](#tagColumnsMissing)               |            |                  |                |
| [tagPrimaryKeyColsMissing](#tagPrimaryKeyColsMissing) |            |                  |                |

## multiLineDescription

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
