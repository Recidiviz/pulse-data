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

from mock import MagicMock, patch

from recidiviz.common.constants import states
from recidiviz.common.constants.states import TEST_STATE_CODE_DOCS
from recidiviz.ingest.direct.direct_ingest_documentation_generator import (
    DirectIngestDocumentationGenerator,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fakes.fake_direct_ingest_controller import (
    FakeDirectIngestPreProcessedIngestViewCollector,
    FakeDirectIngestRegionRawFileConfig,
)
from recidiviz.tests.utils.fake_region import fake_region


class DirectIngestDocumentationGeneratorTest(unittest.TestCase):
    """Tests for DirectIngestDocumentationGenerator."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "project-id"
        self.maxDiff = None

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    @patch(
        "recidiviz.ingest.direct.direct_ingest_documentation_generator.DirectIngestRegionRawFileConfig"
    )
    @patch(
        "recidiviz.ingest.direct.direct_ingest_documentation_generator.DirectIngestDocumentationGenerator"
        "._get_updated_by"
    )
    @patch(
        "recidiviz.ingest.direct.direct_ingest_documentation_generator.DirectIngestDocumentationGenerator"
        "._get_last_updated"
    )
    @patch(
        "recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region",
        return_value=fake_region(
            region_code=TEST_STATE_CODE_DOCS, region_module=fake_regions
        ),
    )
    @patch(
        "recidiviz.ingest.direct.direct_ingest_documentation_generator.DirectIngestDocumentationGenerator"
        ".get_referencing_views"
    )
    def test_generate_raw_file_docs_for_region(
        self,
        mock_referencing_views: MagicMock,
        _mock_region: MagicMock,
        mock_last_updated: MagicMock,
        mock_updated_by: MagicMock,
        mock_raw_config: MagicMock,
    ) -> None:
        region_code = states.StateCode.US_WW.value.lower()
        region_config = DirectIngestRegionRawFileConfig(
            region_code=region_code,
            region_module=fake_regions,
        )
        mock_raw_config.return_value = region_config
        mock_updated_by.return_value = "Julia Dressel"
        mock_last_updated.return_value = "2021-02-10"
        mock_referencing_views.return_value = {
            "multiLineDescription": ["view_one", "view_two"],
            "tagColumnsMissing": ["view_one"],
            "tagNotHistorical": [],
            "tagPrimaryKeyColsMissing": [],
        }

        documentation_generator = DirectIngestDocumentationGenerator()
        documentation = documentation_generator.generate_raw_file_docs_for_region(
            region_code
        )

        expected_raw_data = """# Test State Raw Data Description

All raw data can be found in append-only tables in the dataset `us_ww_raw_data`. Views on the raw data
table that show the latest state of this table (i.e. select the most recently received row for each primary key) can be
found in `us_ww_raw_data_up_to_date_views`.

## Table of Contents

|                           **Table**                            | **Referencing Views** |**Last Updated**|**Updated By**|
|----------------------------------------------------------------|-----------------------|----------------|--------------|
|[multiLineDescription](raw_data/multiLineDescription.md)        |view_one,<br />view_two|2021-02-10      |Julia Dressel |
|[tagColumnsMissing](raw_data/tagColumnsMissing.md)              |view_one               |2021-02-10      |Julia Dressel |
|[tagNotHistorical](raw_data/tagNotHistorical.md)                |                       |2021-02-10      |Julia Dressel |
|[tagPrimaryKeyColsMissing](raw_data/tagPrimaryKeyColsMissing.md)|                       |2021-02-10      |Julia Dressel |
"""

        expected_multi_line = """## multiLineDescription

First raw file.

|      Column       |                                                                     Column Description                                                                      |Part of Primary Key?|                               Distinct Values                                |Is PII?|
|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|------------------------------------------------------------------------------|-------|
|col_name_1a        |First column.                                                                                                                                                |YES                 |<b>VAL1: </b> value 1, <br/><b>VAL2: </b> value 2, <br/><b>UNKWN: </b> Unknown|False  |
|col_name_1b        |A column description that is long enough to take up multiple lines. This text block will be interpreted literally and trailing/leading whitespace is removed.|YES                 |N/A                                                                           |False  |
|undocumented_column|<No documentation>                                                                                                                                           |                    |N/A                                                                           |False  |
"""

        expected_tag_columns_missing = """## tagColumnsMissing

tagColumnsMissing file description

|Column|Column Description|Part of Primary Key?|Distinct Values|Is PII?|
|------|------------------|--------------------|---------------|-------|
"""

        expected_tag_not_historical = """## tagNotHistorical

tagNotHistorical file description

| Column |Column Description|Part of Primary Key?|Distinct Values|Is PII?|
|--------|------------------|--------------------|---------------|-------|
|column_1|<No documentation>|YES                 |N/A            |False  |
"""

        expected_tag_primary_key_cols_missing = """## tagPrimaryKeyColsMissing

tagPrimaryKeyColsMissing file description

| Column | Column Description |Part of Primary Key?|Distinct Values|Is PII?|
|--------|--------------------|--------------------|---------------|-------|
|column_1|column_1 description|                    |N/A            |False  |
"""

        expected_documentation = {
            "multiLineDescription.md": expected_multi_line,
            "raw_data.md": expected_raw_data,
            "tagColumnsMissing.md": expected_tag_columns_missing,
            "tagNotHistorical.md": expected_tag_not_historical,
            "tagPrimaryKeyColsMissing.md": expected_tag_primary_key_cols_missing,
        }

        self.assertIsNotNone(documentation)
        self.assertEqual(expected_documentation, documentation)

    def test_generate_raw_file_docs_for_region_region_not_found(self) -> None:
        documentation_generator = DirectIngestDocumentationGenerator()

        with self.assertRaisesRegex(
            ValueError, "^Missing raw data configs for region: US_NOT_REAL"
        ):
            documentation_generator.generate_raw_file_docs_for_region("US_NOT_REAL")

    @patch(
        "recidiviz.ingest.direct.views.direct_ingest_big_query_view_types"
        ".get_region_raw_file_config"
    )
    def test_get_referencing_views(
        self,
        mock_config_fn: MagicMock,
    ) -> None:
        mock_config_fn.return_value = FakeDirectIngestRegionRawFileConfig("US_XX")
        documentation_generator = DirectIngestDocumentationGenerator()
        tags = ["tagFullyEmptyFile", "tagHeadersNoContents", "tagBasicData"]
        my_collector = FakeDirectIngestPreProcessedIngestViewCollector(
            region=fake_region(), controller_ingest_view_rank_list=tags
        )
        expected_referencing_views = {
            "tagFullyEmptyFile": ["tagFullyEmptyFile"],
            "tagHeadersNoContents": ["tagHeadersNoContents"],
            "tagBasicData": ["tagBasicData", "gatedTagNotInTagsList"],
        }
        self.assertEqual(
            documentation_generator.get_referencing_views(
                my_collector
            ),  # pylint: disable=W0212
            expected_referencing_views,
        )
