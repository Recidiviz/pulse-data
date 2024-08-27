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
from collections import defaultdict
from typing import List

from mock import MagicMock, patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants import states
from recidiviz.common.constants.states import TEST_STATE_CODE_DOCS
from recidiviz.ingest.direct.direct_ingest_documentation_generator import (
    DirectIngestDocumentationGenerator,
)
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fakes.fake_ingest_raw_file_import_controller import (
    FakeDirectIngestRegionRawFileConfig,
)
from recidiviz.tests.utils.fake_region import fake_region


class FakeDirectIngestViewQueryBuilderCollector(DirectIngestViewQueryBuilderCollector):
    """A test version of DirectIngestViewQueryBuilderCollector"""

    def __init__(
        self, region: DirectIngestRegion, expected_ingest_views: List[str] | None
    ):
        super().__init__(region, expected_ingest_views)

    def _collect_query_builders(self) -> List[DirectIngestViewQueryBuilder]:
        builders = (
            [
                DirectIngestViewQueryBuilder(
                    region=self.region.region_code,
                    ingest_view_name=tag,
                    view_query_template=(f"SELECT * FROM {{{tag}}}"),
                )
                for tag in self.expected_ingest_views
            ]
            if self.expected_ingest_views
            else []
        )

        builders.append(
            DirectIngestViewQueryBuilder(
                region=self.region.region_code,
                ingest_view_name="gatedTagNotInTagsList",
                view_query_template="SELECT * FROM {tagBasicData} LEFT OUTER JOIN {tagBasicData} USING (col);",
            )
        )

        return builders


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
        "recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region",
        return_value=fake_region(
            region_code=TEST_STATE_CODE_DOCS, region_module=fake_regions
        ),
    )
    @patch(
        "recidiviz.ingest.direct.direct_ingest_documentation_generator.DirectIngestDocumentationGenerator"
        ".get_referencing_views"
    )
    @patch(
        "recidiviz.ingest.direct.direct_ingest_documentation_generator.RawDataReferenceReasonsYamlLoader"
        ".get_downstream_referencing_views"
    )
    def test_generate_raw_file_docs_for_region(
        self,
        mock_downstream_referencing_views: MagicMock,
        mock_referencing_views: MagicMock,
        _mock_region: MagicMock,
        mock_raw_config: MagicMock,
    ) -> None:
        region_code = states.StateCode.US_WW.value.lower()
        region_config = DirectIngestRegionRawFileConfig(
            region_code=region_code,
            region_module=fake_regions,
        )
        mock_raw_config.return_value = region_config
        mock_referencing_views.return_value = {
            "multiLineDescription": ["view_one", "view_two"],
            "tagColumnsMissing": ["view_one"],
            "tagNotHistorical": [],
            "tagPrimaryKeyColsMissing": [],
            "tagExemptFromValidations": [],
        }
        mock_downstream_referencing_views.return_value = defaultdict(
            set,
            {
                "multiLineDescription": {
                    BigQueryAddress.from_str("dataset.view_three"),
                    BigQueryAddress.from_str("dataset.view_four"),
                },
                "tagColumnsMissing": {BigQueryAddress.from_str("dataset.view_four")},
            },
        )

        documentation_generator = DirectIngestDocumentationGenerator()
        documentation = documentation_generator.generate_raw_file_docs_for_region(
            region_code
        )

        expected_raw_data = """# Test State Raw Data Description

All raw data can be found in append-only tables in the dataset `us_ww_raw_data`. Views on the raw data
table that show the latest state of this table (i.e. select the most recently received row for each primary key) can be
found in `us_ww_raw_data_up_to_date_views`.

## Table of Contents

|                           **Table**                            |**Referencing Ingest Views**|     **Referencing Downstream Views**     |
|----------------------------------------------------------------|----------------------------|------------------------------------------|
|[multiLineDescription](raw_data/multiLineDescription.md)        |view_one,<br />view_two     |dataset.view_four,<br />dataset.view_three|
|[tagColumnsMissing](raw_data/tagColumnsMissing.md)              |view_one                    |dataset.view_four                         |
|[tagExemptFromValidations](raw_data/tagExemptFromValidations.md)|                            |                                          |
|[tagNotHistorical](raw_data/tagNotHistorical.md)                |                            |                                          |
|[tagPrimaryKeyColsMissing](raw_data/tagPrimaryKeyColsMissing.md)|                            |                                          |
"""

        expected_multi_line = """## multiLineDescription

First raw file.

|      Column       |                                                                     Column Description                                                                      |Part of Primary Key?|                               Distinct Values                                |Is PII?|
|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|------------------------------------------------------------------------------|-------|
|col_name_1a        |First column.                                                                                                                                                |YES                 |<b>VAL1: </b> value 1, <br/><b>VAL2: </b> value 2, <br/><b>UNKWN: </b> Unknown|False  |
|col_name_1b        |A column description that is long enough to take up multiple lines. This text block will be interpreted literally and trailing/leading whitespace is removed.|YES                 |N/A                                                                           |False  |
|undocumented_column|<No documentation>                                                                                                                                           |                    |N/A                                                                           |False  |


### Related Tables

|     Related table      |Cardinality|                             Join logic                             |
|------------------------|-----------|--------------------------------------------------------------------|
|tagNotHistorical        |ONE TO MANY|multiLineDescription.col_name_1a = tagNotHistorical.column_1        |
|tagPrimaryKeyColsMissing|MANY TO ONE|tagPrimaryKeyColsMissing.column_1 = multiLineDescription.col_name_1a|
"""

        expected_tag_columns_missing = """## tagColumnsMissing

tagColumnsMissing file description

|Column|Column Description|Part of Primary Key?|Distinct Values|Is PII?|
|------|------------------|--------------------|---------------|-------|
"""
        expected_tag_exempt_from_validations = """## tagExemptFromValidations

tag with import-blocking validation exemptions

|Column|Column Description|Part of Primary Key?|Distinct Values|Is PII?|Import-Blocking Validation Exemptions|
|------|------------------|--------------------|---------------|-------|-------------------------------------|
|COL1  |COL1 description  |YES                 |N/A            |False  |N/A                                  |
|COL2  |COL2 description  |                    |N/A            |False  |<ul><li>NONNULL_VALUES</li></ul>     |


### Table-Wide Import-Blocking Validation Exemptions

|         Validation Type         |Exemption Reason|
|---------------------------------|----------------|
|STABLE_HISTORICAL_RAW_DATA_COUNTS|reason          |
"""
        expected_tag_not_historical = """## tagNotHistorical

tagNotHistorical file description

| Column |Column Description|Part of Primary Key?|Distinct Values|Is PII?|
|--------|------------------|--------------------|---------------|-------|
|column_1|<No documentation>|                    |N/A            |False  |


### Related Tables

|   Related table    |Cardinality|                         Join logic                         |
|--------------------|-----------|------------------------------------------------------------|
|multiLineDescription|MANY TO ONE|multiLineDescription.col_name_1a = tagNotHistorical.column_1|
"""

        expected_tag_primary_key_cols_missing = """## tagPrimaryKeyColsMissing

tagPrimaryKeyColsMissing file description

| Column | Column Description |Part of Primary Key?|Distinct Values|Is PII?|
|--------|--------------------|--------------------|---------------|-------|
|column_1|column_1 description|                    |N/A            |False  |


### Related Tables

|   Related table    |Cardinality|                             Join logic                             |
|--------------------|-----------|--------------------------------------------------------------------|
|multiLineDescription|ONE TO MANY|tagPrimaryKeyColsMissing.column_1 = multiLineDescription.col_name_1a|
"""
        expected_documentation = {
            "multiLineDescription.md": expected_multi_line,
            "raw_data.md": expected_raw_data,
            "tagColumnsMissing.md": expected_tag_columns_missing,
            "tagExemptFromValidations.md": expected_tag_exempt_from_validations,
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
        "recidiviz.ingest.direct.views.direct_ingest_view_query_builder"
        ".get_region_raw_file_config"
    )
    def test_get_referencing_views(
        self,
        mock_config_fn: MagicMock,
    ) -> None:
        mock_config_fn.return_value = FakeDirectIngestRegionRawFileConfig("US_XX")
        documentation_generator = DirectIngestDocumentationGenerator()
        tags = ["tagFullyEmptyFile", "tagHeadersNoContents", "tagBasicData"]
        my_collector = FakeDirectIngestViewQueryBuilderCollector(
            region=fake_region(), expected_ingest_views=tags
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
