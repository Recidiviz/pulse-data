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
"""Tests for helper functions for hydrating datetime SQL parsers."""
import unittest
from typing import Iterator

import attr
from mock import ANY, Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawDataClassification,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers import (
    update_parsers_in_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING


class HydrateDatetimeSqlParsersTest(unittest.TestCase):
    """Tests functions included in the datetime SQL parser hydration script"""

    def setUp(self) -> None:
        # Basic raw file info
        self.sparse_config = DirectIngestRawFileConfig(
            state_code=StateCode.US_XX,
            file_tag="myFile",
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[],
            custom_line_terminator=None,
            primary_key_cols=[],
            supplemental_order_by_clause="",
            encoding="UTF-8",
            separator=",",
            ignore_quotes=False,
            always_historical_export=True,
            no_valid_primary_keys=False,
            import_chunk_size_rows=10,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )
        # Some datetime parsers
        self.parsers_list = [
            "SAFE.PARSE_DATETIME('%m/%d/%y', {col_name})",
            "SAFE.PARSE_DATETIME('%m/%d/%Y', {col_name})",
        ]
        # Input config to be updated
        self.input_config = attr.evolve(
            self.sparse_config,
            columns=[
                RawTableColumnInfo(
                    name="dateCol1",
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="test",
                ),
                RawTableColumnInfo(
                    name="dateCol2",
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="test",
                ),
            ],
        )

    @patch(
        "recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers.get_region_raw_file_config"
    )
    @patch(
        "recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers.BigQueryClientImpl"
    )
    @patch(
        "recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers.RawDataConfigWriter"
    )
    def test_update_parsers_unsuccessful(
        self,
        mock_raw_data_config_writer: Mock,
        mock_bq_client: Mock,
        mock_get_region_raw_file_config: Mock,
    ) -> None:
        mock_get_region_raw_file_config.return_value = Mock(
            raw_file_configs={"myFile": self.input_config},
            get_datetime_parsers=lambda: self.parsers_list,
            default_config=Mock(),
        )

        def mock_run_query_async(query_str: str, use_query_cache: bool) -> Iterator:
            # pylint: disable=unused-argument
            if "dateCol1" in query_str:
                # No non-null entries
                return iter([[0, 0, 0]])
            if "dateCol2" in query_str:
                # Some entries, but no parsers work
                return iter([[0, 0, 42]])
            return iter([[]])

        mock_bq_client.return_value = Mock(run_query_async=mock_run_query_async)

        fake_region_config_writer = Mock()
        mock_raw_data_config_writer.return_value = fake_region_config_writer

        update_parsers_in_region(
            StateCode("US_XX"), GCP_PROJECT_STAGING, ["myFile"], None, None
        )

        # Since no columns were updated, we shouldn't have called this function
        fake_region_config_writer.output_to_file.assert_not_called()

    @patch(
        "recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers.get_region_raw_file_config"
    )
    @patch(
        "recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers.BigQueryClientImpl"
    )
    @patch(
        "recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers.RawDataConfigWriter"
    )
    def test_update_parsers_successful(
        self,
        mock_raw_data_config_writer: Mock,
        mock_bq_client: Mock,
        mock_get_region_raw_file_config: Mock,
    ) -> None:
        mock_get_region_raw_file_config.return_value = Mock(
            raw_file_configs={"myFile": self.input_config},
            get_datetime_parsers=lambda: self.parsers_list,
            default_config=Mock(),
        )

        def mock_run_query_async(query_str: str, use_query_cache: bool) -> Iterator:
            # pylint: disable=unused-argument
            if "dateCol1" in query_str:
                # First parser works
                return iter([[137, 0, 137]])
            if "dateCol2" in query_str:
                # Second parser works
                return iter([[0, 42, 42]])
            return iter([[]])

        mock_bq_client.return_value = Mock(run_query_async=mock_run_query_async)

        fake_region_config_writer = Mock()
        mock_raw_data_config_writer.return_value = fake_region_config_writer

        update_parsers_in_region(
            StateCode("US_XX"), GCP_PROJECT_STAGING, ["myFile"], None, None
        )

        updated_config = attr.evolve(
            self.input_config,
            columns=[
                RawTableColumnInfo(
                    name="dateCol1",
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="test",
                    datetime_sql_parsers=self.parsers_list[:1],
                ),
                RawTableColumnInfo(
                    name="dateCol2",
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="test",
                    datetime_sql_parsers=self.parsers_list[-1:],
                ),
            ],
        )
        fake_region_config_writer.output_to_file.assert_called_with(
            raw_file_config=updated_config,
            output_path=ANY,
            default_encoding=ANY,
            default_separator=ANY,
            default_ignore_quotes=ANY,
            default_always_historical_export=ANY,
            default_no_valid_primary_keys=ANY,
            default_line_terminator=ANY,
            default_update_cadence=ANY,
            default_infer_columns_from_config=ANY,
            default_import_blocking_validation_exemptions=ANY,
        )
