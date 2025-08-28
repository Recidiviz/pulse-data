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
from typing import Any, Callable, Iterator

import attr
from mock import Mock, patch
from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers import (
    update_parsers_in_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING


class FakeQueryJob:
    def __init__(self, result: Iterator[dict[str, Any]]) -> None:
        self._result = result

    def result(self) -> Iterator[dict[str, Any]]:
        return self._result


def mock_update_region_raw_file_yamls(
    region_config: DirectIngestRegionRawFileConfig,
    config_updater_fn: Callable[
        [DirectIngestRegionRawFileConfig], DirectIngestRegionRawFileConfig
    ],
) -> DirectIngestRegionRawFileConfig:
    # Update the region config but don't write anything to file
    return config_updater_fn(region_config)


class HydrateDatetimeSqlParsersTest(unittest.TestCase):
    """Tests functions included in the datetime SQL parser hydration script"""

    def setUp(self) -> None:
        self.file_tag = "myFile"
        # Basic raw file info
        self.sparse_config = DirectIngestRawFileConfig(
            state_code=StateCode.US_XX,
            file_tag=self.file_tag,
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
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
            no_valid_primary_keys=False,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )
        # Some datetime parsers
        self.parsers_list = [
            "SAFE.PARSE_DATETIME('%m/%d/%y', {col_name})",
            "SAFE.PARSE_DATETIME('%m/%d/%Y', {col_name})",
        ]
        # Datetime config to add to our DirectIngestRegionRawFileConfig
        # so get_datetime_parsers will return our parsers list
        self.datetime_config = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            columns=[
                RawTableColumnInfo(
                    name="dateCol1",
                    state_code=StateCode.US_XX,
                    file_tag="myFile2",
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="test",
                    datetime_sql_parsers=self.parsers_list[:1],
                ),
                RawTableColumnInfo(
                    name="dateCol2",
                    state_code=StateCode.US_XX,
                    file_tag="myFile2",
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="test",
                    datetime_sql_parsers=self.parsers_list[-1:],
                ),
            ],
        )
        # Input config to be updated
        self.input_config = attr.evolve(
            self.sparse_config,
            columns=[
                RawTableColumnInfo(
                    name="dateCol1",
                    state_code=StateCode.US_XX,
                    file_tag=self.sparse_config.file_tag,
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="test",
                ),
                RawTableColumnInfo(
                    name="dateCol2",
                    state_code=StateCode.US_XX,
                    file_tag=self.sparse_config.file_tag,
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="test",
                ),
            ],
        )
        self.mock_region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            raw_file_configs={
                self.file_tag: self.input_config,
                "myFile2": self.datetime_config,
            },
            yaml_config_file_dir="test_dir",
        )
        self.update_region_raw_file_yamls_patcher = patch(
            "recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers.update_region_raw_file_yamls",
            side_effect=mock_update_region_raw_file_yamls,
        )
        self.update_region_raw_file_yamls_patcher.start()

    def tearDown(self) -> None:
        self.update_region_raw_file_yamls_patcher.stop()

    @patch(
        "recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers.BigQueryClientImpl"
    )
    def test_update_parsers_unsuccessful(
        self,
        mock_bq_client: Mock,
    ) -> None:
        def mock_run_query_async(query_str: str, use_query_cache: bool) -> FakeQueryJob:
            # pylint: disable=unused-argument
            if "dateCol1" in query_str:
                # No non-null entries
                return FakeQueryJob(
                    iter(
                        [
                            {
                                "nonnull_values": 0,
                                "example_nonnull_value": None,
                                "nonnull_parsed_values0": 0,
                                "example_unparsed_value0": None,
                                "nonnull_parsed_values1": 0,
                                "example_unparsed_value1": None,
                            }
                        ]
                    )
                )
            if "dateCol2" in query_str:
                # Some entries, but no parsers work
                return FakeQueryJob(
                    iter(
                        [
                            {
                                "nonnull_values": 42,
                                "example_nonnull_value": "NULL",
                                "nonnull_parsed_values0": 0,
                                "example_unparsed_value0": "NULL",
                                "nonnull_parsed_values1": 0,
                                "example_unparsed_value1": "NULL",
                            }
                        ]
                    )
                )
            return FakeQueryJob(iter([]))

        mock_bq_client.return_value = Mock(run_query_async=mock_run_query_async)

        update_parsers_in_region(
            region_config=self.mock_region_raw_file_config,
            state_code=StateCode("US_XX"),
            project_id=GCP_PROJECT_STAGING,
            file_tags=[self.file_tag],
            sandbox_dataset_prefix=None,
            parser=None,
        )

        # Config should be unchanged
        self.assertEqual(
            self.input_config,
            self.mock_region_raw_file_config.raw_file_configs[self.file_tag],
        )

    @patch(
        "recidiviz.tools.ingest.development.hydrate_datetime_sql_parsers.BigQueryClientImpl"
    )
    def test_update_parsers_successful(
        self,
        mock_bq_client: Mock,
    ) -> None:
        def mock_run_query_async(query_str: str, use_query_cache: bool) -> FakeQueryJob:
            # pylint: disable=unused-argument
            if "dateCol1" in query_str:
                # First parser works
                return FakeQueryJob(
                    iter(
                        [
                            {
                                "nonnull_values": 137,
                                "example_nonnull_value": "1992-01-01",
                                "nonnull_parsed_values0": 137,
                                "example_unparsed_value0": None,
                                "nonnull_parsed_values1": 0,
                                "example_unparsed_value1": "1992-01-01",
                            }
                        ]
                    )
                )
            if "dateCol2" in query_str:
                # Second parser works
                return FakeQueryJob(
                    iter(
                        [
                            {
                                "nonnull_values": 42,
                                "example_nonnull_value": "1992-01-01",
                                "nonnull_parsed_values0": 0,
                                "example_unparsed_value0": "1992-01-01",
                                "nonnull_parsed_values1": 42,
                                "example_unparsed_value1": None,
                            }
                        ]
                    )
                )
            return FakeQueryJob(iter([]))

        mock_bq_client.return_value = Mock(run_query_async=mock_run_query_async)

        update_parsers_in_region(
            region_config=self.mock_region_raw_file_config,
            state_code=StateCode("US_XX"),
            project_id=GCP_PROJECT_STAGING,
            file_tags=[self.file_tag],
            sandbox_dataset_prefix=None,
            parser=None,
        )

        actual_config = self.mock_region_raw_file_config.raw_file_configs[self.file_tag]
        for col in actual_config.current_columns:
            if not col.datetime_sql_parsers:
                self.fail(
                    f"No datetime parsers were added to datetime column [{col.name}]"
                )
            parser = one(col.datetime_sql_parsers)
            # because DirectIngestRegionRawFileConfig.get_datetime_parsers returns a set
            # we can't be sure which order we tried the parsers in, so we don't know which
            # exact parser was successful
            self.assertIn(parser, self.parsers_list)
