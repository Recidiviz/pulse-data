#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#  #
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  #
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  #
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Unit tests for fetching column values for a specific state."""
import unittest
from typing import Any, Iterator

from mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    ColumnEnumValueInfo,
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.tools.ingest.development.fetch_column_values_for_state import (
    update_region_config_with_enum_values,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING


class FakeQueryJob:
    def __init__(self, result: Iterator[dict[str, Any]]) -> None:
        self._result = result

    def result(self) -> Iterator[dict[str, Any]]:
        return self._result


class FetchColumnValuesForStateTest(unittest.TestCase):
    """Unit tests for fetching column values for a specific state."""

    def setUp(self) -> None:
        self.file_tag = "myFile"
        self.state_code = StateCode.US_XX
        self.project_id = GCP_PROJECT_STAGING
        self.issue_id = 123
        self.sandbox_dataset_prefix = None

        self.sample_column_info_enum = RawTableColumnInfo(
            name="test_column_enum",
            state_code=StateCode.US_ND,
            file_tag=self.file_tag,
            description="A test enum column",
            field_type=RawTableColumnFieldType.STRING,
            known_values=[
                ColumnEnumValueInfo(value="existing_value_1", description="desc1"),
                ColumnEnumValueInfo(value="existing_value_2", description="desc2"),
            ],
        )

        self.sample_column_info_non_enum = RawTableColumnInfo(
            name="test_column_non_enum",
            state_code=StateCode.US_ND,
            file_tag=self.file_tag,
            description="A test non-enum column",
            field_type=RawTableColumnFieldType.STRING,
            known_values=None,
        )

        self.sample_raw_file_config = DirectIngestRawFileConfig(
            state_code=StateCode.US_ND,
            file_tag=self.file_tag,
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[self.sample_column_info_enum, self.sample_column_info_non_enum],
            custom_line_terminator=None,
            primary_key_cols=["test_column_enum"],
            supplemental_order_by_clause="",
            encoding="UTF-8",
            separator=",",
            ignore_quotes=False,
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
            no_valid_primary_keys=False,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.IRREGULAR,
        )

        self.mock_region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code=self.state_code.value,
            raw_file_configs={self.file_tag: self.sample_raw_file_config},
            yaml_config_file_dir="test_dir",
        )

        self.bq_client_patcher = patch(
            "recidiviz.big_query.big_query_client.BigQueryClientImpl"
        )
        self.mock_bq_client = self.bq_client_patcher.start()
        self.mock_bq_client_instance = Mock()
        self.mock_bq_client.return_value = self.mock_bq_client_instance

        self.metadata_project_id_patcher = patch(
            "recidiviz.utils.metadata.local_project_id_override"
        )
        self.metadata_project_id_patcher.start()

    def tearDown(self) -> None:
        self.bq_client_patcher.stop()
        self.metadata_project_id_patcher.stop()

    def test_update_region_config_with_enum_values(self) -> None:
        self.mock_bq_client_instance.run_query_async.return_value = FakeQueryJob(
            iter(
                [
                    {"values": "new_value_1"},
                ]
            )
        )

        updated_region_config = update_region_config_with_enum_values(
            region_config=self.mock_region_raw_file_config,
            project_id=self.project_id,
            bq_client=self.mock_bq_client_instance,
            issue_id=self.issue_id,
            sandbox_dataset_prefix=self.sandbox_dataset_prefix,
            file_tags=[],
        )
        updated_file_config = updated_region_config.raw_file_configs[self.file_tag]

        if not updated_file_config.all_columns[0].known_values:
            self.fail(
                f"Expected known_values to be populated for [{updated_file_config.all_columns[0].name}]."
            )
        self.assertEqual(
            len(updated_file_config.all_columns[0].known_values),
            3,
        )
        self.assertIn(
            ColumnEnumValueInfo(
                value="new_value_1",
                description="TO" + f"DO(#{self.issue_id}) Document this value.",
            ),
            updated_file_config.all_columns[0].known_values,
        )
        self.assertEqual(updated_file_config.all_columns[1].known_values, None)
