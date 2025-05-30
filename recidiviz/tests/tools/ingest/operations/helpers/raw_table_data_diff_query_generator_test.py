# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for RawTableDataDiffQueryGenerator"""
import datetime
from typing import Any, Dict, List

import pandas as pd

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tools.ingest.operations.helpers.raw_table_data_diff_query_generator import (
    RawTableDataDiffQueryGenerator,
)


class RawTableDataDiffQueryGeneratorTest(BigQueryEmulatorTestCase):
    """Tests for RawTableDataDiffQueryGenerator"""

    def setUp(self) -> None:
        super().setUp()
        self.region_code = "us_xx"
        self.file_tag = "tagBasicData"
        self.src_dataset_id = "us_xx_raw_data"
        self.src_table_address = BigQueryAddress(
            dataset_id=self.src_dataset_id, table_id=self.file_tag
        )
        self.cmp_dataset_id = "us_xx_raw_data_secondary"
        self.cmp_table_address = BigQueryAddress(
            dataset_id=self.cmp_dataset_id, table_id=self.file_tag
        )
        self.create_mock_table(
            address=self.src_table_address,
            schema=[
                schema_field_for_type("COL1", str),
                schema_field_for_type("COL2", str),
                schema_field_for_type("COL3", str),
                schema_field_for_type("update_datetime", datetime.datetime),
                schema_field_for_type("is_deleted", bool),
            ],
        )
        self.create_mock_table(
            address=self.cmp_table_address,
            schema=[
                schema_field_for_type("COL1", str),
                schema_field_for_type("COL2", str),
                schema_field_for_type("COL3", str),
                schema_field_for_type("update_datetime", datetime.datetime),
                schema_field_for_type("is_deleted", bool),
            ],
        )
        self.query_generator = RawTableDataDiffQueryGenerator(
            src_project_id=self.project_id,
            src_dataset_id=self.src_dataset_id,
            cmp_project_id=self.project_id,
            cmp_dataset_id=self.cmp_dataset_id,
            region_raw_file_config=DirectIngestRegionRawFileConfig(
                self.region_code, region_module=fake_regions
            ),
            truncate_update_datetime_col_name="update_datetime",
            optional_datetime_filter=None,
        )
        self.query_str = self.query_generator.generate_query(self.file_tag)

    def _load_data(
        self, src_data: List[Dict[str, Any]], cmp_data: List[Dict[str, Any]]
    ) -> None:
        self.load_rows_into_table(self.src_table_address, src_data)
        self.load_rows_into_table(self.cmp_table_address, cmp_data)

    def test_diff_data_success(self) -> None:
        data = [
            {
                "COL1": "val1",
                "COL2": "val2",
                "COL3": "val3",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
            {
                "COL1": "val4",
                "COL2": "val5",
                "COL3": "val6",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
        ]

        self._load_data(src_data=data, cmp_data=data)

        result = self.query(self.query_str)
        self.assertTrue(result.empty)

    def test_diff_data_failure(self) -> None:
        expected_msg = """The following 1 columns had differences:
\t- COL2

In the following 1 rows:
update_datetime: 2024-01-26T00:00:00
COL1: val4, COL3: val6, is_deleted: False
COLUMNS WITH DIFFERENCES:
	COL2:
		src: val5
		cmp: val55555

"""
        src_data = [
            {
                "COL1": "val1",
                "COL2": "val2",
                "COL3": "val3",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
            {
                "COL1": "val4",
                "COL2": "val5",
                "COL3": "val6",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
        ]
        cmp_data = [
            {
                "COL1": "val1",
                "COL2": "val2",
                "COL3": "val3",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
            {
                "COL1": "val4",
                "COL2": "val55555",  # different value
                "COL3": "val6",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
        ]

        self._load_data(src_data=src_data, cmp_data=cmp_data)

        raw_result = self.query(self.query_str)
        parsed_result = self.query_generator.parse_query_result(
            raw_result.to_dict("records")
        )

        self.assertEqual(parsed_result.build_result_rows_str(), expected_msg)

    def test_datetime_filter(self) -> None:
        src_data = [
            {
                "COL1": "val1",
                "COL2": "val2",
                "COL3": "val3",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
            {
                "COL1": "val4",
                "COL2": "val5",
                "COL3": "val6",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
        ]
        cmp_data = [
            {
                "COL1": "val1",
                "COL2": "val2",
                "COL3": "val3",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
            {
                "COL1": "val4",
                "COL2": "val55555",  # different value
                "COL3": "val6",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
        ]

        self._load_data(src_data=src_data, cmp_data=cmp_data)

        query_generator = RawTableDataDiffQueryGenerator(
            src_project_id=self.project_id,
            src_dataset_id=self.src_dataset_id,
            cmp_project_id=self.project_id,
            cmp_dataset_id=self.cmp_dataset_id,
            region_raw_file_config=DirectIngestRegionRawFileConfig(
                self.region_code, region_module=fake_regions
            ),
            truncate_update_datetime_col_name="update_datetime",
            optional_datetime_filter="WHERE update_datetime < '2024-01-26'",
        )

        result = self.query(query_generator.generate_query(self.file_tag))
        self.assertTrue(result.empty)

    def test_diff_data_missing_src(self) -> None:
        expected_msg = """The following 1 comparison table rows had no exact primary key match in the source table:
update_datetime: 2024-01-26T00:00:00
COL1: val4, COL2: val55555, COL3: val6, is_deleted: False

"""
        src_data = [
            {
                "COL1": "val1",
                "COL2": "val2",
                "COL3": "val3",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
        ]
        cmp_data = [
            {
                "COL1": "val1",
                "COL2": "val2",
                "COL3": "val3",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
            {
                "COL1": "val4",
                "COL2": "val55555",
                "COL3": "val6",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
        ]
        self._load_data(src_data=src_data, cmp_data=cmp_data)

        raw_result = self.query(self.query_str)
        parsed_result = self.query_generator.parse_query_result(
            raw_result.applymap(lambda x: None if pd.isna(x) else x).to_dict("records")
        )
        self.assertEqual(parsed_result.build_result_rows_str(), expected_msg)

    def test_diff_data_missing_cmp(self) -> None:
        expected_msg = """The following 1 source table rows had no exact primary key match in the comparison table:
update_datetime: 2024-01-26T00:00:00
COL1: val4, COL2: val5, COL3: val6, is_deleted: False

"""
        src_data = [
            {
                "COL1": "val1",
                "COL2": "val2",
                "COL3": "val3",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
            {
                "COL1": "val4",
                "COL2": "val5",
                "COL3": "val6",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
        ]
        cmp_data = [
            {
                "COL1": "val1",
                "COL2": "val2",
                "COL3": "val3",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
                "is_deleted": False,
            },
        ]
        self._load_data(src_data=src_data, cmp_data=cmp_data)

        raw_result = self.query(self.query_str)
        parsed_result = self.query_generator.parse_query_result(
            raw_result.applymap(lambda x: None if pd.isna(x) else x).to_dict("records")
        )
        self.assertEqual(parsed_result.build_result_rows_str(), expected_msg)
