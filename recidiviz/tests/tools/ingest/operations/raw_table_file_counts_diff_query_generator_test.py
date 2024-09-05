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
"""Tests for RawTableFileCountsDiffQueryGenerator"""
import datetime
from typing import Any, Dict, List

import pandas as pd
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tools.ingest.operations.raw_table_file_counts_diff_query_generator import (
    RawTableFileCountsDiffQueryGenerator,
)


class RawTableFileCountsDiffQueryGeneratorTest(BigQueryEmulatorTestCase):
    """Tests for RawTableFileCountsDiffQueryGenerator"""

    def setUp(self) -> None:
        super().setUp()
        self.region_code = "us_xx"
        self.file_tag = "test_table"
        self.src_table_address = BigQueryAddress(
            dataset_id="us_xx_raw_data", table_id="test_table"
        )
        self.cmp_table_address = BigQueryAddress(
            dataset_id="us_xx_raw_data_secondary", table_id="test_table"
        )
        self.create_mock_table(
            address=self.src_table_address,
            schema=[
                schema_field_for_type("file_id", int),
                schema_field_for_type("col1", str),
                schema_field_for_type("update_datetime", datetime.datetime),
            ],
        )
        self.create_mock_table(
            address=self.cmp_table_address,
            schema=[
                schema_field_for_type("file_id", str),
                schema_field_for_type("col1", str),
                schema_field_for_type("update_datetime", datetime.datetime),
            ],
        )
        self.query_generator = RawTableFileCountsDiffQueryGenerator(
            region_code=self.region_code,
            src_project_id=self.project_id,
            src_ingest_instance=DirectIngestInstance.PRIMARY,
            cmp_project_id=self.project_id,
            cmp_ingest_instance=DirectIngestInstance.SECONDARY,
        )
        self.query_str = self.query_generator.generate_query(self.file_tag)

    def _load_data(
        self, src_data: List[Dict[str, Any]], cmp_data: List[Dict[str, Any]]
    ) -> None:
        self.load_rows_into_table(self.src_table_address, src_data)
        self.load_rows_into_table(self.cmp_table_address, cmp_data)

    def test_diff_file_count_success(self) -> None:
        data = [
            {
                "file_id": "1",
                "col1": "val",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "2",
                "col1": "dif_val",  # only checking for counts not same data
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
        ]

        self._load_data(src_data=data, cmp_data=data)

        result = self.query(self.query_str)
        self.assertTrue(result.empty)

    def test_diff_file_count_missing_src(self) -> None:
        src_data = [
            {
                "file_id": "1",
                "col1": "val",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "2",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
        ]
        cmp_data = [
            {
                "file_id": "1",
                "col1": "val",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "2",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "3",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 2, 26, 0, 0, 0, 0),
            },
        ]
        self._load_data(src_data=src_data, cmp_data=cmp_data)

        result = self.query(self.query_str)
        result_list = result.to_dict("records")
        result_row = one(result_list)

        # should return row with the problematic update_datetime
        self.assertEqual(
            result_row["update_datetime"], datetime.datetime(2024, 2, 26, 0, 0, 0, 0)
        )
        self.assertTrue(pd.isnull(result_row["src_file_id_count"]))
        self.assertTrue(pd.isnull(result_row["src_row_count"]))

    def test_diff_file_count_missing_cmp(self) -> None:
        src_data = [
            {
                "file_id": "1",
                "col1": "val",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "2",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "3",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 2, 26, 0, 0, 0, 0),
            },
        ]
        cmp_data = [
            {
                "file_id": "1",
                "col1": "val",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "2",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
        ]
        self._load_data(src_data=src_data, cmp_data=cmp_data)

        result = self.query(self.query_str)
        result_list = result.to_dict("records")
        result_row = one(result_list)

        self.assertEqual(
            result_row["update_datetime"], datetime.datetime(2024, 2, 26, 0, 0, 0, 0)
        )
        self.assertTrue(pd.isnull(result_row["cmp_file_id_count"]))
        self.assertTrue(pd.isnull(result_row["cmp_row_count"]))

    def test_diff_file_count_different_id_count(self) -> None:
        src_data = [
            {
                "file_id": "1",
                "col1": "val",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "2",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "3",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
        ]
        cmp_data = [
            {
                "file_id": "1",
                "col1": "val",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "2",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "2",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
        ]
        self._load_data(src_data=src_data, cmp_data=cmp_data)

        result = self.query(self.query_str)
        result_list = result.to_dict("records")
        result_row = one(result_list)

        self.assertEqual(
            result_row["update_datetime"], datetime.datetime(2024, 1, 26, 0, 0, 0, 0)
        )
        self.assertNotEqual(
            result_row["src_file_id_count"], result_row["cmp_file_id_count"]
        )
        self.assertEqual(result_row["src_row_count"], result_row["cmp_row_count"])

    def test_diff_file_count_different_row_count(self) -> None:
        src_data = [
            {
                "file_id": "1",
                "col1": "val",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "2",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "2",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
        ]
        cmp_data = [
            {
                "file_id": "1",
                "col1": "val",
                "update_datetime": datetime.datetime(2023, 1, 26, 0, 0, 0, 0),
            },
            {
                "file_id": "2",
                "col1": "val",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
        ]
        self._load_data(src_data=src_data, cmp_data=cmp_data)

        result = self.query(self.query_str)
        result_list = result.to_dict("records")
        result_row = one(result_list)

        self.assertEqual(
            result_row["update_datetime"], datetime.datetime(2024, 1, 26, 0, 0, 0, 0)
        )
        self.assertEqual(
            result_row["src_file_id_count"], result_row["cmp_file_id_count"]
        )
        self.assertNotEqual(result_row["src_row_count"], result_row["cmp_row_count"])

    def test_truncate_datetime(self) -> None:
        data = [
            {
                "file_id": "1",
                "col1": "val",
                "update_datetime": datetime.datetime(2023, 1, 26, 1, 2, 3, 4),
            },
            {
                "file_id": "2",
                "col1": "dif_val",
                "update_datetime": datetime.datetime(2024, 1, 26, 0, 0, 0, 0),
            },
        ]

        self._load_data(src_data=data, cmp_data=data)

        query_generator = RawTableFileCountsDiffQueryGenerator(
            region_code=self.region_code,
            src_project_id=self.project_id,
            src_ingest_instance=DirectIngestInstance.PRIMARY,
            cmp_project_id=self.project_id,
            cmp_ingest_instance=DirectIngestInstance.SECONDARY,
            truncate_update_datetime_part="DAY",
        )
        query_str = query_generator.generate_query(self.file_tag)

        result = self.query(query_str)
        self.assertTrue(result.empty)
