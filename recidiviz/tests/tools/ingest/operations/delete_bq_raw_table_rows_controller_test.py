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
"""Tests for the delete_bq_raw_table_rows_controller module."""
from typing import Any, Dict, List

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tools.ingest.operations.delete_bq_raw_table_rows_controller import (
    DeleteBQRawTableRowsController,
)


class TestDeleteBQRawTableRowsController(BigQueryEmulatorTestCase):
    """Tests for the DeleteBQRawTableRowsController class"""

    def setUp(self) -> None:
        super().setUp()
        self.state_code = StateCode.US_XX
        self.ingest_instance = DirectIngestInstance.PRIMARY
        self.file_tag = "tag1"
        self.file_tag_to_file_ids_to_delete = {self.file_tag: [1]}
        self.controller = DeleteBQRawTableRowsController(
            bq_client=self.bq_client,
            state_code=self.state_code,
            ingest_instance=self.ingest_instance,
            dry_run=False,
            skip_prompts=True,
        )
        self.table_address = BigQueryAddress(
            dataset_id=self.controller.dataset_id, table_id=self.file_tag
        )
        self._load_data()

    def _assert_query_results(
        self, query: str, expected_result: List[Dict[str, Any]]
    ) -> None:
        raw_result = self.query(query)
        self.assertEqual(raw_result.to_dict("records"), expected_result)

    def _load_data(self) -> None:
        self.create_mock_table(
            address=self.table_address,
            schema=[
                schema_field_for_type("file_id", int),
                schema_field_for_type("data", str),
            ],
        )
        data = [{"file_id": 1, "data": "data1"}, {"file_id": 2, "data": "data2"}]
        self.load_rows_into_table(self.table_address, data)

    def test_run(self) -> None:
        self.controller.run(file_tag_to_file_ids_to_delete={self.file_tag: [1]})
        self._assert_query_results(
            f"SELECT * FROM {self.table_address.to_str()}",
            [{"file_id": 2, "data": "data2"}],
        )

    def test_run_multiple_file_ids(self) -> None:
        self.controller.run(file_tag_to_file_ids_to_delete={self.file_tag: [1, 2]})
        self._assert_query_results(f"SELECT * FROM {self.table_address.to_str()}", [])

    def test_run_no_matches(self) -> None:
        self.controller.run(file_tag_to_file_ids_to_delete={self.file_tag: [3]})
        self._assert_query_results(
            f"SELECT * FROM {self.table_address.to_str()}",
            [{"file_id": 1, "data": "data1"}, {"file_id": 2, "data": "data2"}],
        )

    def test_run_multiple_file_tags(self) -> None:
        table_address_2 = BigQueryAddress(
            dataset_id=self.controller.dataset_id, table_id="tag2"
        )
        self.create_mock_table(
            address=table_address_2,
            schema=[
                schema_field_for_type("file_id", int),
                schema_field_for_type("data", str),
            ],
        )
        data = [{"file_id": 1, "data": "data3"}, {"file_id": 2, "data": "data4"}]
        self.load_rows_into_table(table_address_2, data)

        self.controller.run(
            file_tag_to_file_ids_to_delete={
                self.file_tag: [1],
                "tag2": [1],
            }
        )

        self._assert_query_results(
            f"SELECT * FROM {self.table_address.to_str()}",
            [{"file_id": 2, "data": "data2"}],
        )
        self._assert_query_results(
            f"SELECT * FROM {table_address_2.to_str()}",
            [{"file_id": 2, "data": "data4"}],
        )
