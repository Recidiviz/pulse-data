# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Unit tests for supplemental_dataset.py"""
import unittest
import uuid
from typing import Any, Dict
from unittest.mock import create_autospec

from google.cloud import bigquery
from mock import MagicMock, patch

from recidiviz.big_query.big_query_utils import transform_dict_to_bigquery_row
from recidiviz.calculator.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.calculator.supplemental.supplemental_dataset import (
    StateSpecificSupplementalDatasetGenerator,
    SupplementalDatasetTable,
)


class UsXxSupplementalDatasetTable(SupplementalDatasetTable):
    def __init__(self) -> None:
        super().__init__(destination_table_id="test_table", dependent_views=[])

    def query_from_dependent_views(self, project_id: str) -> str:
        return f"SELECT * FROM `{project_id}.test_dataset.test_table`"

    def process_row(self, row: bigquery.table.Row) -> Dict[str, Any]:
        return dict(row)


class UsXxSupplementalDatasetGenerator(StateSpecificSupplementalDatasetGenerator):
    def __init__(self) -> None:
        super().__init__(supplemental_dataset_tables=[UsXxSupplementalDatasetTable()])


@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="12345678"))
@patch(
    "recidiviz.utils.metadata.project_id", MagicMock(return_value="recidiviz-project")
)
@patch(
    "uuid.uuid4",
    MagicMock(side_effect=[uuid.UUID("504ad25e-1b61-4263-9de0-bad0ae1dfd1b")]),
)
@patch("google.cloud.bigquery.Client")
@patch(
    "recidiviz.calculator.supplemental.supplemental_dataset.rematerialize_views_for_view_builders",
    MagicMock(side_effect=None),
)
class TestStateSpecificSupplementalDatasetGenerator(unittest.TestCase):
    """Unit tests for StateSpecificSupplementalDatasetGenerator"""

    def setUp(self) -> None:
        self.client_patcher = patch(
            "recidiviz.calculator.supplemental.supplemental_dataset.BigQueryClientImpl"
        )
        self.mock_client = self.client_patcher.start().return_value
        self.mock_client.project_id = "test-project"
        self.mock_dataset_reference = create_autospec(bigquery.DatasetReference)
        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset_reference

        self.supplemental_dataset_generator = UsXxSupplementalDatasetGenerator()

        self.mock_query_reference_table = create_autospec(bigquery.QueryJob)
        self.initial_rows = [
            transform_dict_to_bigquery_row({"some_column": "some_value"})
        ]
        self.mock_query_reference_table.result.side_effect = [self.initial_rows, []]

    def test_generate_supplemental_data_table(
        self, _mock_bigquery_client: MagicMock
    ) -> None:
        self.mock_client.create_dataset_if_necessary.side_effect = None
        self.mock_client.create_table_with_schema.side_effect = None
        self.mock_client.run_query_async.return_value = self.mock_query_reference_table
        self.mock_client.update_schema.side_effect = None

        self.supplemental_dataset_generator.generate_dataset_table("test_table")

        self.mock_client.run_query_async.assert_called_with(
            query_str="SELECT * FROM `test-project.test_dataset.test_table`"
        )
        self.mock_client.paged_read_and_process.assert_called()
        self.mock_client.insert_into_table_from_table_async.assert_called_with(
            source_dataset_id="temp_supplemental_data",
            source_table_id="temp_test_table_504ad25e-1b61-4263-9de0-bad0ae1dfd1b",
            destination_dataset_id=SUPPLEMENTAL_DATA_DATASET,
            destination_table_id="test_table",
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        self.mock_client.delete_table.assert_called_with(
            dataset_id="temp_supplemental_data",
            table_id="temp_test_table_504ad25e-1b61-4263-9de0-bad0ae1dfd1b",
        )
