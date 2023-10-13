# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for bigquery_helpers_test.py"""
import sys
import unittest
from typing import Any, List, Optional
from unittest.mock import MagicMock, create_autospec, patch

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.tools.utils.bigquery_helpers import (
    dataset_id_to_filter_regex,
    dataset_prefix_to_filter_regex,
    run_operation_for_tables,
)


class TableListItemFactory:
    @classmethod
    def build_from_address(
        cls, project_id: str, address: BigQueryAddress
    ) -> bigquery.table.TableListItem:
        return cls.build(
            project_id=project_id,
            dataset_id=address.dataset_id,
            table_id=address.table_id,
        )

    @classmethod
    def build(
        cls, project_id: str, dataset_id: str, table_id: str, table_type: str = "TABLE"
    ) -> bigquery.table.TableListItem:
        return bigquery.table.TableListItem(
            {
                "tableReference": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": table_id,
                    "tableType": table_type,
                }
            }
        )


class DatasetListItemFactory:
    @classmethod
    def build(
        cls, project_id: str, dataset_id: str
    ) -> bigquery.dataset.DatasetListItem:
        return bigquery.dataset.DatasetListItem(
            {
                "datasetReference": {"projectId": project_id, "datasetId": dataset_id},
            }
        )


class RunOperationForTablesTest(unittest.TestCase):
    """Tests for bigquery_helpers_test.py"""

    def setUp(self) -> None:
        self.project_id = "recidiviz-456"
        self.mock_bq_client = create_autospec(BigQueryClient)

        self.dataset_1 = "my_dataset"
        self.dataset_2 = "my_dataset_2"
        self.sandbox_dataset = "my_prefix_my_dataset"

        self.mock_bq_client.list_datasets.return_value = [
            DatasetListItemFactory.build(self.project_id, self.dataset_1),
            DatasetListItemFactory.build(self.project_id, self.dataset_2),
            DatasetListItemFactory.build(self.project_id, self.sandbox_dataset),
        ]

        def mock_list_tables(dataset_id: str) -> List[bigquery.table.TableListItem]:
            if dataset_id == self.dataset_1:
                table_ids = ["table_1", "table_2"]
            elif dataset_id == self.dataset_2:
                table_ids = ["table_3"]
            elif dataset_id == self.sandbox_dataset:
                table_ids = ["table_1", "table_2"]
            else:
                raise ValueError(f"Unexpected datset_id [{dataset_id}]")

            return [
                TableListItemFactory.build(self.project_id, dataset_id, table_id)
                for table_id in table_ids
            ]

        self.mock_bq_client.list_tables.side_effect = mock_list_tables

    def test_run_operation_for_tables(self) -> None:
        # pylint:disable=unused-argument
        def return_address(
            client: BigQueryClient, address: ProjectSpecificBigQueryAddress
        ) -> ProjectSpecificBigQueryAddress:
            return address

        result = run_operation_for_tables(
            client=self.mock_bq_client,
            operation=return_address,
            prompt=None,
            dataset_filter=None,
        )

        self.assertEqual(
            {
                ProjectSpecificBigQueryAddress(
                    project_id="recidiviz-456",
                    dataset_id="my_dataset",
                    table_id="table_1",
                ),
                ProjectSpecificBigQueryAddress(
                    project_id="recidiviz-456",
                    dataset_id="my_dataset",
                    table_id="table_2",
                ),
                ProjectSpecificBigQueryAddress(
                    project_id="recidiviz-456",
                    dataset_id="my_dataset_2",
                    table_id="table_3",
                ),
                ProjectSpecificBigQueryAddress(
                    project_id="recidiviz-456",
                    dataset_id="my_prefix_my_dataset",
                    table_id="table_1",
                ),
                ProjectSpecificBigQueryAddress(
                    project_id="recidiviz-456",
                    dataset_id="my_prefix_my_dataset",
                    table_id="table_2",
                ),
            },
            set(result.values()),
        )

        def return_table_id(
            client: BigQueryClient, address: ProjectSpecificBigQueryAddress
        ) -> str:
            return address.table_id

        result_2 = run_operation_for_tables(
            client=self.mock_bq_client,
            prompt=None,
            operation=return_table_id,
            dataset_filter=None,
        )

        self.assertEqual(
            [
                "table_1",
                # Duplicates are from the sandbox dataset
                "table_1",
                "table_2",
                "table_2",
                "table_3",
            ],
            sorted(result_2.values()),
        )

    def test_run_operation_for_tables_filtered(self) -> None:
        # pylint:disable=unused-argument
        def return_address(
            client: BigQueryClient, address: ProjectSpecificBigQueryAddress
        ) -> ProjectSpecificBigQueryAddress:
            return address

        result = run_operation_for_tables(
            client=self.mock_bq_client,
            prompt=None,
            operation=return_address,
            dataset_filter=dataset_prefix_to_filter_regex("my_prefix"),
        )
        self.assertEqual(
            {
                ProjectSpecificBigQueryAddress(
                    project_id="recidiviz-456",
                    dataset_id="my_prefix_my_dataset",
                    table_id="table_1",
                ),
                ProjectSpecificBigQueryAddress(
                    project_id="recidiviz-456",
                    dataset_id="my_prefix_my_dataset",
                    table_id="table_2",
                ),
            },
            set(result.values()),
        )

        result = run_operation_for_tables(
            client=self.mock_bq_client,
            prompt=None,
            operation=return_address,
            dataset_filter=dataset_id_to_filter_regex("my_dataset"),
        )

        self.assertEqual(
            {
                ProjectSpecificBigQueryAddress(
                    project_id="recidiviz-456",
                    dataset_id="my_dataset",
                    table_id="table_1",
                ),
                ProjectSpecificBigQueryAddress(
                    project_id="recidiviz-456",
                    dataset_id="my_dataset",
                    table_id="table_2",
                ),
            },
            set(result.values()),
        )

    @patch("recidiviz.tools.utils.bigquery_helpers.prompt_for_confirmation")
    def test_run_operation_for_tables_with_prompt_exit(
        self, mock_prompt: MagicMock
    ) -> None:
        # pylint:disable=unused-argument
        def failing_action(
            client: BigQueryClient, address: ProjectSpecificBigQueryAddress
        ) -> ProjectSpecificBigQueryAddress:
            raise ValueError("Fail! Shouldn't call me.")

        def fail_prompt_for_confirmation(*args: Any, **kwargs: Any) -> Optional[bool]:
            sys.exit(1)

        mock_prompt.side_effect = fail_prompt_for_confirmation
        with self.assertRaises(SystemExit):
            run_operation_for_tables(
                client=self.mock_bq_client,
                prompt="Continue",
                operation=failing_action,
                dataset_filter=dataset_id_to_filter_regex("my_dataset"),
            )

        mock_prompt.assert_called_with("Continue for 1 datasets")
        self.mock_bq_client.list_datasets.assert_called_once()
        self.mock_bq_client.list_tables.assert_not_called()

    @patch("recidiviz.tools.utils.bigquery_helpers.prompt_for_confirmation")
    def test_run_operation_for_tables_with_prompt_continue(
        self, mock_prompt: MagicMock
    ) -> None:
        # pylint:disable=unused-argument
        def return_address(
            client: BigQueryClient, address: ProjectSpecificBigQueryAddress
        ) -> ProjectSpecificBigQueryAddress:
            return address

        def succeed_prompt_for_confirmation(
            *args: Any, **kwargs: Any
        ) -> Optional[bool]:
            return True

        mock_prompt.side_effect = succeed_prompt_for_confirmation
        result = run_operation_for_tables(
            client=self.mock_bq_client,
            prompt="Continue",
            operation=return_address,
            dataset_filter=dataset_id_to_filter_regex("my_dataset"),
        )

        mock_prompt.assert_called_with("Continue for 1 datasets")

        self.assertEqual(
            {
                ProjectSpecificBigQueryAddress(
                    project_id="recidiviz-456",
                    dataset_id="my_dataset",
                    table_id="table_1",
                ),
                ProjectSpecificBigQueryAddress(
                    project_id="recidiviz-456",
                    dataset_id="my_dataset",
                    table_id="table_2",
                ),
            },
            set(result.values()),
        )
