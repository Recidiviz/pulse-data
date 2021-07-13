# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for classes in big_query_view_dag_walker.py"""

import unittest
from typing import Dict, List, Set
from unittest import mock

from google.cloud import bigquery
from mock import call, patch

from recidiviz.big_query.big_query_table_checker import BigQueryTableChecker
from recidiviz.big_query.big_query_view import BigQueryAddress, BigQueryView
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.view_update_manager_utils import (
    delete_unmanaged_views_and_tables_from_dataset,
    get_managed_views_for_dataset_map,
)
from recidiviz.view_registry.deployed_views import DEPLOYED_VIEW_BUILDERS


class TestViewUpdateManagerUtils(unittest.TestCase):
    """Tests for ViewUpdateManagerUtils"""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"

        with patch.object(
            BigQueryTableChecker, "_table_has_column"
        ) as mock_table_has_column, patch.object(
            BigQueryTableChecker, "_table_exists"
        ) as mock_table_exists:
            mock_table_has_column.return_value = True
            mock_table_exists.return_value = True

            self.all_views = [
                view_builder.build() for view_builder in DEPLOYED_VIEW_BUILDERS
            ]
        self.empty_view_list: List[BigQueryView] = []
        self.one_view_list = [
            BigQueryView(
                dataset_id="dataset_0",
                view_id="table_0",
                description="table_0 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
            )
        ]
        # Views forming a DAG shaped like an X:
        #  1     2
        #   \   /
        #     3
        #   /   \
        #  4     5
        self.x_shaped_dag_views_list = [
            BigQueryView(
                dataset_id="dataset_1",
                view_id="table_1",
                description="table_1 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
            ),
            BigQueryView(
                dataset_id="dataset_2",
                view_id="table_2",
                description="table_2 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
            ),
            BigQueryView(
                dataset_id="dataset_3",
                view_id="table_3",
                description="table_3 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_1.table_1`
            JOIN `{project_id}.dataset_2.table_2`
            USING (col)""",
            ),
            BigQueryView(
                dataset_id="dataset_4",
                view_id="table_4",
                description="table_4 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
            ),
            BigQueryView(
                dataset_id="dataset_5",
                view_id="table_5",
                description="table_5 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
            ),
        ]

        # Views forming a DAG with a diamond in it:
        #  1     2
        #   \   /
        #     3
        #   /   \
        #  4     5
        #   \   /
        #     6
        self.diamond_shaped_dag_views_list = self.x_shaped_dag_views_list.copy()
        self.diamond_shaped_dag_views_list.append(
            BigQueryView(
                dataset_id="dataset_6",
                view_id="table_6",
                description="table_6 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_4.table_4`
            JOIN `{project_id}.dataset_5.table_5`
            USING (col)""",
            )
        )

        self.multiple_views_one_dataset_list = [
            BigQueryView(
                dataset_id="dataset_1",
                view_id="table_1",
                description="table_1 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
            ),
            BigQueryView(
                dataset_id="dataset_1",
                view_id="table_2",
                description="table_2 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
            ),
            BigQueryView(
                dataset_id="dataset_3",
                view_id="table_3",
                description="table_3 description",
                view_query_template="""
                    SELECT * FROM `{project_id}.dataset_1.table_1`
                    JOIN `{project_id}.dataset_1.table_2`
                    USING (col)""",
            ),
            BigQueryView(
                dataset_id="dataset_4",
                view_id="table_4",
                description="table_4 description",
                view_query_template="""
                    SELECT * FROM `{project_id}.dataset_3.table_3`""",
            ),
        ]

        self.all_views_same_dataset = [
            BigQueryView(
                dataset_id="dataset_1",
                view_id="table_1",
                description="table_1 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
            ),
            BigQueryView(
                dataset_id="dataset_1",
                view_id="table_2",
                description="table_2 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
            ),
            BigQueryView(
                dataset_id="dataset_1",
                view_id="table_3",
                description="table_3 description",
                view_query_template="""
                    SELECT * FROM `{project_id}.dataset_1.table_1`
                    JOIN `{project_id}.dataset_1.table_2`
                    USING (col)""",
            ),
        ]
        self.project_id = "fake-recidiviz-project"
        self.mock_view_dataset_name = "my_views_dataset"
        self.mock_dataset_ref_ds_1 = bigquery.dataset.DatasetReference(
            self.project_id, "dataset_1"
        )
        self.mock_table_resource_template = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": self.mock_view_dataset_name,
                "tableId": "fake_table",
            },
        }
        self.mock_table_resource_ds_1_table_1 = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "table_1",
            },
        }

        self.mock_table_resource_ds_1_table_2 = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "table_2",
            },
        }

        self.project_number_patcher = patch("recidiviz.utils.metadata.project_number")
        self.project_number_patcher.start().return_value = "123456789"

        self.bq_client_patcher = mock.patch(
            "recidiviz.big_query.view_update_manager_utils.BigQueryClient"
        )
        self.mock_client = self.bq_client_patcher.start().return_value

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def test_get_managed_views_for_dataset_map_empty_list(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.empty_view_list)
        # Act
        result_dict = get_managed_views_for_dataset_map(walker)
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {}
        self.assertEqual(expected_result, result_dict)

    def test_get_managed_views_for_dataset_map_one_view_list(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.one_view_list)
        # Act
        result_dict = get_managed_views_for_dataset_map(walker)
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {
            "dataset_0": {BigQueryAddress(dataset_id="dataset_0", table_id="table_0")}
        }
        self.assertEqual(expected_result, result_dict)

    def test_get_managed_views_for_dataset_map_x_shaped_dag(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)
        # Act
        result_dict = get_managed_views_for_dataset_map(walker)
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {
            "dataset_1": {BigQueryAddress(dataset_id="dataset_1", table_id="table_1")},
            "dataset_2": {BigQueryAddress(dataset_id="dataset_2", table_id="table_2")},
            "dataset_3": {BigQueryAddress(dataset_id="dataset_3", table_id="table_3")},
            "dataset_4": {BigQueryAddress(dataset_id="dataset_4", table_id="table_4")},
            "dataset_5": {BigQueryAddress(dataset_id="dataset_5", table_id="table_5")},
        }
        self.assertEqual(expected_result, result_dict)

    def test_get_managed_views_for_dataset_map_diamond_shaped_dag(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)
        # Act
        result_dict = get_managed_views_for_dataset_map(walker)
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {
            "dataset_1": {BigQueryAddress(dataset_id="dataset_1", table_id="table_1")},
            "dataset_2": {BigQueryAddress(dataset_id="dataset_2", table_id="table_2")},
            "dataset_3": {BigQueryAddress(dataset_id="dataset_3", table_id="table_3")},
            "dataset_4": {BigQueryAddress(dataset_id="dataset_4", table_id="table_4")},
            "dataset_5": {BigQueryAddress(dataset_id="dataset_5", table_id="table_5")},
            "dataset_6": {BigQueryAddress(dataset_id="dataset_6", table_id="table_6")},
        }
        self.assertEqual(expected_result, result_dict)

    def test_get_managed_views_for_dataset_map_all_views_same_dataset(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.all_views_same_dataset)
        # Act
        result_dict = get_managed_views_for_dataset_map(walker)
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {
            "dataset_1": {
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1"),
                BigQueryAddress(dataset_id="dataset_1", table_id="table_2"),
                BigQueryAddress(dataset_id="dataset_1", table_id="table_3"),
            },
        }
        self.assertEqual(expected_result, result_dict)

    def test_get_managed_views_for_dataset_map_multiple_views_one_dataset(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.multiple_views_one_dataset_list)
        # Act
        result_dict = get_managed_views_for_dataset_map(walker)
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {
            "dataset_1": {
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1"),
                BigQueryAddress(dataset_id="dataset_1", table_id="table_2"),
            },
            "dataset_3": {BigQueryAddress(dataset_id="dataset_3", table_id="table_3")},
            "dataset_4": {BigQueryAddress(dataset_id="dataset_4", table_id="table_4")},
        }
        self.assertEqual(expected_result, result_dict)

    def test_delete_unmanaged_views_and_tables_simple(self) -> None:
        managed_tables = {BigQueryAddress(dataset_id="dataset_1", table_id="table_1")}
        self.mock_client.list_tables.return_value = [
            bigquery.table.TableListItem(self.mock_table_resource_ds_1_table_1),
            bigquery.table.TableListItem(self.mock_table_resource_ds_1_table_2),
        ]
        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset_ref_ds_1
        self.mock_client.dataset_exists.return_value = True
        expected_deleted_views: Set[BigQueryAddress] = {
            BigQueryAddress(dataset_id="dataset_1", table_id="table_2")
        }
        deleted_views = delete_unmanaged_views_and_tables_from_dataset(
            self.mock_client, "dataset_1", managed_tables, dry_run=False
        )
        self.mock_client.dataset_ref_for_id.assert_called()
        self.mock_client.dataset_exists.assert_called()
        self.mock_client.list_tables.assert_called()
        self.mock_client.delete_table.assert_called_with("dataset_1", "table_2")
        self.assertEqual(deleted_views, expected_deleted_views)

    def test_delete_unmanaged_views_and_tables_no_unmanaged_views(self) -> None:
        managed_tables = {
            BigQueryAddress(dataset_id="dataset_1", table_id="table_1"),
            BigQueryAddress(dataset_id="dataset_1", table_id="table_2"),
        }
        self.mock_client.list_tables.return_value = [
            bigquery.table.TableListItem(self.mock_table_resource_ds_1_table_1),
            bigquery.table.TableListItem(self.mock_table_resource_ds_1_table_2),
        ]
        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset_ref_ds_1
        self.mock_client.dataset_exists.return_value = True
        expected_deleted_views: Set[BigQueryAddress] = set()
        deleted_views = delete_unmanaged_views_and_tables_from_dataset(
            self.mock_client, "dataset_1", managed_tables, dry_run=False
        )
        self.mock_client.dataset_ref_for_id.assert_called()
        self.mock_client.dataset_exists.assert_called()
        self.mock_client.list_tables.assert_called()
        self.mock_client.delete_table.assert_not_called()
        self.assertEqual(deleted_views, expected_deleted_views)

    def test_delete_unmanaged_views_and_tables_all_unmanaged_views(self) -> None:
        managed_tables: Set[BigQueryAddress] = set()
        self.mock_client.list_tables.return_value = [
            bigquery.table.TableListItem(self.mock_table_resource_ds_1_table_1),
            bigquery.table.TableListItem(self.mock_table_resource_ds_1_table_2),
        ]
        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset_ref_ds_1
        self.mock_client.dataset_exists.return_value = True
        expected_deleted_views: Set[BigQueryAddress] = {
            BigQueryAddress(dataset_id="dataset_1", table_id="table_1"),
            BigQueryAddress(dataset_id="dataset_1", table_id="table_2"),
        }
        deleted_views = delete_unmanaged_views_and_tables_from_dataset(
            self.mock_client, "dataset_1", managed_tables, dry_run=False
        )
        self.mock_client.dataset_ref_for_id.assert_called()
        self.mock_client.dataset_exists.assert_called()
        delete_calls = [call("dataset_1", "table_1"), call("dataset_1", "table_2")]
        self.mock_client.list_tables.assert_called()
        self.mock_client.delete_table.assert_has_calls(delete_calls, any_order=True)
        self.assertEqual(deleted_views, expected_deleted_views)

    def test_delete_unmanaged_views_and_tables_no_views_existing_dataset(self) -> None:
        managed_tables: Set[BigQueryAddress] = set()
        self.mock_client.list_tables.return_value = []
        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset_ref_ds_1
        self.mock_client.dataset_exists.return_value = True
        expected_deleted_views: Set[BigQueryAddress] = set()
        deleted_views = delete_unmanaged_views_and_tables_from_dataset(
            self.mock_client, "dataset_1", managed_tables, dry_run=False
        )
        self.mock_client.dataset_ref_for_id.assert_called()
        self.mock_client.dataset_exists.assert_called()
        self.mock_client.list_tables.assert_called()
        self.mock_client.delete_table.assert_not_called()
        self.assertEqual(deleted_views, expected_deleted_views)

    def test_delete_unmanaged_views_and_tables_dry_run(self) -> None:
        managed_tables = {BigQueryAddress(dataset_id="dataset_1", table_id="table_1")}
        self.mock_client.list_tables.return_value = [
            bigquery.table.TableListItem(self.mock_table_resource_ds_1_table_1),
            bigquery.table.TableListItem(self.mock_table_resource_ds_1_table_2),
        ]
        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset_ref_ds_1
        self.mock_client.dataset_exists.return_value = True
        expected_deleted_views: Set[BigQueryAddress] = {
            BigQueryAddress(dataset_id="dataset_1", table_id="table_2")
        }
        deleted_views = delete_unmanaged_views_and_tables_from_dataset(
            self.mock_client, "dataset_1", managed_tables, dry_run=True
        )
        self.mock_client.dataset_ref_for_id.assert_called()
        self.mock_client.dataset_exists.assert_called()
        self.mock_client.list_tables.assert_called()
        self.mock_client.delete_table.assert_not_called()
        self.assertEqual(deleted_views, expected_deleted_views)

    def test_delete_unmanaged_views_and_tables_dataset_doesnt_exist(self) -> None:
        managed_tables = {BigQueryAddress(dataset_id="dataset_1", table_id="table_1")}
        self.mock_client.list_tables.return_value = [
            bigquery.table.TableListItem(self.mock_table_resource_ds_1_table_1),
            bigquery.table.TableListItem(self.mock_table_resource_ds_1_table_2),
        ]
        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset_ref_ds_1
        self.mock_client.dataset_exists.return_value = False
        with self.assertRaises(ValueError):
            delete_unmanaged_views_and_tables_from_dataset(
                self.mock_client, "dataset_bogus", managed_tables, dry_run=False
            )
        self.mock_client.dataset_ref_for_id.assert_called()
        self.mock_client.dataset_exists.assert_called()
        self.mock_client.list_tables.assert_not_called()
        self.mock_client.delete_table.assert_not_called()
