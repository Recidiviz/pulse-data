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
from recidiviz.big_query.big_query_view import (
    BigQueryAddress,
    BigQueryView,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.view_update_manager_utils import (
    cleanup_datasets_and_delete_unmanaged_views,
    delete_unmanaged_views_and_tables_from_dataset,
    get_managed_view_and_materialized_table_addresses_by_dataset,
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
        result_dict = get_managed_view_and_materialized_table_addresses_by_dataset(
            walker
        )
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {}
        self.assertEqual(expected_result, result_dict)

    def test_get_managed_views_for_dataset_map_one_view_list(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.one_view_list)
        # Act
        result_dict = get_managed_view_and_materialized_table_addresses_by_dataset(
            walker
        )
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {
            "dataset_0": {BigQueryAddress(dataset_id="dataset_0", table_id="table_0")}
        }
        self.assertEqual(expected_result, result_dict)

    def test_get_managed_views_for_dataset_map_x_shaped_dag(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)
        # Act
        result_dict = get_managed_view_and_materialized_table_addresses_by_dataset(
            walker
        )
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
        result_dict = get_managed_view_and_materialized_table_addresses_by_dataset(
            walker
        )
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
        result_dict = get_managed_view_and_materialized_table_addresses_by_dataset(
            walker
        )
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
        result_dict = get_managed_view_and_materialized_table_addresses_by_dataset(
            walker
        )
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

    def test_cleanup_datasets_and_delete_unmanaged_views_unmanaged_view_in_ds(
        self,
    ) -> None:
        datasets_that_have_ever_been_managed = {
            "dataset_1",
        }

        dataset = bigquery.dataset.DatasetReference(self.project_id, "dataset_1")

        sample_views = [
            {
                "view_id": "my_fake_view",
                "view_query": "SELECT NULL LIMIT 0",
            },
            {"view_id": "my_other_fake_view", "view_query": "SELECT NULL LIMIT 0"},
        ]
        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_1",
                description=f"{view['view_id']} description",
                view_query_template="a",
                should_materialize=False,
                materialized_address_override=None,
                should_build_predicate=None,
                **view,
            )
            for view in sample_views
        ]

        mock_table_resource_ds_1_table_1 = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "my_fake_view",
            },
        }

        mock_table_resource_ds_1_table_2 = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "my_other_fake_view",
            },
        }

        mock_table_resource_ds_1_table_3 = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "bogus_view",
            },
        }
        self.mock_client.list_tables.return_value = [
            bigquery.table.TableListItem(mock_table_resource_ds_1_table_1),
            bigquery.table.TableListItem(mock_table_resource_ds_1_table_2),
            bigquery.table.TableListItem(mock_table_resource_ds_1_table_3),
        ]
        self.mock_client.dataset_ref_for_id.return_value = dataset
        self.mock_client.dataset_exists.return_value = True

        views_to_update = [view_builder.build() for view_builder in mock_view_builders]

        dag_walker = BigQueryViewDagWalker(views_to_update)

        managed_views_map = (
            get_managed_view_and_materialized_table_addresses_by_dataset(dag_walker)
        )

        cleanup_datasets_and_delete_unmanaged_views(
            self.mock_client,
            managed_views_map,
            datasets_that_have_ever_been_managed=datasets_that_have_ever_been_managed,
            dry_run=False,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.mock_client.list_tables.assert_called()
        self.mock_client.delete_table.assert_called_once_with("dataset_1", "bogus_view")

    def test_cleanup_datasets_and_delete_unmanaged_views_unmanaged_view_in_multiple_ds(
        self,
    ) -> None:
        datasets_that_have_ever_been_managed = {
            "dataset_1",
            "dataset_2",
        }

        dataset = bigquery.dataset.DatasetReference(self.project_id, "dataset_1")
        dataset_2 = bigquery.dataset.DatasetReference(self.project_id, "dataset_2")

        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_1",
                view_id="my_fake_view",
                description="my_fake_view description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=False,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_2",
                view_id="my_fake_view_2",
                description="my_fake_view_2 description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=False,
            ),
        ]

        mock_table_resource_ds_1_table = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "my_fake_view",
            },
        }

        mock_table_resource_ds_1_table_bogus = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "bogus_view_1",
            },
        }

        mock_table_resource_ds_2_table = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_2",
                "tableId": "my_fake_view_2",
            },
        }

        mock_table_resource_ds_2_table_bogus = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_2",
                "tableId": "bogus_view_2",
            },
        }

        def mock_list_tables(dataset_id: str) -> bigquery.table.TableListItem:
            if dataset_id == dataset.dataset_id:
                return [
                    bigquery.table.TableListItem(mock_table_resource_ds_1_table),
                    bigquery.table.TableListItem(mock_table_resource_ds_1_table_bogus),
                ]
            if dataset_id == dataset_2.dataset_id:
                return [
                    bigquery.table.TableListItem(mock_table_resource_ds_2_table),
                    bigquery.table.TableListItem(mock_table_resource_ds_2_table_bogus),
                ]
            raise ValueError(f"No tables for id: {dataset_id}")

        self.mock_client.list_tables.side_effect = mock_list_tables

        def get_dataset_ref(dataset_id: str) -> bigquery.dataset.DatasetReference:
            if dataset_id == dataset.dataset_id:
                return dataset
            if dataset_id == dataset_2.dataset_id:
                return dataset_2
            raise ValueError(f"No dataset for id: {dataset_id}")

        self.mock_client.dataset_ref_for_id.side_effect = get_dataset_ref
        self.mock_client.dataset_exists.return_value = True

        views_to_update = [view_builder.build() for view_builder in mock_view_builders]

        dag_walker = BigQueryViewDagWalker(views_to_update)

        managed_views_map = (
            get_managed_view_and_materialized_table_addresses_by_dataset(dag_walker)
        )

        cleanup_datasets_and_delete_unmanaged_views(
            self.mock_client,
            managed_views_map,
            datasets_that_have_ever_been_managed=datasets_that_have_ever_been_managed,
            dry_run=False,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.mock_client.list_tables.assert_called()
        self.mock_client.delete_table.assert_has_calls(
            [call("dataset_1", "bogus_view_1"), call("dataset_2", "bogus_view_2")],
            any_order=True,
        )
        self.assertEqual(self.mock_client.delete_table.call_count, 2)

    def test_cleanup_datasets_and_delete_unmanaged_views_no_unmanaged_views(
        self,
    ) -> None:
        datasets_that_have_ever_been_managed = {
            "dataset_1",
        }

        dataset = bigquery.dataset.DatasetReference(self.project_id, "dataset_1")

        sample_views = [
            {
                "view_id": "my_fake_view",
                "view_query": "SELECT NULL LIMIT 0",
            },
            {"view_id": "my_other_fake_view", "view_query": "SELECT NULL LIMIT 0"},
        ]
        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_1",
                description=f"{view['view_id']} description",
                view_query_template="a",
                should_materialize=False,
                materialized_address_override=None,
                should_build_predicate=None,
                **view,
            )
            for view in sample_views
        ]

        mock_table_resource_ds_1_table_1 = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "my_fake_view",
            },
        }

        mock_table_resource_ds_1_table_2 = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "my_other_fake_view",
            },
        }

        self.mock_client.list_tables.return_value = [
            bigquery.table.TableListItem(mock_table_resource_ds_1_table_1),
            bigquery.table.TableListItem(mock_table_resource_ds_1_table_2),
        ]
        self.mock_client.dataset_ref_for_id.return_value = dataset
        self.mock_client.dataset_exists.return_value = True

        views_to_update = [view_builder.build() for view_builder in mock_view_builders]

        dag_walker = BigQueryViewDagWalker(views_to_update)

        managed_views_map = (
            get_managed_view_and_materialized_table_addresses_by_dataset(dag_walker)
        )

        cleanup_datasets_and_delete_unmanaged_views(
            self.mock_client,
            managed_views_map,
            datasets_that_have_ever_been_managed=datasets_that_have_ever_been_managed,
            dry_run=False,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.mock_client.list_tables.assert_called()
        self.mock_client.delete_table.assert_not_called()

    def test_cleanup_datasets_and_delete_unmanaged_views_unmanaged_dataset(
        self,
    ) -> None:
        datasets_that_have_ever_been_managed = {
            "dataset_1",
            "bogus_dataset",
        }

        dataset = bigquery.dataset.DatasetReference(self.project_id, "dataset_1")
        dataset_2 = bigquery.dataset.DatasetReference(self.project_id, "bogus_dataset")

        sample_views = [
            {
                "view_id": "my_fake_view",
                "view_query": "SELECT NULL LIMIT 0",
            }
        ]
        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_1",
                description=f"{view['view_id']} description",
                view_query_template="a",
                should_materialize=False,
                materialized_address_override=None,
                should_build_predicate=None,
                **view,
            )
            for view in sample_views
        ]

        mock_table_resource_ds_1_table_1 = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "my_fake_view",
            },
        }

        self.mock_client.list_tables.return_value = [
            bigquery.table.TableListItem(mock_table_resource_ds_1_table_1),
        ]

        def get_dataset_ref(dataset_id: str) -> bigquery.dataset.DatasetReference:
            if dataset_id == dataset.dataset_id:
                return dataset
            if dataset_id == dataset_2.dataset_id:
                return dataset_2
            raise ValueError(f"No dataset for id: {dataset_id}")

        self.mock_client.dataset_ref_for_id.side_effect = get_dataset_ref
        self.mock_client.dataset_exists.return_value = True

        views_to_update = [view_builder.build() for view_builder in mock_view_builders]

        dag_walker = BigQueryViewDagWalker(views_to_update)

        managed_views_map = (
            get_managed_view_and_materialized_table_addresses_by_dataset(dag_walker)
        )

        cleanup_datasets_and_delete_unmanaged_views(
            self.mock_client,
            managed_views_map,
            datasets_that_have_ever_been_managed=datasets_that_have_ever_been_managed,
            dry_run=False,
        )

        self.mock_client.delete_dataset.assert_called_with(
            self.mock_client.dataset_ref_for_id("bogus_dataset"), delete_contents=True
        )
        self.mock_client.list_tables.assert_called()
        self.mock_client.delete_table.assert_not_called()

    def test_cleanup_datasets_and_delete_unmanaged_views_dataset_not_in_master_list(
        self,
    ) -> None:
        datasets_that_have_ever_been_managed = {
            "dataset_1",
        }

        dataset = bigquery.dataset.DatasetReference(self.project_id, "dataset_1")
        dataset_2 = bigquery.dataset.DatasetReference(self.project_id, "bogus_dataset")

        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_1",
                view_id="my_fake_view",
                description="my_fake_view description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=False,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="bogus_dataset",
                view_id="my_fake_view_2",
                description="my_fake_view_2 description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=False,
            ),
        ]

        mock_table_resource_ds_1_table_1 = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "my_fake_view",
            },
        }

        mock_table_resource_ds_2_table_1 = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "bogus_dataset",
                "tableId": "my_fake_view_2",
            },
        }

        def mock_list_tables(dataset_id: str) -> bigquery.table.TableListItem:
            if dataset_id == dataset.dataset_id:
                return [
                    bigquery.table.TableListItem(mock_table_resource_ds_1_table_1),
                ]
            if dataset_id == dataset_2.dataset_id:
                return [
                    bigquery.table.TableListItem(mock_table_resource_ds_2_table_1),
                ]
            raise ValueError(f"No tables for id: {dataset_id}")

        self.mock_client.list_tables.side_effect = mock_list_tables

        def get_dataset_ref(dataset_id: str) -> bigquery.dataset.DatasetReference:
            if dataset_id == dataset.dataset_id:
                return dataset
            if dataset_id == dataset_2.dataset_id:
                return dataset_2
            raise ValueError(f"No dataset for id: {dataset_id}")

        self.mock_client.dataset_ref_for_id.side_effect = get_dataset_ref
        self.mock_client.dataset_exists.return_value = True

        views_to_update = [view_builder.build() for view_builder in mock_view_builders]

        dag_walker = BigQueryViewDagWalker(views_to_update)

        managed_views_map = (
            get_managed_view_and_materialized_table_addresses_by_dataset(dag_walker)
        )

        with self.assertRaises(ValueError):
            cleanup_datasets_and_delete_unmanaged_views(
                self.mock_client,
                managed_views_map,
                datasets_that_have_ever_been_managed=datasets_that_have_ever_been_managed,
                dry_run=False,
            )

        self.mock_client.delete_dataset.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    def test_cleanup_datasets_and_delete_unmanaged_views_unmanaged_dataset_and_dataset_not_in_BigQuery(
        self,
    ) -> None:
        datasets_that_have_ever_been_managed = {
            "dataset_1",
            "dataset_unmanaged",
        }

        dataset = bigquery.dataset.DatasetReference(self.project_id, "dataset_1")
        dataset_2 = bigquery.dataset.DatasetReference(
            self.project_id, "dataset_unmanaged"
        )

        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_1",
                view_id="my_fake_view",
                description="my_fake_view description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=False,
            )
        ]

        mock_table_resource_ds_1_table_1 = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "my_fake_view",
            },
        }

        self.mock_client.list_tables.return_value = [
            bigquery.table.TableListItem(mock_table_resource_ds_1_table_1),
        ]

        def get_dataset_ref(dataset_id: str) -> bigquery.dataset.DatasetReference:
            if dataset_id == dataset.dataset_id:
                return dataset
            if dataset_id == dataset_2.dataset_id:
                return dataset_2
            raise ValueError(f"No dataset for id: {dataset_id}")

        self.mock_client.dataset_ref_for_id.side_effect = get_dataset_ref

        def mock_dataset_exists(dataset_ref: bigquery.dataset.DatasetReference) -> bool:
            if dataset_ref == dataset:
                return True
            return False

        self.mock_client.dataset_exists.side_effect = mock_dataset_exists

        views_to_update = [view_builder.build() for view_builder in mock_view_builders]

        dag_walker = BigQueryViewDagWalker(views_to_update)

        managed_views_map = (
            get_managed_view_and_materialized_table_addresses_by_dataset(dag_walker)
        )

        with self.assertLogs() as captured_log:
            cleanup_datasets_and_delete_unmanaged_views(
                self.mock_client,
                managed_views_map,
                datasets_that_have_ever_been_managed=datasets_that_have_ever_been_managed,
                dry_run=False,
            )
        # check that there is only one log message
        self.assertEqual(len(captured_log.records), 1)
        self.mock_client.delete_dataset.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    def test_cleanup_datasets_and_delete_unmanaged_views_dry_run(
        self,
    ) -> None:
        datasets_that_have_ever_been_managed = {
            "dataset_1",
            "dataset_2",
            "dataset_unmanaged",
        }

        dataset = bigquery.dataset.DatasetReference(self.project_id, "dataset_1")
        dataset_2 = bigquery.dataset.DatasetReference(self.project_id, "dataset_2")
        dataset_3 = bigquery.dataset.DatasetReference(
            self.project_id, "dataset_unmanaged"
        )

        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_1",
                view_id="my_fake_view",
                description="my_fake_view description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=False,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id="dataset_2",
                view_id="my_fake_view_2",
                description="my_fake_view_2 description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=False,
            ),
        ]

        mock_table_resource_ds_1_table = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "my_fake_view",
            },
        }

        mock_table_resource_ds_1_table_bogus = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_1",
                "tableId": "bogus_view_1",
            },
        }

        mock_table_resource_ds_2_table = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_2",
                "tableId": "my_fake_view_2",
            },
        }

        mock_table_resource_ds_2_table_bogus = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": "dataset_2",
                "tableId": "bogus_view_2",
            },
        }

        def mock_list_tables(dataset_id: str) -> bigquery.table.TableListItem:
            if dataset_id == dataset.dataset_id:
                return [
                    bigquery.table.TableListItem(mock_table_resource_ds_1_table),
                    bigquery.table.TableListItem(mock_table_resource_ds_1_table_bogus),
                ]
            if dataset_id == dataset_2.dataset_id:
                return [
                    bigquery.table.TableListItem(mock_table_resource_ds_2_table),
                    bigquery.table.TableListItem(mock_table_resource_ds_2_table_bogus),
                ]
            raise ValueError(f"No tables for id: {dataset_id}")

        self.mock_client.list_tables.side_effect = mock_list_tables

        def get_dataset_ref(dataset_id: str) -> bigquery.dataset.DatasetReference:
            if dataset_id == dataset.dataset_id:
                return dataset
            if dataset_id == dataset_2.dataset_id:
                return dataset_2
            if dataset_id == dataset_3.dataset_id:
                return dataset_3
            raise ValueError(f"No dataset for id: {dataset_id}")

        self.mock_client.dataset_ref_for_id.side_effect = get_dataset_ref
        self.mock_client.dataset_exists.return_value = True

        views_to_update = [view_builder.build() for view_builder in mock_view_builders]

        dag_walker = BigQueryViewDagWalker(views_to_update)

        managed_views_map = (
            get_managed_view_and_materialized_table_addresses_by_dataset(dag_walker)
        )

        cleanup_datasets_and_delete_unmanaged_views(
            self.mock_client,
            managed_views_map,
            datasets_that_have_ever_been_managed=datasets_that_have_ever_been_managed,
            dry_run=True,
        )

        self.mock_client.delete_dataset.assert_not_called()
        self.mock_client.list_tables.assert_called()
        self.mock_client.delete_table.assert_not_called()
