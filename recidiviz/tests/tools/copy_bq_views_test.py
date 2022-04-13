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
# =============================================================================
"""Tests the copy_bq_views script."""
import unittest
from unittest import mock

from google.cloud import bigquery

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.tools.copy_bq_views import copy_bq_views


class CopyBQViewsTest(unittest.TestCase):
    """Tests for the copy_bq_views script."""

    def setUp(self) -> None:
        self.mock_source_project_id = "fake-recidiviz-project"
        self.mock_source_dataset_id = "fake-dataset"
        self.mock_source_dataset = bigquery.dataset.DatasetReference(
            self.mock_source_project_id, self.mock_source_dataset_id
        )

        self.mock_destination_project_id = "fake-recidiviz-project-2"
        self.mock_destination_dataset_id = "fake-dataset-2"
        self.mock_destination_dataset = bigquery.dataset.DatasetReference(
            self.mock_destination_project_id, self.mock_destination_dataset_id
        )

        self.client_patcher = mock.patch("recidiviz.big_query.big_query_client.client")
        self.mock_client = self.client_patcher.start().return_value

        self.mock_view = SimpleBigQueryViewBuilder(
            project_id=self.mock_source_project_id,
            dataset_id="dataset",
            view_id="test_view",
            description="test_view description",
            view_query_template="SELECT NULL LIMIT 0",
            should_materialize=True,
        ).build()

    def tearDown(self) -> None:
        self.client_patcher.stop()

    # pylint: disable=unused-argument
    def table_exists_side_effect(
        self, dataset_ref: bigquery.DatasetReference, table_id: str
    ) -> bool:
        return dataset_ref.dataset_id == self.mock_source_dataset_id

    @mock.patch("recidiviz.big_query.big_query_client.BigQueryClientImpl.copy_view")
    @mock.patch("recidiviz.big_query.big_query_client.BigQueryClientImpl.table_exists")
    def test_copy_bq_views(
        self, mock_table_exists: mock.MagicMock, mock_copy_view: mock.MagicMock
    ) -> None:
        """Check that copy_view is called when the view does not exist in the destination dataset."""
        self.mock_client.list_tables.return_value = [self.mock_view]
        self.mock_client.get_table.return_value = self.mock_view
        mock_table_exists.side_effect = self.table_exists_side_effect

        copy_bq_views(
            source_project_id=self.mock_source_project_id,
            source_dataset_id=self.mock_source_dataset_id,
            destination_project_id=self.mock_destination_project_id,
            destination_dataset_id=self.mock_destination_dataset_id,
        )

        expected_view = SimpleBigQueryViewBuilder(
            project_id=self.mock_destination_project_id,
            dataset_id=self.mock_destination_dataset_id,
            view_id=self.mock_view.view_id,
            description=self.mock_view.description,
            view_query_template=self.mock_view.view_query,
            should_materialize=True,
        ).build()

        expected_destination_dataset_ref = bigquery.DatasetReference(
            project=self.mock_destination_project_id,
            dataset_id=self.mock_destination_dataset_id,
        )

        mock_copy_view.assert_called()
        self.assertEqual(expected_view, mock_copy_view.call_args_list[0][1].get("view"))
        self.assertEqual(
            self.mock_destination_project_id,
            mock_copy_view.call_args_list[0][1].get("destination_client").project_id,
        )
        self.assertEqual(
            expected_destination_dataset_ref,
            mock_copy_view.call_args_list[0][1].get("destination_dataset_ref"),
        )

    @mock.patch("recidiviz.big_query.big_query_client.BigQueryClientImpl.copy_view")
    @mock.patch("recidiviz.big_query.big_query_client.BigQueryClientImpl.table_exists")
    def test_copy_bq_views_already_exists(
        self, mock_table_exists: mock.MagicMock, mock_copy_view: mock.MagicMock
    ) -> None:
        """Check that copy_view is not called when the view already exists in the destination dataset."""
        self.mock_client.list_tables.return_value = [self.mock_view]
        self.mock_client.get_table.return_value = self.mock_view
        mock_table_exists.return_value = True

        copy_bq_views(
            source_project_id=self.mock_source_project_id,
            source_dataset_id=self.mock_source_dataset_id,
            destination_project_id=self.mock_destination_project_id,
            destination_dataset_id=self.mock_destination_dataset_id,
        )

        mock_copy_view.assert_not_called()

    @mock.patch("recidiviz.big_query.big_query_client.BigQueryClientImpl.copy_view")
    @mock.patch("recidiviz.big_query.big_query_client.BigQueryClientImpl.table_exists")
    def test_copy_bq_views_not_table(
        self, mock_table_exists: mock.MagicMock, mock_copy_view: mock.MagicMock
    ) -> None:
        """Check that copy_view is not called on a table that does not have a view_query."""
        table_ref = bigquery.TableReference(self.mock_source_dataset, "table_not_view")
        schema_fields = [bigquery.SchemaField("fake_schema_field", "STRING")]
        table_not_view = bigquery.Table(table_ref, schema_fields)
        self.mock_client.list_tables.return_value = [table_not_view]
        self.mock_client.get_table.return_value = table_not_view
        mock_table_exists.side_effect = self.table_exists_side_effect

        copy_bq_views(
            source_project_id=self.mock_source_project_id,
            source_dataset_id=self.mock_source_dataset_id,
            destination_project_id=self.mock_destination_project_id,
            destination_dataset_id=self.mock_destination_dataset_id,
        )

        mock_copy_view.assert_not_called()

    @mock.patch("recidiviz.big_query.big_query_client.BigQueryClientImpl.copy_view")
    @mock.patch("recidiviz.big_query.big_query_client.BigQueryClientImpl.table_exists")
    def test_copy_bq_views_raw_project_id(
        self, mock_table_exists: mock.MagicMock, mock_copy_view: mock.MagicMock
    ) -> None:
        """Check that copy_view is called, even when the project_id is in the view_query_template."""
        view_with_project_id = SimpleBigQueryViewBuilder(
            project_id=self.mock_source_project_id,
            dataset_id=self.mock_source_dataset_id,
            view_id="test_view",
            description="test_view description",
            view_query_template=f"SELECT * FROM {self.mock_source_project_id}.other_dataset.table LIMIT 0",
            should_materialize=True,
        ).build()

        self.mock_client.list_tables.return_value = [view_with_project_id]
        self.mock_client.get_table.return_value = view_with_project_id
        mock_table_exists.side_effect = self.table_exists_side_effect

        copy_bq_views(
            source_project_id=self.mock_source_project_id,
            source_dataset_id=self.mock_source_dataset_id,
            destination_project_id=self.mock_destination_project_id,
            destination_dataset_id=self.mock_destination_dataset_id,
        )

        expected_view = SimpleBigQueryViewBuilder(
            project_id=self.mock_destination_project_id,
            dataset_id=self.mock_destination_dataset_id,
            view_id="test_view",
            description="test_view description",
            view_query_template=f"SELECT * FROM {self.mock_destination_project_id}.other_dataset.table LIMIT 0",
            should_materialize=True,
        ).build()

        expected_destination_dataset_ref = bigquery.DatasetReference(
            project=self.mock_destination_project_id,
            dataset_id=self.mock_destination_dataset_id,
        )

        mock_copy_view.assert_called()
        self.assertEqual(expected_view, mock_copy_view.call_args_list[0][1].get("view"))
        self.assertEqual(
            self.mock_destination_project_id,
            mock_copy_view.call_args_list[0][1].get("destination_client").project_id,
        )
        self.assertEqual(
            expected_destination_dataset_ref,
            mock_copy_view.call_args_list[0][1].get("destination_dataset_ref"),
        )
