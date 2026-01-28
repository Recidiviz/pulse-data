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
"""Tests for BigQueryClientImpl"""
import datetime
import io
import random
import unittest

# pylint: disable=protected-access
# pylint: disable=unused-argument
from concurrent import futures
from typing import Any, Dict, Iterator, List
from unittest.mock import MagicMock, call, create_autospec, patch

import __main__
from freezegun import freeze_time
from google.api_core.future.polling import DEFAULT_RETRY, PollingFuture
from google.api_core.retry import Retry
from google.cloud import bigquery, exceptions
from google.cloud.bigquery import CopyJob, QueryJobConfig, SchemaField
from google.cloud.bigquery_datatransfer import (
    CheckValidCredsRequest,
    CheckValidCredsResponse,
    DataTransferServiceClient,
    StartManualTransferRunsResponse,
    TransferConfig,
    TransferRun,
    TransferState,
)

from recidiviz.big_query import big_query_client
from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_column import String
from recidiviz.big_query.export.export_query_config import ExportQueryConfig
from recidiviz.cloud_resources.resource_label import ResourceLabel
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


# TODO(#27976) migrate to use the BigQueryEmulatorTestCase for testing instead of
# mocking out the client where possible!
class BigQueryClientImplTest(unittest.TestCase):
    """Tests for BigQueryClientImpl"""

    def setUp(self) -> None:
        self.mock_project_id = "fake-recidiviz-project"
        self.mock_dataset_id = "fake-dataset"
        self.mock_table_id = "test_table"
        self.mock_table_address = BigQueryAddress(
            dataset_id=self.mock_dataset_id, table_id=self.mock_table_id
        )
        mock_dataset_ref = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_table_address.dataset_id
        )
        self.mock_table = mock_dataset_ref.table(self.mock_table_address.table_id)
        self.mock_schema = [
            bigquery.SchemaField(
                mode="NULLABLE", field_type="STRING", name="fake_column"
            )
        ]

        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.main_patcher = patch.object(
            __main__, "__file__", "a/b/fake_script.py", create=True
        )
        self.mock_main = self.main_patcher.start()

        self.client_patcher = patch(
            "recidiviz.big_query.big_query_client.bigquery.Client"
        )
        self.client_fn = self.client_patcher.start()
        self.mock_client = MagicMock()
        self.other_mock_client = MagicMock()
        self.client_fn.side_effect = [self.mock_client, self.other_mock_client]
        # Reset client caching
        big_query_client._clients_by_project_id_by_region.clear()

        self.copy_job_config_patcher = patch(
            "recidiviz.big_query.big_query_client.bigquery.CopyJobConfig"
        )
        self.mock_copy_job_config = self.copy_job_config_patcher.start()
        self.copy_job_config = bigquery.CopyJobConfig()
        self.mock_copy_job_config.return_value = self.copy_job_config

        self.mock_view = SimpleBigQueryViewBuilder(
            dataset_id=self.mock_dataset_id,
            view_id="test_view",
            description="test_view description",
            view_query_template="SELECT NULL LIMIT 0",
            should_materialize=True,
        ).build()

        self.bq_client = BigQueryClientImpl()

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.main_patcher.stop()
        self.metadata_patcher.stop()
        self.copy_job_config_patcher.stop()

    def test_create_dataset_if_necessary(self) -> None:
        """Check that a dataset is created if it does not exist."""
        self.mock_client.get_dataset.side_effect = exceptions.NotFound("!")
        self.mock_client.create_dataset.return_value.access_entries = []
        self.bq_client.create_dataset_if_necessary(self.mock_table_address.dataset_id)
        self.mock_client.create_dataset.assert_called()

    def test_create_dataset_if_necessary_dataset_exists(self) -> None:
        """Check that a dataset is not created if it already exists."""
        self.mock_client.get_dataset.side_effect = None
        self.bq_client.create_dataset_if_necessary(self.mock_table_address.dataset_id)
        self.mock_client.create_dataset.assert_not_called()

    def test_create_dataset_if_necessary_table_expiration(self) -> None:
        """Check that the dataset is created with a set table expiration if the dataset does not exist and the
        new_dataset_table_expiration_ms is specified."""
        self.mock_client.get_dataset.side_effect = exceptions.NotFound("!")
        self.mock_client.create_dataset.return_value.access_entries = []

        self.bq_client.create_dataset_if_necessary(
            self.mock_dataset_id, default_table_expiration_ms=6000
        )

        self.mock_client.create_dataset.assert_called()

    def test_create_dataset_if_necessary_sets_labels(self) -> None:
        self.mock_client.get_dataset.side_effect = exceptions.NotFound("!")
        self.mock_client.create_dataset.return_value.access_entries = []

        with freeze_time(datetime.datetime(2020, 1, 1, 1, 1, 1)):
            self.bq_client.create_dataset_if_necessary(
                self.mock_table_address.dataset_id
            )

        call_args_list = self.mock_client.update_dataset.call_args_list
        self.assertEqual(1, len(call_args_list))
        [(args, _kwargs)] = call_args_list
        self.assertEqual(
            {
                "vanta-owner": "joshua",
                "vanta-description": "generated-automatically-by-infrastructure-on-2020-01-01",
            },
            args[0].labels,
        )

    def test_create_dataset_if_necessary_sets_labels_manual(self) -> None:
        self.mock_client.get_dataset.side_effect = exceptions.NotFound("!")
        self.mock_client.create_dataset.return_value.access_entries = [
            bigquery.AccessEntry(
                role="OWNER",
                entity_type=bigquery.enums.EntityTypes.USER_BY_EMAIL,
                entity_id="foo@recidiviz.org",
            )
        ]

        with freeze_time(datetime.datetime(2020, 1, 1, 1, 1, 1)):
            self.bq_client.create_dataset_if_necessary(
                self.mock_table_address.dataset_id
            )

        call_args_list = self.mock_client.update_dataset.call_args_list
        self.assertEqual(1, len(call_args_list))
        [(args, _kwargs)] = call_args_list
        self.assertEqual(
            {
                "vanta-owner": "foo",
                "vanta-description": "generated-from-fake_script-by-foo-on-2020-01-01",
            },
            args[0].labels,
        )

    def test_create_dataset_if_necessary_sets_labels_replace_chars(self) -> None:
        self.mock_client.get_dataset.side_effect = exceptions.NotFound("!")
        self.mock_client.create_dataset.return_value.access_entries = [
            bigquery.AccessEntry(
                role="OWNER",
                entity_type=bigquery.enums.EntityTypes.USER_BY_EMAIL,
                entity_id="foo.Bar@recidiviz.org",
            )
        ]

        with freeze_time(datetime.datetime(2020, 1, 1, 1, 1, 1)):
            self.bq_client.create_dataset_if_necessary(
                self.mock_table_address.dataset_id
            )

        call_args_list = self.mock_client.update_dataset.call_args_list
        self.assertEqual(1, len(call_args_list))
        [(args, _kwargs)] = call_args_list
        self.assertEqual(
            {
                "vanta-owner": "foo-bar",
                "vanta-description": "generated-from-fake_script-by-foo-bar-on-2020-01-01",
            },
            args[0].labels,
        )

    def test_create_dataset_if_necessary_sets_labels_manual_non_recidiviz(self) -> None:
        self.mock_client.get_dataset.side_effect = exceptions.NotFound("!")
        self.mock_client.create_dataset.return_value.access_entries = [
            bigquery.AccessEntry(
                role="OWNER",
                entity_type=bigquery.enums.EntityTypes.USER_BY_EMAIL,
                entity_id="bar@gmail.com",
            )
        ]

        with freeze_time(datetime.datetime(2020, 1, 1, 1, 1, 1)):
            self.bq_client.create_dataset_if_necessary(
                self.mock_table_address.dataset_id
            )

        call_args_list = self.mock_client.update_dataset.call_args_list
        self.assertEqual(1, len(call_args_list))
        [(args, _kwargs)] = call_args_list
        self.assertEqual(
            {
                "vanta-owner": "joshua",
                "vanta-description": "generated-from-fake_script-by-unknown-on-2020-01-01",
            },
            args[0].labels,
        )

    def test_create_dataset_if_necessary_sets_labels_truncate(self) -> None:
        self.mock_client.get_dataset.side_effect = exceptions.NotFound("!")
        self.mock_client.create_dataset.return_value.access_entries = [
            bigquery.AccessEntry(
                role="OWNER",
                entity_type=bigquery.enums.EntityTypes.USER_BY_EMAIL,
                entity_id="1234567890123456789012345@recidiviz.org",
            )
        ]

        with freeze_time(datetime.datetime(2020, 1, 1, 1, 1, 1)):
            self.bq_client.create_dataset_if_necessary(
                self.mock_table_address.dataset_id
            )

        call_args_list = self.mock_client.update_dataset.call_args_list
        self.assertEqual(1, len(call_args_list))
        [(args, _kwargs)] = call_args_list
        self.assertEqual(
            {
                "vanta-owner": "1234567890123456789012345",
                "vanta-description": "generated-from-fake_script-by-1234567890123456789012345-on-2020",
            },
            args[0].labels,
        )

    def test_multiple_client_locations(self) -> None:
        other_location_bq_client = BigQueryClientImpl(region_override="us-east1")

        self.bq_client.get_table(self.mock_table_address)
        self.mock_client.get_table.assert_called()
        self.other_mock_client.get_table.assert_not_called()

        # The client that was created with a different location will use a new client
        other_location_bq_client.dataset_exists(self.mock_table_address.dataset_id)
        self.other_mock_client.get_dataset.assert_called()
        self.mock_client.get_dataset.assert_not_called()

        # Creating another client with the default location uses the original
        # bigquery.Client.
        default_location_bq_client = BigQueryClientImpl()
        default_location_bq_client.run_query_async(
            query_str="some query", use_query_cache=True
        )
        self.mock_client.query.assert_called()
        self.other_mock_client.query.assert_not_called()

    def test_table_exists(self) -> None:
        """Check that table_exists returns True if the table exists."""
        self.mock_client.get_table.side_effect = None
        self.assertTrue(self.bq_client.table_exists(self.mock_table_address))

    def test_table_exists_does_not_exist(self) -> None:
        """Check that table_exists returns False if the table does not exist."""
        self.mock_client.get_table.side_effect = exceptions.NotFound("!")
        table_exists = self.bq_client.table_exists(self.mock_table_address)
        self.assertFalse(table_exists)

    @patch("google.cloud.bigquery.QueryJobConfig")
    def test_run_query_async_with_job_labels(self, mock_job_config: MagicMock) -> None:
        self.bq_client.run_query_async(
            query_str="fake query",
            use_query_cache=False,
            job_labels=[
                ResourceLabel(key="fake", value="label"),
                ResourceLabel(
                    key="fake2",
                    value="label2",
                    parents=[ResourceLabel(key="fake3", value="label3")],
                ),
            ],
        )

        self.mock_client.query.assert_called_with(
            query="fake query",
            location=BigQueryClient.DEFAULT_REGION,
            job_config=mock_job_config.return_value,
            timeout=None,
        )
        mock_job_config.assert_called_with(
            use_query_cache=False,
            labels={
                "fake": "label",
                "fake2": "label2",
                "fake3": "label3",
                "platform_environment": "local_machine",
            },
        )

    @patch("google.cloud.bigquery.job.QueryJobConfig")
    def test_address_generates_job_labels(self, mock_job_config: MagicMock) -> None:
        self.bq_client.create_table_from_query_async(
            address=BigQueryAddress(
                dataset_id="us_xx_normalized_state",
                table_id="state_person",
            ),
            query="fake query",
            use_query_cache=False,
            job_labels=[ResourceLabel(key="test", value="123")],
        )

        mock_job_config.assert_called_with(
            use_query_cache=False,
            labels={
                "test": "123",
                "dataset_id": "us_xx_normalized_state",
                "table_id": "state_person",
                "big_query_address": "us_xx_normalized_state---state_person",
                "state_code": "us_xx",
                "platform_environment": "local_machine",
            },
        )

    @patch("google.cloud.bigquery.QueryJobConfig")
    def test_run_query_async_with_default_labels(
        self, mock_job_config: MagicMock
    ) -> None:
        bq_client_with_default_labels = BigQueryClientImpl(
            default_job_labels=[
                ResourceLabel(key="default", value="label"),
            ]
        )

        bq_client_with_default_labels.run_query_async(
            query_str="fake query",
            use_query_cache=False,
        )

        self.mock_client.query.assert_called_with(
            query="fake query",
            location=BigQueryClient.DEFAULT_REGION,
            job_config=mock_job_config.return_value,
            timeout=None,
        )
        mock_job_config.assert_called_with(
            use_query_cache=False,
            labels={"default": "label", "platform_environment": "local_machine"},
        )

    @patch("google.cloud.bigquery.job.QueryJobConfig")
    def test_with_default_labels_and_job_labels(
        self, mock_job_config: MagicMock
    ) -> None:
        bq_client_with_default_labels = BigQueryClientImpl(
            default_job_labels=[
                ResourceLabel(key="default", value="label"),
            ]
        )

        bq_client_with_default_labels.insert_into_table_from_query_async(
            destination_address=BigQueryAddress(
                dataset_id="normalized_state", table_id="state_person"
            ),
            query="fake query",
            use_query_cache=False,
            job_labels=[
                ResourceLabel(key="fake", value="label"),
                ResourceLabel(
                    key="fake2",
                    value="label2",
                    parents=[ResourceLabel(key="default", value="label")],
                ),
            ],
        )

        mock_job_config.assert_called_with(
            use_query_cache=False,
            labels={
                "fake": "label",
                "default": "label",
                "fake2": "label2",
                "dataset_id": "normalized_state",
                "table_id": "state_person",
                "big_query_address": "normalized_state---state_person",
                "state_code": "state_agnostic",
                "platform_environment": "local_machine",
            },
        )

    @patch("google.cloud.bigquery.QueryJobConfig")
    def test_run_query_async_labels_conflict(self, mock_job_config: MagicMock) -> None:
        bq_client_with_default_labels = BigQueryClientImpl(
            default_job_labels=[
                ResourceLabel(key="default", value="label"),
            ]
        )

        with self.assertRaisesRegex(
            ValueError, r"Found conflicting labels for key \[default\]: .*"
        ):
            bq_client_with_default_labels.run_query_async(
                query_str="fake query",
                use_query_cache=False,
                job_labels=[ResourceLabel(key="default", value="not-label")],
            )

    @patch("google.cloud.bigquery.QueryJobConfig")
    def test_get_row_counts_for_tables(self, mock_job_config: MagicMock) -> None:
        # Arrange
        self.mock_client.query.side_effect = [
            [{"num_rows": 120}],
            [{"num_rows": 0}],
        ]
        self.mock_client.list_tables.return_value = [
            bigquery.table.TableListItem.from_string(
                "fake-recidiviz-project.fake-dataset.foo"
            ),
            bigquery.table.TableListItem.from_string(
                "fake-recidiviz-project.fake-dataset.bar"
            ),
        ]

        # Act
        results = self.bq_client.get_row_counts_for_tables(
            self.mock_table_address.dataset_id
        )

        # Assert
        self.assertEqual(results, {"foo": 120, "bar": 0})
        self.mock_client.get_dataset.assert_called()
        expected_calls = [
            call(
                query="""
                SELECT COUNT(*) as num_rows
                FROM `fake-recidiviz-project.fake-dataset.foo`
            """,
                location=BigQueryClient.DEFAULT_REGION,
                job_config=mock_job_config.return_value,
                timeout=None,
            ),
            call(
                query="""
                SELECT COUNT(*) as num_rows
                FROM `fake-recidiviz-project.fake-dataset.bar`
            """,
                location=BigQueryClient.DEFAULT_REGION,
                job_config=mock_job_config.return_value,
                timeout=None,
            ),
        ]
        self.mock_client.query.assert_has_calls(expected_calls)
        mock_job_config.assert_called_with(
            use_query_cache=False,
            labels={
                "dataset_id": "fake-dataset",
                "platform_environment": "local_machine",
            },
        )

    def test_get_row_counts_for_tables_no_dataset(self) -> None:
        # Arrange
        self.mock_client.get_dataset.side_effect = exceptions.NotFound("!")

        # Act
        results = self.bq_client.get_row_counts_for_tables(
            self.mock_table_address.dataset_id
        )

        # Assert
        self.assertEqual(results, {})
        self.mock_client.get_dataset.assert_called()
        self.mock_client.query.assert_not_called()

    def test_get_row_counts_for_tables_empty_dataset(self) -> None:
        # Arrange
        self.mock_client.get_dataset.return_value = MagicMock()
        # list_tables returns no tables so the dataset is considered empty
        self.mock_client.list_tables.return_value = iter([])

        # Act
        results = self.bq_client.get_row_counts_for_tables(
            self.mock_table_address.dataset_id
        )

        # Assert
        self.assertEqual(results, {})
        self.mock_client.get_dataset.assert_called()
        self.mock_client.list_tables.assert_called()
        self.mock_client.query.assert_not_called()

    def test_create_or_update_view_creates_view(self) -> None:
        """create_or_update_view creates a View if it does not exist."""
        self.mock_client.update_table.side_effect = exceptions.NotFound("!")
        self.bq_client.create_or_update_view(self.mock_view)
        self.mock_client.create_table.assert_called()

    def test_create_or_update_view_updates_view(self) -> None:
        """create_or_update_view updates a View if it already exist."""
        self.mock_client.get_table.side_effect = None
        self.bq_client.create_or_update_view(self.mock_view)
        self.mock_client.update_table.assert_called()
        self.mock_client.create_table.assert_not_called()

    def test_create_or_update_view_creates_view_not_might_exist(self) -> None:
        """create_or_update_view creates a View if `might_exist` is not set"""
        self.bq_client.create_or_update_view(self.mock_view, might_exist=False)
        self.mock_client.update_table.assert_not_called()
        self.mock_client.create_table.assert_called()

    def test_export_to_cloud_storage(self) -> None:
        """export_to_cloud_storage extracts the table corresponding to the
        view."""
        self.assertIsNotNone(
            self.bq_client.export_table_to_cloud_storage_async(
                source_table_address=self.mock_table_address,
                destination_uri=f"gs://{self.mock_project_id}-bucket/destination_path.json",
                destination_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
                print_header=True,
            )
        )
        self.mock_client.extract_table.assert_called()

    def test_export_to_cloud_storage_no_table(self) -> None:
        """export_to_cloud_storage does not extract from a table if the table
        does not exist."""
        self.mock_client.get_table.side_effect = exceptions.NotFound("!")
        with self.assertLogs(level="WARNING"):
            self.assertIsNone(
                self.bq_client.export_table_to_cloud_storage_async(
                    source_table_address=self.mock_table_address,
                    destination_uri=f"gs://{self.mock_project_id}-bucket/destination_path.json",
                    destination_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
                    print_header=True,
                )
            )
            self.mock_client.extract_table.assert_not_called()

    def test_load_table_async_create_dataset(self) -> None:
        """Test that load_table_from_cloud_storage tries to create a parent dataset."""

        self.mock_client.get_dataset.side_effect = exceptions.NotFound("!")

        self.bq_client.load_table_from_cloud_storage(
            destination_address=self.mock_table_address,
            destination_table_schema=[
                SchemaField(
                    "my_column",
                    "STRING",
                    "NULLABLE",
                    None,
                )
            ],
            source_uris=["gs://bucket/export-uri"],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        self.mock_client.create_dataset.assert_called()
        self.mock_client.load_table_from_uri.assert_called()

    def test_load_table_async_dataset_exists(self) -> None:
        """Test that load_table_from_cloud_storage does not try to create a
        parent dataset if it already exists."""

        self.bq_client.load_table_from_cloud_storage(
            destination_address=self.mock_table_address,
            destination_table_schema=[
                SchemaField(
                    "my_column",
                    "STRING",
                    "NULLABLE",
                )
            ],
            source_uris=["gs://bucket/export-uri"],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        self.mock_client.create_dataset.assert_not_called()
        self.mock_client.load_table_from_uri.assert_called()

    def test_load_into_table_from_file_create_dataset(self) -> None:
        """Test that load_into_table_from_file tries to create a parent dataset."""

        self.mock_client.get_dataset.side_effect = exceptions.NotFound("!")

        self.bq_client.load_into_table_from_file(
            destination_address=self.mock_table_address,
            source=io.StringIO("data"),
            schema=self.mock_schema,
        )

        self.mock_client.create_dataset.assert_called()
        self.mock_client.load_table_from_file.assert_called()

    def test_load_into_table_from_file_dataset_exists(self) -> None:
        """Test that load_into_table_from_file does not try to create a
        parent dataset if it already exists."""

        self.bq_client.load_into_table_from_file(
            destination_address=self.mock_table_address,
            source=io.StringIO("data"),
            schema=self.mock_schema,
        )

        self.mock_client.create_dataset.assert_not_called()
        self.mock_client.load_table_from_file.assert_called()

    def test_export_query_results_to_cloud_storage_no_table(self) -> None:
        bucket = self.mock_project_id + "-bucket"
        self.mock_client.get_table.side_effect = exceptions.NotFound("!")
        with self.assertLogs(level="WARNING"):
            self.bq_client.export_query_results_to_cloud_storage(
                export_configs=[
                    ExportQueryConfig.from_view_query(
                        view=self.mock_view,
                        view_filter_clause="WHERE x = y",
                        intermediate_table_name="some_table_id",
                        output_uri=f"gs://{bucket}/view.json",
                        output_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
                    )
                ],
                print_header=True,
                use_query_cache=False,
            )

    def test_export_query_results_to_cloud_storage(self) -> None:
        """export_query_results_to_cloud_storage creates the table from the view query and
        exports the table."""
        bucket = self.mock_project_id + "-bucket"
        query_job: futures.Future = futures.Future()
        query_job.set_result([])
        extract_job = MagicMock(futures.Future)
        extract_job.result.return_value = None
        extract_job.destination_uris = [
            "gs://bucket/export-uri",
            "gs://bucket/export-uri-with-wildcard-*",
        ]
        extract_job.destination_uri_file_counts = [1, 5]
        self.mock_client.query.return_value = query_job
        self.mock_client.extract_table.return_value = extract_job
        results = self.bq_client.export_query_results_to_cloud_storage(
            export_configs=[
                ExportQueryConfig.from_view_query(
                    view=self.mock_view,
                    view_filter_clause="WHERE x = y",
                    intermediate_table_name=self.mock_table_id,
                    output_uri=f"gs://{bucket}/view.json",
                    output_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
                )
            ],
            print_header=True,
            use_query_cache=False,
        )
        self.mock_client.query.assert_called()
        self.mock_client.extract_table.assert_called()
        self.mock_client.delete_table.assert_called_with(
            bigquery.DatasetReference(
                self.mock_project_id, self.mock_view.dataset_id
            ).table(self.mock_table_id),
            not_found_ok=True,
        )

        self.assertEqual(len(results), 1)

        destinations = results[0][1]
        self.assertEqual(len(destinations), 6)
        outputted_uris = [destination.uri() for destination in destinations]
        self.assertEqual(
            outputted_uris,
            [
                "gs://bucket/export-uri",
                "gs://bucket/export-uri-with-wildcard-000000000000",
                "gs://bucket/export-uri-with-wildcard-000000000001",
                "gs://bucket/export-uri-with-wildcard-000000000002",
                "gs://bucket/export-uri-with-wildcard-000000000003",
                "gs://bucket/export-uri-with-wildcard-000000000004",
            ],
        )

    def test_export_query_results_to_cloud_storage_multiple_files_for_non_wildcard(
        self,
    ) -> None:
        """export_query_results_to_cloud_storage creates the table from the view query and
        exports the table."""
        bucket = self.mock_project_id + "-bucket"
        query_job: futures.Future = futures.Future()
        query_job.set_result([])
        extract_job = MagicMock(futures.Future)
        extract_job.result.return_value = None
        extract_job.destination_uris = [
            "gs://bucket/export-uri",
        ]
        # It shouldn't be possible to have multiple files for a non-wildcard uri, but we should handle it gracefully
        extract_job.destination_uri_file_counts = [5]
        self.mock_client.query.return_value = query_job
        self.mock_client.extract_table.return_value = extract_job

        with self.assertRaisesRegex(
            ValueError,
            "Expected a wildcard in the destination uri",
        ):
            self.bq_client.export_query_results_to_cloud_storage(
                export_configs=[
                    ExportQueryConfig.from_view_query(
                        view=self.mock_view,
                        view_filter_clause="WHERE x = y",
                        intermediate_table_name=self.mock_table_id,
                        output_uri=f"gs://{bucket}/view.json",
                        output_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
                    )
                ],
                print_header=True,
                use_query_cache=False,
            )
        self.mock_client.query.assert_called()
        self.mock_client.extract_table.assert_called()

        # Tests that we still do cleanup even though the extract failed.
        self.mock_client.delete_table.assert_called_with(
            bigquery.DatasetReference(
                self.mock_project_id, self.mock_view.dataset_id
            ).table(self.mock_table_id),
            not_found_ok=True,
        )

    def test_export_query_results_to_cloud_storage_extract_throws(self) -> None:
        """Tests that if export_query_results_to_cloud_storage fails at the extract
        step, we throw an error, but still do intermediate table cleanup.
        """
        bucket = self.mock_project_id + "-bucket"
        query_job: futures.Future = futures.Future()
        query_job.set_result([])
        extract_job: futures.Future = futures.Future()
        extract_job.set_exception(ValueError("Extract failed!!"))
        self.mock_client.query.return_value = query_job
        self.mock_client.extract_table.return_value = extract_job

        with self.assertRaisesRegex(ValueError, "Extract failed!!"):
            self.bq_client.export_query_results_to_cloud_storage(
                export_configs=[
                    ExportQueryConfig.from_view_query(
                        view=self.mock_view,
                        view_filter_clause="WHERE x = y",
                        intermediate_table_name=self.mock_table_id,
                        output_uri=f"gs://{bucket}/view.json",
                        output_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
                    )
                ],
                print_header=True,
                use_query_cache=False,
            )
        self.mock_client.query.assert_called()
        self.mock_client.extract_table.assert_called()

        # Tests that we still do cleanup even though the extract failed.
        self.mock_client.delete_table.assert_called_with(
            bigquery.DatasetReference(
                self.mock_project_id, self.mock_view.dataset_id
            ).table(self.mock_table_id),
            not_found_ok=True,
        )

    def test_create_table_from_query_async(self) -> None:
        """Tests that the create_table_from_query function calls the function to create a table from a query."""
        self.bq_client.create_table_from_query_async(
            address=self.mock_table_address,
            query="SELECT * FROM some.fake.table",
            query_parameters=[],
            use_query_cache=False,
        )
        self.mock_client.query.assert_called()

    def test_create_table_from_query(self) -> None:
        """Tests that the create_table_from_query function calls the function to create a table from a query."""
        self.bq_client.create_table_from_query(
            address=self.mock_table_address,
            query="SELECT * FROM some.fake.table",
            query_parameters=[],
            use_query_cache=False,
        )
        self.mock_client.query.assert_called()

    def test_create_table_from_query_async_with_clustering_fields(
        self,
    ) -> None:
        """
        Tests that create_table_from_query_async() handles BigQueryViews that
        include clustering_fields in the config passed to client.query().
        """
        fake_query = "SELECT NULL LIMIT 0"
        fake_cluster_fields = ["clustering_field_1", "clustering_field_2"]
        self.bq_client.create_table_from_query_async(
            address=BigQueryAddress(
                dataset_id=self.mock_dataset_id,
                table_id="fake_table_temp",
            ),
            query=fake_query,
            clustering_fields=fake_cluster_fields,
            use_query_cache=False,
            overwrite=True,
        )

        # get inputs passed to client.query()
        _, inputs = self.mock_client.query.call_args
        # verify that job_config contains the correct clustering fields
        self.assertEqual(inputs["job_config"].clustering_fields, fake_cluster_fields)

    def test_create_table_from_query_async_with_output_schema_no_overwrite_mismatched_schemas_should_fail(
        self,
    ) -> None:
        """
        Tests that create_table_from_query_async() will throw an error if the provided output schema does not match the existing schema when overwrite is False.
        """
        mock_table = create_autospec(bigquery.Table)
        mock_table.clustering_fields = None
        mock_table.time_partitioning = None
        # Set the existing schema to be different from the provided schema
        mock_table.schema = [
            bigquery.SchemaField(
                name="fake_column",
                field_type="STRING",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name="fake_column_2",
                field_type="STRING",
                mode="REQUIRED",
            ),
        ]
        self.mock_client.get_table.return_value = mock_table

        fake_query = "SELECT NULL LIMIT 0"

        with self.assertRaisesRegex(
            ValueError, "Found updated table schema for destination table"
        ):
            self.bq_client.create_table_from_query_async(
                address=BigQueryAddress(
                    dataset_id=self.mock_dataset_id,
                    table_id="fake_table_temp",
                ),
                query=fake_query,
                use_query_cache=False,
                overwrite=False,
                output_schema=[
                    bigquery.SchemaField(
                        name="fake_column",
                        field_type="STRING",
                        mode="NULLABLE",
                    )
                ],
            )

    def test_create_table_from_query_async_with_output_schema_no_overwrite_matching_schemas(
        self,
    ) -> None:
        """
        Tests that create_table_from_query_async() works if the provided output schema does match the existing schema when overwrite is False.
        """
        mock_table = create_autospec(bigquery.Table)
        mock_table.clustering_fields = None
        mock_table.time_partitioning = None
        # Set the existing schema to be the same as the provided schema
        mock_table.schema = [
            bigquery.SchemaField(
                name="fake_column",
                field_type="STRING",
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name="fake_column_2",
                field_type="STRING",
                mode="REQUIRED",
            ),
        ]
        self.mock_client.get_table.return_value = mock_table

        fake_query = "SELECT NULL LIMIT 0"

        self.bq_client.create_table_from_query_async(
            address=BigQueryAddress(
                dataset_id=self.mock_dataset_id,
                table_id="fake_table_temp",
            ),
            query=fake_query,
            use_query_cache=False,
            overwrite=False,
            output_schema=[
                # Put these out of order to test that the order doesn't matter
                bigquery.SchemaField(
                    name="fake_column_2",
                    field_type="STRING",
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    name="fake_column",
                    field_type="STRING",
                    mode="REQUIRED",
                ),
            ],
        )

        self.mock_client.query.assert_called_once()

    def test_insert_into_table_with_clustering_fails_without_write_truncate(
        self,
    ) -> None:
        """
        Tests that insert_into_table_from_query_async() fails with BigQueryViews that
        include new clustering_fields without WRITE_TRUNCATE permissions.
        """
        fake_query = "SELECT NULL LIMIT 0"
        fake_cluster_fields = ["clustering_field_1", "clustering_field_2"]

        # verify ValueError thrown
        with self.assertRaisesRegex(
            ValueError,
            "Trying to materialize into a table using different clustering fields than what currently exists requires 'WRITE_TRUNCATE' write_disposition.",
        ):
            self.bq_client.insert_into_table_from_query_async(
                destination_address=BigQueryAddress(
                    dataset_id=self.mock_dataset_id,
                    table_id="fake_table_temp",
                ),
                query=fake_query,
                clustering_fields=fake_cluster_fields,
                use_query_cache=False,
            )

    @patch("google.cloud.bigquery.job.QueryJobConfig")
    def test_insert_into_table_from_table_async(
        self, mock_job_config: MagicMock
    ) -> None:
        """Tests that the insert_into_table_from_table_async function runs a query."""
        mock_table = create_autospec(bigquery.Table)
        mock_table.schema = self.mock_schema
        self.mock_client.get_table.return_value = mock_table

        self.bq_client.insert_into_table_from_table_async(
            source_address=self.mock_table_address,
            destination_address=BigQueryAddress(
                dataset_id=self.mock_dataset_id,
                table_id="fake_table_temp",
            ),
            use_query_cache=False,
        )
        expected_query = f"INSERT INTO `fake-recidiviz-project.{self.mock_dataset_id}.fake_table_temp` (fake_column) \nSELECT fake_column \nFROM `fake-recidiviz-project.{self.mock_dataset_id}.{self.mock_table_id}`"
        self.mock_client.get_table.assert_called()
        self.mock_client.query.assert_called_with(
            query=expected_query,
            location=BigQueryClient.DEFAULT_REGION,
            job_config=mock_job_config(),
        )

    @patch("google.cloud.bigquery.job.QueryJobConfig")
    def test_insert_into_table_from_table_async_source_column_mapping(
        self, mock_job_config: MagicMock
    ) -> None:
        """Tests that the insert_into_table_from_table_async function runs a query
        correctly mapping source columns to destination columns."""
        src_mapping = {"fake_column": "new_fake_column"}
        mock_table_source = create_autospec(bigquery.Table)
        mock_table_source.schema = [
            # Should include column src and dst have in common according to mapping
            bigquery.SchemaField(
                mode="NULLABLE", field_type="STRING", name="fake_column"
            ),
            # Should include column src and dst have in common
            bigquery.SchemaField(
                mode="NULLABLE", field_type="STRING", name="fake_column_A"
            ),
            # Should ignore column src and dst don't have in common and is not in mapping
            bigquery.SchemaField(
                mode="NULLABLE", field_type="STRING", name="fake_column_B"
            ),
        ]
        mock_table_dest = create_autospec(bigquery.Table)
        mock_table_dest.schema = [
            bigquery.SchemaField(
                mode="NULLABLE", field_type="STRING", name="fake_column_A"
            ),
            bigquery.SchemaField(
                mode="NULLABLE", field_type="STRING", name="new_fake_column"
            ),
            # Should ignore column src and dst don't have in common and is not in mapping
            bigquery.SchemaField(
                mode="NULLABLE", field_type="STRING", name="fake_column_C"
            ),
        ]

        def _get_table(_table_ref: bigquery.TableReference) -> bigquery.Table:
            if _table_ref.table_id == "fake_table_temp":
                return mock_table_dest
            if _table_ref.table_id == self.mock_table_id:
                return mock_table_source
            raise ValueError("Unrecognized table!")

        self.mock_client.get_table.side_effect = _get_table

        self.bq_client.insert_into_table_from_table_async(
            source_address=self.mock_table_address,
            destination_address=BigQueryAddress(
                dataset_id=self.mock_dataset_id,
                table_id="fake_table_temp",
            ),
            use_query_cache=False,
            source_to_destination_column_mapping=src_mapping,
        )
        expected_query = f"INSERT INTO `fake-recidiviz-project.{self.mock_dataset_id}.fake_table_temp` (fake_column_A, new_fake_column) \nSELECT fake_column_A, fake_column \nFROM `fake-recidiviz-project.{self.mock_dataset_id}.{self.mock_table_id}`"
        self.mock_client.get_table.assert_called()
        self.mock_client.query.assert_called_with(
            query=expected_query,
            location=BigQueryClient.DEFAULT_REGION,
            job_config=mock_job_config(),
        )

    @patch("google.cloud.bigquery.job.QueryJobConfig")
    def test_insert_into_table_from_table_async_invalid_source_column_mapping(
        self, mock_job_config: MagicMock
    ) -> None:
        """Tests that the insert_into_table_from_table_async throws a ValueError when
        trying to provide a mapping for a column that does not exist."""
        mock_table = create_autospec(bigquery.Table)
        mock_table.schema = self.mock_schema
        self.mock_client.get_table.return_value = mock_table

        with self.assertRaises(ValueError) as context:
            self.bq_client.insert_into_table_from_table_async(
                source_address=self.mock_table_address,
                destination_address=BigQueryAddress(
                    dataset_id=self.mock_dataset_id,
                    table_id="fake_table_temp",
                ),
                use_query_cache=False,
                source_to_destination_column_mapping={
                    "doesnt_exist_column": "new_fake_column"
                },
            )
        self.assertEqual(
            "Mapping in source_to_destination_column_mapping contains invalid columns:\n"
            "Source columns missing from source table: [doesnt_exist_column]\n"
            "Source columns: [fake_column]\n"
            "Destination columns missing from destination table: [new_fake_column]\n"
            "Destination columns: [fake_column]\n",
            str(context.exception),
        )

    @patch("google.cloud.bigquery.job.QueryJobConfig")
    def test_insert_into_table_from_table_async_source_has_too_many_cols(
        self, mock_job_config: MagicMock
    ) -> None:
        """Tests that the insert_into_table_from_table_async function runs a query."""
        mock_table_source = create_autospec(bigquery.Table)
        mock_table_source.schema = self.mock_schema
        mock_table_dest = create_autospec(bigquery.Table)
        mock_table_dest.schema = [
            bigquery.SchemaField(
                mode="NULLABLE", field_type="STRING", name="fake_column"
            ),
            bigquery.SchemaField(
                name="new_schema_field", field_type="STRING", mode="REQUIRED"
            ),
            bigquery.SchemaField(
                name="partition_field", field_type="DATETIME", mode="REQUIRED"
            ),
        ]

        def _get_table(_table_ref: bigquery.TableReference) -> bigquery.Table:
            if _table_ref.table_id == "fake_table_temp":
                return mock_table_dest
            if _table_ref.table_id == self.mock_table_id:
                return mock_table_source
            raise ValueError("Unrecognized table!")

        self.mock_client.get_table.side_effect = _get_table

        self.bq_client.insert_into_table_from_table_async(
            source_address=self.mock_table_address,
            destination_address=BigQueryAddress(
                dataset_id=self.mock_dataset_id,
                table_id="fake_table_temp",
            ),
            use_query_cache=False,
        )
        expected_query = f"INSERT INTO `fake-recidiviz-project.{self.mock_dataset_id}.fake_table_temp` (fake_column) \nSELECT fake_column \nFROM `fake-recidiviz-project.{self.mock_dataset_id}.{self.mock_table_id}`"
        self.mock_client.get_table.assert_called()
        self.mock_client.query.assert_called_with(
            query=expected_query,
            location=BigQueryClient.DEFAULT_REGION,
            job_config=mock_job_config(),
        )

    @patch("google.cloud.bigquery.job.QueryJobConfig")
    def test_insert_into_table_from_table_different_datasets_async(
        self, mock_job_config: MagicMock
    ) -> None:
        """Tests that the insert_into_table_from_table_async function runs a query with the correct source and
        destination datasets."""
        mock_table = create_autospec(bigquery.Table)
        mock_table.schema = self.mock_schema
        self.mock_client.get_table.return_value = mock_table

        self.bq_client.insert_into_table_from_table_async(
            source_address=BigQueryAddress(
                dataset_id="mock-source-dataset",
                table_id="mock_source_table",
            ),
            destination_address=BigQueryAddress(
                dataset_id="mock-destination-dataset",
                table_id="mock_destination_table",
            ),
            use_query_cache=False,
        )
        expected_query = "INSERT INTO `fake-recidiviz-project.mock-destination-dataset.mock_destination_table` (fake_column) \nSELECT fake_column \nFROM `fake-recidiviz-project.mock-source-dataset.mock_source_table`"
        self.mock_client.get_table.assert_called()
        self.mock_client.query.assert_called_with(
            query=expected_query,
            location=BigQueryClient.DEFAULT_REGION,
            job_config=mock_job_config(),
        )

    def test_insert_into_table_from_table_invalid_destination(self) -> None:
        """Tests that the insert_into_table_from_table_async function does not run the query if the destination
        table does not exist."""
        self.mock_client.get_table.side_effect = exceptions.NotFound("!")

        with self.assertRaisesRegex(
            ValueError,
            r"Destination table \[fake-recidiviz-project.fake_source_dataset_id.fake_table_id\] does not exist",
        ):
            self.bq_client.insert_into_table_from_table_async(
                source_address=self.mock_table_address,
                destination_address=BigQueryAddress(
                    dataset_id="fake_source_dataset_id",
                    table_id="fake_table_id",
                ),
                use_query_cache=False,
            )
        self.mock_client.get_table.assert_called()
        self.mock_client.query.assert_not_called()

    def test_insert_into_table_from_table_invalid_filter_clause(self) -> None:
        """Tests that the insert_into_table_from_table_async function does not run the query if the filter clause
        does not start with a WHERE."""
        with self.assertRaisesRegex(
            ValueError,
            r"Found filter clause \[bad filter clause\] that does not begin with WHERE",
        ):
            self.bq_client.insert_into_table_from_table_async(
                source_address=self.mock_table_address,
                destination_address=BigQueryAddress(
                    dataset_id="fake_source_dataset_id",
                    table_id="fake_table_id",
                ),
                source_data_filter_clause="bad filter clause",
                use_query_cache=False,
            )
        self.mock_client.query.assert_not_called()

    @patch("google.cloud.bigquery.job.QueryJobConfig")
    def test_insert_into_table_from_table_with_filter_clause(
        self, mock_job_config: MagicMock
    ) -> None:
        """Tests that the insert_into_table_from_table_async generates a valid query when given a filter clause."""
        mock_table = create_autospec(bigquery.Table)
        mock_table.schema = self.mock_schema
        self.mock_client.get_table.return_value = mock_table
        filter_clause = "WHERE fake_column IN ('US_ND')"
        job_config = mock_job_config()
        self.bq_client.insert_into_table_from_table_async(
            source_address=self.mock_table_address,
            destination_address=BigQueryAddress(
                dataset_id="fake_source_dataset_id",
                table_id="fake_table_id",
            ),
            source_data_filter_clause=filter_clause,
            use_query_cache=False,
        )
        expected_query = (
            f"INSERT INTO `fake-recidiviz-project.fake_source_dataset_id.fake_table_id` (fake_column) \nSELECT fake_column \nFROM `fake-recidiviz-project.{self.mock_dataset_id}.{self.mock_table_id}` "
            "\n WHERE fake_column IN ('US_ND')"
        )
        self.mock_client.query.assert_called_with(
            query=expected_query,
            location=BigQueryClient.DEFAULT_REGION,
            job_config=job_config,
        )

    def test_load_into_table_from_cloud_storage(self) -> None:
        self.mock_client.get_dataset.side_effect = exceptions.NotFound("!")

        self.bq_client.load_table_from_cloud_storage(
            destination_address=self.mock_table_address,
            destination_table_schema=[SchemaField("my_column", "STRING", "NULLABLE")],
            source_uris=["gs://bucket/export-uri"],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        self.mock_client.create_dataset.assert_called()
        self.mock_client.load_table_from_uri.assert_called()

    def test_stream_into_table(self) -> None:
        self.mock_client.insert_rows.return_value = None

        self.bq_client.stream_into_table(
            address=self.mock_table_address,
            rows=[{"a": 1, "b": "foo"}, {"a": 2, "b": "bar"}],
        )

        self.mock_client.get_table.assert_called()
        self.mock_client.insert_rows.assert_called()

    def test_stream_into_table_invalid_table(self) -> None:
        error_message = "TEST - Something went wrong"
        self.mock_client.get_table.side_effect = ValueError(error_message)

        with self.assertRaisesRegex(ValueError, error_message):
            self.bq_client.stream_into_table(
                address=self.mock_table_address,
                rows=[{"a": 1, "b": "foo"}, {"a": 2, "b": "bar"}],
            )

        self.mock_client.get_table.assert_called()
        self.mock_client.insert_rows.assert_not_called()

    def test_stream_into_table_failed_insert(self) -> None:
        self.mock_client.insert_rows.return_value = [
            {"index": 1, "errors": "Incorrect columns"}
        ]

        with self.assertRaisesRegex(RuntimeError, "Incorrect columns"):
            self.bq_client.stream_into_table(
                address=self.mock_table_address,
                rows=[{"a": 1, "b": "foo"}, {"a": 2, "b": "bar"}],
            )

        self.mock_client.get_table.assert_called()
        self.mock_client.insert_rows.assert_called()

    def test_load_into_table_async(self) -> None:
        self.bq_client.load_into_table_async(
            address=self.mock_table_address,
            rows=[{"a": 1, "b": "foo"}, {"a": 2, "b": "bar"}],
        )

        self.mock_client.get_table.assert_called()
        self.mock_client.load_table_from_json.assert_called()

    def test_load_into_table_async_invalid_table(self) -> None:
        error_message = "TEST - Something went wrong"
        self.mock_client.get_table.side_effect = ValueError(error_message)

        with self.assertRaisesRegex(ValueError, error_message):
            self.bq_client.load_into_table_async(
                address=self.mock_table_address,
                rows=[{"a": 1, "b": "foo"}, {"a": 2, "b": "bar"}],
            )

        self.mock_client.get_table.assert_called()
        self.mock_client.load_table_from_json.assert_not_called()

    @patch("google.cloud.bigquery.QueryJobConfig")
    def test_delete_from_table(self, mock_job_config: MagicMock) -> None:
        """Tests that the delete_from_table function runs a query."""
        self.bq_client.delete_from_table_async(
            self.mock_table_address, filter_clause="WHERE x > y"
        )
        expected_query = (
            "DELETE FROM `fake-recidiviz-project.fake-dataset.test_table` WHERE x > y"
        )

        mock_job_config.assert_has_calls(
            [
                call(
                    use_query_cache=False,
                    labels={
                        "dataset_id": "fake-dataset",
                        "table_id": "test_table",
                        "big_query_address": "fake-dataset---test_table",
                        "state_code": "state_agnostic",
                        "platform_environment": "local_machine",
                    },
                )
            ]
        )

        self.mock_client.query.assert_called_with(
            query=expected_query,
            location="US",
            job_config=mock_job_config(),
            timeout=None,
        )

    @patch("google.cloud.bigquery.QueryJobConfig")
    def test_delete_from_table_no_filter(self, mock_job_config: MagicMock) -> None:
        """Tests that the delete_from_table function runs a query without a filter."""
        self.bq_client.delete_from_table_async(self.mock_table_address)
        expected_query = (
            "DELETE FROM `fake-recidiviz-project.fake-dataset.test_table` WHERE true"
        )

        mock_job_config.assert_has_calls(
            [
                call(
                    use_query_cache=False,
                    labels={
                        "dataset_id": "fake-dataset",
                        "table_id": "test_table",
                        "big_query_address": "fake-dataset---test_table",
                        "state_code": "state_agnostic",
                        "platform_environment": "local_machine",
                    },
                )
            ]
        )

        self.mock_client.query.assert_called_with(
            query=expected_query,
            location="US",
            job_config=mock_job_config(),
            timeout=None,
        )

    def test_delete_from_table_invalid_filter_clause(self) -> None:
        """Tests that the delete_from_table function does not run a query when the filter clause is invalid."""
        with self.assertRaisesRegex(
            ValueError,
            "Cannot delete from a table without a valid filter clause starting with WHERE.",
        ):
            self.bq_client.delete_from_table_async(
                self.mock_table_address, filter_clause="x > y"
            )
        self.mock_client.query.assert_not_called()

    def test_materialize_view_to_table(self) -> None:
        """Tests that the materialize_view_to_table function calls the function to create a table from a query."""
        mock_table = create_autospec(bigquery.Table)
        self.mock_client.get_table.return_value = mock_table

        self.bq_client.materialize_view_to_table(
            view=self.mock_view, use_query_cache=False, view_configuration_changed=True
        )

        expected_job_config_matcher = MaterializeTableJobConfigMatcher(
            expected_destination="fake-recidiviz-project.fake-dataset.test_view_materialized"
        )
        self.mock_client.query.assert_called_with(
            query="SELECT * FROM `fake-recidiviz-project.fake-dataset.test_view`",
            location=BigQueryClient.DEFAULT_REGION,
            job_config=expected_job_config_matcher,
        )
        self.mock_client.get_table.assert_called_with(
            bigquery.TableReference(
                bigquery.DatasetReference("fake-recidiviz-project", "fake-dataset"),
                "test_view_materialized",
            )
        )
        self.mock_client.update_table.assert_called_with(
            mock_table, ["description"], retry=big_query_client.UPDATE_DESCRIPTION_RETRY
        )
        self.assertEqual(
            mock_table.description,
            "Materialized data from view [fake-dataset.test_view]. "
            "View description:\ntest_view description\nExplore this view's lineage at https://go/lineage-staging/fake-dataset.test_view",
        )

    def test_materialize_view_to_table_materialized_address_override(self) -> None:
        """Tests that the materialize_view_to_table function properly calls the function
        to create a table from a query, even when the view is configured to materialize
        in a custom location.
        """
        mock_table = create_autospec(bigquery.Table)
        self.mock_client.get_table.return_value = mock_table

        mock_view = SimpleBigQueryViewBuilder(
            dataset_id="dataset",
            view_id="test_view",
            description="test_view description",
            view_query_template="SELECT NULL LIMIT 0",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="custom_dataset", table_id="custom_view"
            ),
        ).build()

        self.bq_client.materialize_view_to_table(
            view=mock_view, use_query_cache=False, view_configuration_changed=True
        )

        expected_job_config_matcher = MaterializeTableJobConfigMatcher(
            expected_destination="fake-recidiviz-project.custom_dataset.custom_view"
        )
        self.mock_client.query.assert_called_with(
            query="SELECT * FROM `fake-recidiviz-project.dataset.test_view`",
            location=BigQueryClient.DEFAULT_REGION,
            job_config=expected_job_config_matcher,
        )
        self.mock_client.get_table.assert_called_with(
            bigquery.TableReference(
                bigquery.DatasetReference("fake-recidiviz-project", "custom_dataset"),
                "custom_view",
            )
        )
        self.mock_client.update_table.assert_called_with(
            mock_table, ["description"], retry=big_query_client.UPDATE_DESCRIPTION_RETRY
        )

    def test_materialize_view_to_table_no_materialized_address(self) -> None:
        """Tests that the materialize_view_to_table function does not call the function to create a table from a
        query if there is no set materialized_address on the view."""
        invalid_view = SimpleBigQueryViewBuilder(
            dataset_id="dataset",
            view_id="test_view",
            description="test_view description",
            view_query_template="SELECT NULL LIMIT 0",
            should_materialize=False,
        ).build()

        with self.assertRaisesRegex(
            ValueError,
            "Trying to materialize a view that does not have a set materialized_address.",
        ):
            self.bq_client.materialize_view_to_table(
                view=invalid_view,
                use_query_cache=False,
                view_configuration_changed=True,
            )
        self.mock_client.query.assert_not_called()

    def test_materialize_view_to_table_with_schema(
        self,
    ) -> None:
        """
        Tests that materialize_view_to_table() handles BigQueryViews that
        include output_schema in the config passed to client.query().
        """
        mock_table = create_autospec(bigquery.Table)
        fake_schema = [
            String(name="field1", description="Field 1", mode="REQUIRED"),
            String(name="field2", description="Field 2", mode="REQUIRED"),
        ]

        self.mock_view = SimpleBigQueryViewBuilder(
            dataset_id=self.mock_dataset_id,
            view_id="test_view",
            description="test_view description",
            view_query_template="SELECT NULL LIMIT 0",
            should_materialize=True,
            schema=fake_schema,
        ).build()

        # Pretend like the table doesn't exist so create_table_with_schema doesn't error
        self.mock_client.get_table.side_effect = exceptions.NotFound("Not found")

        # Once it's created, make it so it does exist
        def create_side_effect(*args: Any, **kwargs: Any) -> None:
            self.mock_client.get_table.side_effect = mock_table
            return mock_table

        self.mock_client.create_table.side_effect = create_side_effect

        self.bq_client.materialize_view_to_table(
            view=self.mock_view, use_query_cache=False, view_configuration_changed=True
        )

        # Should have deleted the table, recreated it, and then run the query with WRITE_APPEND
        self.mock_client.delete_table.assert_called_once()
        self.mock_client.create_table.assert_called_once()

        # get inputs passed to client.query()
        _, inputs = self.mock_client.query.call_args
        # verify that job_config contains the correct write_disposition
        self.assertEqual(
            inputs["job_config"].write_disposition,
            bigquery.WriteDisposition.WRITE_APPEND,
        )

    def test_create_table_with_schema(self) -> None:
        """Tests that the create_table_with_schema function calls the create_table function on the client."""
        self.mock_client.get_table.side_effect = exceptions.NotFound("!")
        schema_fields = [bigquery.SchemaField("new_schema_field", "STRING")]

        self.bq_client.create_table_with_schema(
            address=self.mock_table_address, schema_fields=schema_fields
        )
        self.mock_client.create_table.assert_called_once()
        table = self.mock_client.create_table.mock_calls[0].args[0]
        self.assertIsInstance(table, bigquery.Table)
        self.assertEqual(None, table.time_partitioning)

    def test_create_table_with_schema_empty_clustering_fields(self) -> None:
        """Tests that the create_table_with_schema function calls the create_table function on the client."""
        self.mock_client.get_table.side_effect = exceptions.NotFound("!")
        schema_fields = [bigquery.SchemaField("new_schema_field", "STRING")]

        self.bq_client.create_table_with_schema(
            address=self.mock_table_address,
            schema_fields=schema_fields,
            clustering_fields=[],
        )
        self.mock_client.create_table.assert_called_once()
        table = self.mock_client.create_table.mock_calls[0].args[0]
        self.assertIsInstance(table, bigquery.Table)
        self.assertEqual(None, table.clustering_fields)

    def test_create_table_with_schema_table_exists(self) -> None:
        """Tests that the create_table_with_schema function raises an error when the table already exists."""
        self.mock_client.get_table.side_effect = None
        schema_fields = [bigquery.SchemaField("new_schema_field", "STRING")]

        with self.assertRaisesRegex(
            ValueError,
            "Trying to create a table that already exists: fake-dataset.test_table.",
        ):
            self.bq_client.create_table_with_schema(
                address=self.mock_table_address, schema_fields=schema_fields
            )
        self.mock_client.create_table.assert_not_called()

    def test_create_table_with_schema_partition(self) -> None:
        """Tests that the create_table_with_schema function calls the create_table
        function on the client and properly adds a partition if one is specified."""
        self.mock_client.get_table.side_effect = exceptions.NotFound("!")
        schema_fields = [
            bigquery.SchemaField("new_schema_field", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("partition_field", "DATETIME", mode="REQUIRED"),
        ]

        self.bq_client.create_table_with_schema(
            address=self.mock_table_address,
            schema_fields=schema_fields,
            time_partitioning=bigquery.TimePartitioning(
                field="partition_field", type_=bigquery.TimePartitioningType.DAY
            ),
        )
        self.mock_client.create_table.assert_called_once()
        table = self.mock_client.create_table.mock_calls[0].args[0]
        self.assertIsInstance(table, bigquery.Table)
        self.assertEqual(
            bigquery.TimePartitioning(
                field="partition_field", type_=bigquery.TimePartitioningType.DAY
            ),
            table.time_partitioning,
        )

    def test_create_table_with_schema_no_partition_require_partition_fields(
        self,
    ) -> None:
        """Tests that the create_table_with_schema function calls the create_table function on the client."""
        self.mock_client.get_table.side_effect = exceptions.NotFound("!")
        schema_fields = [bigquery.SchemaField("new_schema_field", "STRING")]

        with self.assertRaisesRegex(
            ValueError,
            "Cannot require a partition filter on a table that is not partitioned.",
        ):
            self.bq_client.create_table_with_schema(
                address=self.mock_table_address,
                schema_fields=schema_fields,
                require_partition_filter=True,
            )

    def test_update_schema_only_field_updates_or_additions(self) -> None:
        """Tests that update_schema() updates with appropriate schema when there are
        valid updates to make.
        """
        schema_fields = [
            bigquery.SchemaField("field_1", "STRING"),
            bigquery.SchemaField("field_2", "STRING"),
            bigquery.SchemaField("field_3", "STRING", description="Old description"),
        ]
        table = bigquery.Table(self.mock_table, schema_fields)
        self.mock_client.get_table.return_value = table

        new_schema_fields = [
            bigquery.SchemaField("field_1", "STRING"),
            bigquery.SchemaField("field_2", "STRING", description="New description"),
            bigquery.SchemaField(
                "field_3", "STRING", description="Updated description"
            ),
        ]

        self.bq_client.update_schema(
            address=self.mock_table_address,
            desired_schema_fields=new_schema_fields,
            allow_field_deletions=True,
        )

        # We should only have to get the table once to update it
        self.mock_client.get_table.assert_called_once()
        self.mock_client.get_table.assert_called_with(self.mock_table)
        # We did not remove any fields
        self.mock_client.query.assert_not_called()

        self.mock_client.update_table.assert_called_once()
        self.mock_client.update_table.assert_called_with(
            bigquery.Table(self.mock_table, new_schema_fields), ["schema"]
        )
        # For some reason, the above equality does not check schema equality properly,
        # so we check it explicitly.
        self.assertEqual(
            new_schema_fields,
            self.mock_client.update_table.mock_calls[0].args[0].schema,
        )

    def test_update_schema_no_update(self) -> None:
        """Tests that update_schema() does not do any updates when the schema hasn't
        changed.
        """
        schema_fields = [
            bigquery.SchemaField("field_1", "STRING"),
            bigquery.SchemaField("field_2", "STRING"),
            bigquery.SchemaField("field_3", "STRING", description="Description"),
        ]
        table = bigquery.Table(self.mock_table, schema_fields)
        self.mock_client.get_table.return_value = table

        new_schema_fields = [
            bigquery.SchemaField("field_1", "STRING"),
            bigquery.SchemaField("field_3", "STRING", description="Description"),
            bigquery.SchemaField("field_2", "STRING"),
        ]

        self.bq_client.update_schema(
            address=self.mock_table_address,
            desired_schema_fields=new_schema_fields,
            allow_field_deletions=True,
        )

        # We should only have to get the table once to update it
        self.mock_client.get_table.assert_called_once()
        self.mock_client.get_table.assert_called_with(self.mock_table)

        # No other work to remove or add fields is done
        self.mock_client.query.assert_not_called()
        self.mock_client.update_table.assert_not_called()

    def test_update_removed_and_added(self) -> None:
        """Tests that update_schema() correctly removes and updates fields."""
        schema_fields = [
            bigquery.SchemaField("field_1", "STRING"),
            bigquery.SchemaField("field_2", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("field_3", "STRING"),
            bigquery.SchemaField("field_4", "STRING", description="Old description"),
        ]
        new_schema_fields = [
            bigquery.SchemaField("field_1", "STRING"),
            bigquery.SchemaField("field_4", "STRING", description="New description"),
        ]
        fields_removed: set[str] = set()

        def mock_get_table(ref: bigquery.TableReference) -> bigquery.Table:
            if ref != self.mock_table:
                raise ValueError(f"Unexpected table [{ref}]")
            if not fields_removed:
                return bigquery.Table(self.mock_table, schema_fields)

            return bigquery.Table(
                self.mock_table,
                [c for c in schema_fields if c.name not in fields_removed],
            )

        self.mock_client.get_table.side_effect = mock_get_table

        class FakeQueryJob:
            def result(self) -> None:
                pass

        def mock_query(query: str, **kwargs: Any) -> FakeQueryJob:
            self.assertTrue("DROP COLUMN field_2" in query)
            fields_removed.add("field_2")
            self.assertTrue("DROP COLUMN field_3" in query)
            fields_removed.add("field_3")
            return FakeQueryJob()

        self.mock_client.query.side_effect = mock_query

        self.bq_client.update_schema(
            address=self.mock_table_address,
            desired_schema_fields=new_schema_fields,
            allow_field_deletions=True,
        )

        # We call get_table() twice - once at the beginning and once after we have
        # removed fields.
        self.mock_client.get_table.assert_has_calls(
            [call(self.mock_table), call(self.mock_table)]
        )

        # Query called to remove field_2
        self.mock_client.query.assert_called_once()

        # Update called to add/update new fields
        self.mock_client.update_table.assert_called_once()
        self.mock_client.update_table.assert_called_with(
            bigquery.Table(self.mock_table, new_schema_fields), ["schema"]
        )
        # For some reason, the above equality does not check schema equality properly,
        # so we check it explicitly.
        self.assertEqual(
            new_schema_fields,
            self.mock_client.update_table.mock_calls[0].args[0].schema,
        )

    def test_update_schema_fails_on_changed_type(self) -> None:
        """Tests that update_schema() throws if we try to change a field type."""
        schema_fields = [
            bigquery.SchemaField("field_1", "STRING"),
            bigquery.SchemaField("field_2", "STRING"),
        ]
        table = bigquery.Table(self.mock_table, schema_fields)
        self.mock_client.get_table.return_value = table

        new_schema_fields = [
            bigquery.SchemaField("field_1", "STRING"),
            bigquery.SchemaField("field_2", "INT"),
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Trying to change the field type of an existing field in "
            "fake-dataset.test_table. Existing field field_2 has type STRING. "
            "Cannot change this type to INT.",
        ):
            self.bq_client.update_schema(
                address=self.mock_table_address,
                desired_schema_fields=new_schema_fields,
                allow_field_deletions=True,
            )

        self.mock_client.get_table.assert_called_once()
        self.mock_client.get_table.assert_called_with(self.mock_table)

        # No other work to remove or add fields is done
        self.mock_client.query.assert_not_called()
        self.mock_client.update_table.assert_not_called()

    def test_update_schema_fails_on_changed_mode(self) -> None:
        """Tests that update_schema() throws if we try to change a field mode."""
        schema_fields = [
            bigquery.SchemaField("field_1", "STRING", "NULLABLE"),
            bigquery.SchemaField("field_2", "STRING"),
        ]
        table = bigquery.Table(self.mock_table, schema_fields)
        self.mock_client.get_table.return_value = table

        new_schema_fields = [
            bigquery.SchemaField("field_1", "STRING", "REQUIRED"),
            bigquery.SchemaField("field_2", "INT"),
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"Cannot change the mode of field SchemaField\('field_1'.*\) to REQUIRED",
        ):
            self.bq_client.update_schema(
                address=self.mock_table_address,
                desired_schema_fields=new_schema_fields,
                allow_field_deletions=True,
            )

        self.mock_client.get_table.assert_called_once()
        self.mock_client.get_table.assert_called_with(self.mock_table)

        # No other work to remove or add fields is done
        self.mock_client.query.assert_not_called()
        self.mock_client.update_table.assert_not_called()

    def test_update_schema_fails_on_no_allowed_deletions(self) -> None:
        """Tests that update_schema() throws if we are attempting to delete a column
        when no_allowed_deletions is False.
        """
        schema_fields = [
            bigquery.SchemaField("field_1", "STRING"),
            bigquery.SchemaField("field_2", "STRING"),
        ]
        table = bigquery.Table(self.mock_table, schema_fields)
        self.mock_client.get_table.return_value = table

        new_schema_fields = [
            bigquery.SchemaField("field_1", "STRING"),
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Found deprecated fields .*field_2.* for table: fake-dataset.test_table "
            "but field deletions is not allowed.",
        ):
            self.bq_client.update_schema(
                address=self.mock_table_address,
                desired_schema_fields=new_schema_fields,
                allow_field_deletions=False,
            )

        self.mock_client.get_table.assert_called_once()
        self.mock_client.get_table.assert_called_with(self.mock_table)

        # No other work to remove or add fields is done
        self.mock_client.query.assert_not_called()
        self.mock_client.update_table.assert_not_called()

    def test_update_schema_fails_on_duplicate_column(self) -> None:
        """Tests that update_schema() throws if we are attempting to add two columns
        with the same name.
        """
        new_schema_fields = [
            bigquery.SchemaField("field_1", "STRING"),
            bigquery.SchemaField("field_2", "STRING"),
            # This column has a duplicate name and should throw
            bigquery.SchemaField("field_1", "INT"),
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"Found multiple columns with name \[field_1\] in new schema for table "
            r"\[fake-dataset.test_table\].",
        ):
            self.bq_client.update_schema(
                address=self.mock_table_address,
                desired_schema_fields=new_schema_fields,
                allow_field_deletions=False,
            )

        self.mock_client.get_table.assert_not_called()

        # No other work to remove or add fields is done
        self.mock_client.query.assert_not_called()
        self.mock_client.update_table.assert_not_called()

    def test_update_schema_fails_on_long_description(self) -> None:
        """Tests that update_schema() throws if we are attempting to add a column
        with a description that goes above the max allowed limit.
        """
        new_schema_fields = [
            bigquery.SchemaField("field_1", "STRING", description="a" * 1025),
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"Attempting to set description for field \[field_1\] on table "
            r"\[fake-dataset.test_table\] that is too long. Max allowed length is 1024 "
            r"characters.",
        ):
            self.bq_client.update_schema(
                address=self.mock_table_address,
                desired_schema_fields=new_schema_fields,
                allow_field_deletions=False,
            )

        self.mock_client.get_table.assert_not_called()

        # No other work to remove or add fields is done
        self.mock_client.query.assert_not_called()
        self.mock_client.update_table.assert_not_called()

    def test__get_excess_schema_fields_simple_excess(self) -> None:
        """Tests _get_excess_schema_fields() when extended_schema is a strict superset of base_schema."""
        base_schema = [bigquery.SchemaField("field_1", "INT")]
        extended_schema = [
            bigquery.SchemaField("field_1", "INT"),
            bigquery.SchemaField("field_2", "INT"),
            bigquery.SchemaField("field_3", "INT"),
        ]

        excess_fields = BigQueryClientImpl._get_excess_schema_fields(
            base_schema, extended_schema
        )

        self.assertEqual(
            excess_fields,
            [
                bigquery.SchemaField("field_2", "INT"),
                bigquery.SchemaField("field_3", "INT"),
            ],
        )

    def test__get_excess_schema_fields_with_extra_base_schema(self) -> None:
        """Tests _get_excess_schema_fields() when base_schema has fields not in extended_schema."""
        base_schema = [
            bigquery.SchemaField("field_1", "INT"),
            bigquery.SchemaField("field_2", "INT"),
        ]
        extended_schema = [
            bigquery.SchemaField("field_1", "INT"),
            bigquery.SchemaField("field_3", "INT"),
            bigquery.SchemaField("field_4", "INT"),
        ]

        excess_fields = BigQueryClientImpl._get_excess_schema_fields(
            base_schema, extended_schema
        )

        self.assertEqual(
            excess_fields,
            [
                bigquery.SchemaField("field_3", "INT"),
                bigquery.SchemaField("field_4", "INT"),
            ],
        )

    def test__get_excess_schema_fields_with_matching_schema(self) -> None:
        """Tests _get_excess_schema_fields() when base_schema is the same as extended_schema."""
        base_schema = [
            bigquery.SchemaField("field_1", "INT"),
            bigquery.SchemaField("field_2", "INT"),
        ]

        excess_fields = BigQueryClientImpl._get_excess_schema_fields(
            base_schema, base_schema
        )

        self.assertListEqual(excess_fields, [])

    def test__get_excess_schema_fields_no_excess(self) -> None:
        """Tests _get_excess_schema_fields() when base_schema is a superset of extended_schema."""
        base_schema = [
            bigquery.SchemaField("field_1", "INT"),
            bigquery.SchemaField("field_2", "INT"),
        ]
        extended_schema = [bigquery.SchemaField("field_2", "INT")]

        excess_fields = BigQueryClientImpl._get_excess_schema_fields(
            base_schema, extended_schema
        )

        self.assertListEqual(excess_fields, [])

    def test__get_excess_schema_fields_different_modes(self) -> None:
        """Tests _get_excess_schema_fields() ignore everything but field name"""
        base_schema = [
            bigquery.SchemaField("field_1", "INT"),
            bigquery.SchemaField("field_2", "INT", mode="NULLABLE"),
        ]
        extended_schema = [bigquery.SchemaField("field_2", "INT", mode="REQUIRED")]

        excess_fields = BigQueryClientImpl._get_excess_schema_fields(
            base_schema, extended_schema
        )

        self.assertListEqual(excess_fields, [])

    def test_delete_table(self) -> None:
        """Tests that our delete table function calls the correct client method."""
        self.bq_client.delete_table(self.mock_table_address)
        self.mock_client.delete_table.assert_called()

    @patch("google.cloud.bigquery.QueryJob")
    def test_paged_read_single_page_single_row(self, mock_query_job: MagicMock) -> None:
        first_row = bigquery.table.Row(
            ["parole", 15, "10N"],
            {"supervision_type": 0, "revocations": 1, "district": 2},
        )

        mock_query_job.result.side_effect = [[first_row]]

        processed_results = []

        def _process_fn(rows: List[bigquery.table.Row]) -> None:
            assert len(rows) == 1
            for row in rows:
                processed_results.append(dict(row))

        self.bq_client.paged_read_and_process(
            query_job=mock_query_job, page_size=1, process_page_fn=_process_fn
        )

        self.assertEqual([dict(first_row)], processed_results)
        mock_query_job.result.assert_has_calls([call(page_size=1)])

    @patch("google.cloud.bigquery.QueryJob")
    def test_paged_read_single_page_multiple_rows(
        self, mock_query_job: MagicMock
    ) -> None:
        first_row = bigquery.table.Row(
            ["parole", 15, "10N"],
            {"supervision_type": 0, "revocations": 1, "district": 2},
        )
        second_row = bigquery.table.Row(
            ["probation", 7, "10N"],
            {"supervision_type": 0, "revocations": 1, "district": 2},
        )

        mock_query_job.result.side_effect = [[first_row, second_row]]

        processed_results = []

        def _process_fn(rows: List[bigquery.table.Row]) -> None:
            assert len(rows) == 2
            for row in rows:
                processed_results.append(dict(row))

        self.bq_client.paged_read_and_process(
            query_job=mock_query_job, page_size=10, process_page_fn=_process_fn
        )

        self.assertEqual([dict(first_row), dict(second_row)], processed_results)
        mock_query_job.result.assert_has_calls([call(page_size=10)])

    @patch("google.cloud.bigquery.QueryJob")
    def test_paged_read_multiple_pages(self, mock_query_job: MagicMock) -> None:
        p1_r1 = bigquery.table.Row(
            ["parole", 15, "10N"],
            {"supervision_type": 0, "revocations": 1, "district": 2},
        )
        p1_r2 = bigquery.table.Row(
            ["probation", 7, "10N"],
            {"supervision_type": 0, "revocations": 1, "district": 2},
        )

        p2_r1 = bigquery.table.Row(
            ["parole", 8, "10F"],
            {"supervision_type": 0, "revocations": 1, "district": 2},
        )
        p2_r2 = bigquery.table.Row(
            ["probation", 3, "10F"],
            {"supervision_type": 0, "revocations": 1, "district": 2},
        )

        mock_query_job.result.side_effect = [[p1_r1, p1_r2, p2_r1, p2_r2]]

        processed_results = []

        def _process_fn(rows: List[bigquery.table.Row]) -> None:
            assert len(rows) == 2
            for row in rows:
                processed_results.append(dict(row))

        self.bq_client.paged_read_and_process(
            query_job=mock_query_job, page_size=2, process_page_fn=_process_fn
        )

        self.assertEqual(
            [dict(p1_r1), dict(p1_r2), dict(p2_r1), dict(p2_r2)], processed_results
        )
        mock_query_job.result.assert_has_calls([call(page_size=2)])

    @patch("google.cloud.bigquery.QueryJob")
    def test_paged_read_multiple_pages_loop_and_a_half(
        self, mock_query_job: MagicMock
    ) -> None:
        p1_r1 = bigquery.table.Row(
            ["parole", 15, "10N"],
            {"supervision_type": 0, "revocations": 1, "district": 2},
        )
        p1_r2 = bigquery.table.Row(
            ["probation", 7, "10N"],
            {"supervision_type": 0, "revocations": 1, "district": 2},
        )
        p1_r3 = bigquery.table.Row(
            ["probation", 2, "10N"],
            {"supervision_type": 0, "revocations": 1, "district": 2},
        )

        p2_r1 = bigquery.table.Row(
            ["parole", 8, "10F"],
            {"supervision_type": 0, "revocations": 1, "district": 2},
        )
        p2_r2 = bigquery.table.Row(
            ["probation", 3, "10F"],
            {"supervision_type": 0, "revocations": 1, "district": 2},
        )

        mock_query_job.result.side_effect = [[p1_r1, p1_r2, p1_r3, p2_r1, p2_r2]]

        processed_results: List[List[Dict]] = []

        def _process_fn(rows: List[bigquery.table.Row]) -> None:
            processed_results.append([dict(row) for row in rows])

        self.bq_client.paged_read_and_process(
            query_job=mock_query_job, page_size=3, process_page_fn=_process_fn
        )

        mock_query_job.result.assert_has_calls([call(page_size=3)])

        self.assertTrue(len(processed_results) == 2)
        self.assertEqual([dict(p1_r1), dict(p1_r2), dict(p1_r3)], processed_results[0])
        self.assertEqual([dict(p2_r1), dict(p2_r2)], processed_results[1])

    @patch("recidiviz.big_query.big_query_client.DataTransferServiceClient")
    @patch(
        "recidiviz.big_query.big_query_client.CROSS_REGION_COPY_STATUS_ATTEMPT_SLEEP_TIME_SEC",
        0.1,
    )
    def test_copy_dataset_tables_across_regions(
        self,
        mock_transfer_client_fn: MagicMock,
    ) -> None:
        mock_transfer_client = create_autospec(DataTransferServiceClient)
        mock_transfer_client_fn.return_value = mock_transfer_client

        mock_table = create_autospec(bigquery.Table)
        mock_table.table_type = "TABLE"
        mock_table.table_id = "my_table"
        mock_table.modified = datetime.datetime(2020, 1, 1)

        updated_mock_table = create_autospec(bigquery.Table)
        updated_mock_table.table_type = "TABLE"
        updated_mock_table.table_id = "my_table"
        updated_mock_table.modified = datetime.datetime(2020, 1, 2)

        mock_table_2 = create_autospec(bigquery.Table)
        mock_table_2.table_type = "TABLE"
        mock_table_2.table_id = "my_table_2"
        mock_table_2.modified = datetime.datetime(2020, 1, 1)

        updated_mock_table_2 = create_autospec(bigquery.Table)
        updated_mock_table_2.table_type = "TABLE"
        updated_mock_table_2.table_id = "my_table_2"
        updated_mock_table_2.modified = datetime.datetime(2020, 1, 2)

        def mock_check_valid_creds(
            request: CheckValidCredsRequest,
        ) -> CheckValidCredsResponse:
            return CheckValidCredsResponse(has_valid_creds=True)

        mock_transfer_client.check_valid_creds.side_effect = mock_check_valid_creds

        config_name = "projects/12345/locations/us/transferConfigs/61421b53-0000-22d3-8007-001a114e540a"

        def mock_create_transfer_config(
            parent: str, transfer_config: TransferConfig
        ) -> TransferConfig:
            self.assertIsNotNone(parent)
            self.assertFalse(transfer_config.params["overwrite_destination_table"])
            transfer_config.name = config_name
            return transfer_config

        mock_transfer_client.create_transfer_config.side_effect = (
            mock_create_transfer_config
        )

        run_name = f"{config_name}/runs/61394d2b-0000-2201-90bd-883d24f36b70"

        run_info_pending = create_autospec(TransferRun)
        run_info_pending.name = run_name
        run_info_pending.state = TransferState.PENDING
        run_info_success = create_autospec(TransferRun)
        run_info_success.name = run_name
        run_info_success.state = TransferState.SUCCEEDED

        mock_start_runs_response = create_autospec(StartManualTransferRunsResponse)
        mock_start_runs_response.runs = [run_info_pending]

        mock_transfer_client.start_manual_transfer_runs.return_value = (
            mock_start_runs_response
        )

        # Return pending, then success
        self.mock_client.list_tables.side_effect = [
            # Expected destination tables
            [mock_table, mock_table_2],
            # Initial destination tables
            [],
            # Pending
            [updated_mock_table],
            # Success
            [updated_mock_table, updated_mock_table_2],
        ]

        self.mock_client.get_table.side_effect = [
            # Source tables
            mock_table,
            mock_table_2,
            # Destination tables, attempt 1
            updated_mock_table,
            # Destination tables, attempt 2
            updated_mock_table,
            updated_mock_table_2,
        ]

        # Transfer still pending even though all tables are present
        mock_transfer_client.get_transfer_run.side_effect = [
            run_info_pending,
        ]

        self.bq_client.copy_dataset_tables_across_regions(
            source_dataset_id="my_src_dataset", destination_dataset_id="my_dst_dataset"
        )

        mock_transfer_client.create_transfer_config.assert_called_once()
        self.mock_client.list_tables.assert_has_calls(
            [
                call("my_src_dataset"),
                call("my_dst_dataset"),
                call("my_dst_dataset"),
                call("my_dst_dataset"),
            ]
        )
        mock_transfer_client.delete_transfer_config.assert_called_once()

    @patch("recidiviz.big_query.big_query_client.DataTransferServiceClient")
    @patch(
        "recidiviz.big_query.big_query_client.CROSS_REGION_COPY_STATUS_ATTEMPT_SLEEP_TIME_SEC",
        0.1,
    )
    def test_copy_dataset_tables_across_regions_nonempty(
        self, mock_transfer_client_fn: MagicMock
    ) -> None:
        mock_transfer_client = create_autospec(DataTransferServiceClient)
        mock_transfer_client_fn.return_value = mock_transfer_client

        mock_table = create_autospec(bigquery.Table)
        mock_table.table_type = "TABLE"
        mock_table.table_id = "my_table"

        mock_table_2 = create_autospec(bigquery.Table)
        mock_table_2.table_type = "TABLE"
        mock_table_2.table_id = "my_table_2"

        def mock_check_valid_creds(
            request: CheckValidCredsRequest,
        ) -> CheckValidCredsResponse:
            return CheckValidCredsResponse(has_valid_creds=True)

        mock_transfer_client.check_valid_creds.side_effect = mock_check_valid_creds

        config_name = "projects/12345/locations/us/transferConfigs/61421b53-0000-22d3-8007-001a114e540a"

        def mock_create_transfer_config(
            parent: str, transfer_config: TransferConfig
        ) -> TransferConfig:
            self.assertIsNotNone(parent)
            self.assertFalse(transfer_config.params["overwrite_destination_table"])
            transfer_config.name = config_name
            return transfer_config

        mock_transfer_client.create_transfer_config.side_effect = (
            mock_create_transfer_config
        )

        run_name = f"{config_name}/runs/61394d2b-0000-2201-90bd-883d24f36b70"

        run_info_pending = create_autospec(TransferRun)
        run_info_pending.name = run_name
        run_info_pending.state = TransferState.PENDING

        mock_start_runs_response = create_autospec(StartManualTransferRunsResponse)
        mock_start_runs_response.runs = [run_info_pending]

        mock_transfer_client.start_manual_transfer_runs.return_value = (
            mock_start_runs_response
        )

        # Return pending always
        self.mock_client.list_tables.side_effect = [
            # Expected destination tables
            [mock_table, mock_table_2],
            # Initial destination tables
            [mock_table, mock_table_2],
        ]

        with self.assertRaisesRegex(ValueError, "not empty"):
            self.bq_client.copy_dataset_tables_across_regions(
                source_dataset_id="my_src_dataset",
                destination_dataset_id="my_dst_dataset",
            )

        mock_transfer_client.create_transfer_config.assert_not_called()
        self.mock_client.list_tables.assert_has_calls(
            [
                call("my_src_dataset"),
                call("my_dst_dataset"),
            ]
        )

    @patch("recidiviz.big_query.big_query_client.DataTransferServiceClient")
    @patch(
        "recidiviz.big_query.big_query_client.CROSS_REGION_COPY_STATUS_ATTEMPT_SLEEP_TIME_SEC",
        0.1,
    )
    def test_copy_dataset_tables_across_regions_overwrite(
        self,
        mock_transfer_client_fn: MagicMock,
    ) -> None:
        mock_transfer_client = create_autospec(DataTransferServiceClient)
        mock_transfer_client_fn.return_value = mock_transfer_client

        mock_table = create_autospec(bigquery.Table)
        mock_table.table_type = "TABLE"
        mock_table.table_id = "my_table"
        mock_table.modified = datetime.datetime(2020, 1, 1)

        updated_mock_table = create_autospec(bigquery.Table)
        updated_mock_table.table_type = "TABLE"
        updated_mock_table.table_id = "my_table"
        updated_mock_table.modified = datetime.datetime(2020, 1, 2)

        mock_table_2 = create_autospec(bigquery.Table)
        mock_table_2.table_type = "TABLE"
        mock_table_2.table_id = "my_table_2"
        mock_table_2.modified = datetime.datetime(2020, 1, 1)

        updated_mock_table_2 = create_autospec(bigquery.Table)
        updated_mock_table_2.table_type = "TABLE"
        updated_mock_table_2.table_id = "my_table_2"
        updated_mock_table_2.modified = datetime.datetime(2020, 1, 2)

        mock_table_3 = create_autospec(bigquery.Table)
        mock_table_3.table_type = "TABLE"
        mock_table_3.table_id = "my_table_3"

        def mock_check_valid_creds(
            request: CheckValidCredsRequest,
        ) -> CheckValidCredsResponse:
            return CheckValidCredsResponse(has_valid_creds=True)

        mock_transfer_client.check_valid_creds.side_effect = mock_check_valid_creds

        config_name = "projects/12345/locations/us/transferConfigs/61421b53-0000-22d3-8007-001a114e540a"

        def mock_create_transfer_config(
            parent: str, transfer_config: TransferConfig
        ) -> TransferConfig:
            self.assertIsNotNone(parent)
            self.assertTrue(transfer_config.params["overwrite_destination_table"])
            transfer_config.name = config_name
            return transfer_config

        mock_transfer_client.create_transfer_config.side_effect = (
            mock_create_transfer_config
        )

        run_name = f"{config_name}/runs/61394d2b-0000-2201-90bd-883d24f36b70"

        run_info_pending = create_autospec(TransferRun)
        run_info_pending.name = run_name
        run_info_pending.state = TransferState.PENDING
        run_info_success = create_autospec(TransferRun)
        run_info_success.name = run_name
        run_info_success.state = TransferState.SUCCEEDED

        mock_start_runs_response = create_autospec(StartManualTransferRunsResponse)
        mock_start_runs_response.runs = [run_info_pending]

        mock_transfer_client.start_manual_transfer_runs.return_value = (
            mock_start_runs_response
        )

        # Return pending, then success
        self.mock_client.list_tables.side_effect = [
            # Expected destination tables
            [mock_table, mock_table_2],
            # Initial destination tables
            [mock_table_2, mock_table_3],
            # Pending
            [updated_mock_table],
            # Success
            [updated_mock_table, updated_mock_table_2],
        ]

        self.mock_client.get_table.side_effect = [
            # Source tables
            mock_table,
            mock_table_2,
            # Destination tables, attempt 1
            updated_mock_table,
            # Destination tables, attempt 2
            updated_mock_table,
            updated_mock_table_2,
        ]

        # Transfer still pending even though all tables are present
        mock_transfer_client.get_transfer_run.side_effect = [
            run_info_pending,
        ]

        self.bq_client.copy_dataset_tables_across_regions(
            source_dataset_id="my_src_dataset",
            destination_dataset_id="my_dst_dataset",
            overwrite_destination_tables=True,
        )

        mock_transfer_client.create_transfer_config.assert_called_once()
        self.mock_client.list_tables.assert_has_calls(
            [
                call("my_src_dataset"),
                call("my_dst_dataset"),
                call("my_dst_dataset"),
                call("my_dst_dataset"),
            ]
        )
        self.mock_client.delete_table.assert_called_with(
            bigquery.DatasetReference(self.mock_project_id, "my_dst_dataset").table(
                mock_table_3.table_id
            ),
            not_found_ok=False,
        )
        mock_transfer_client.delete_transfer_config.assert_called_once()

    @patch("recidiviz.big_query.big_query_client.DataTransferServiceClient")
    @patch(
        "recidiviz.big_query.big_query_client.CROSS_REGION_COPY_STATUS_ATTEMPT_SLEEP_TIME_SEC",
        0.1,
    )
    def test_copy_dataset_tables_across_regions_timeout(
        self, mock_transfer_client_fn: MagicMock
    ) -> None:
        mock_transfer_client = create_autospec(DataTransferServiceClient)
        mock_transfer_client_fn.return_value = mock_transfer_client

        mock_table = create_autospec(bigquery.Table)
        mock_table.table_type = "TABLE"
        mock_table.table_id = "my_table"
        mock_table.modified = datetime.datetime(2020, 1, 1)

        updated_mock_table = create_autospec(bigquery.Table)
        updated_mock_table.table_type = "TABLE"
        updated_mock_table.table_id = "my_table"
        updated_mock_table.modified = datetime.datetime(2020, 1, 2)

        mock_table_2 = create_autospec(bigquery.Table)
        mock_table_2.table_type = "TABLE"
        mock_table_2.table_id = "my_table_2"
        mock_table_2.modified = datetime.datetime(2020, 1, 1)

        config_name = "projects/12345/locations/us/transferConfigs/61421b53-0000-22d3-8007-001a114e540a"

        def mock_create_transfer_config(
            parent: str, transfer_config: TransferConfig
        ) -> TransferConfig:
            self.assertIsNotNone(parent)
            self.assertTrue(transfer_config.params["overwrite_destination_table"])
            transfer_config.name = config_name
            return transfer_config

        mock_transfer_client.create_transfer_config.side_effect = (
            mock_create_transfer_config
        )

        run_name = f"{config_name}/runs/61394d2b-0000-2201-90bd-883d24f36b70"

        run_info_pending = create_autospec(TransferRun)
        run_info_pending.name = run_name
        run_info_pending.state = TransferState.PENDING

        mock_start_runs_response = create_autospec(StartManualTransferRunsResponse)
        mock_start_runs_response.runs = [run_info_pending]

        mock_transfer_client.start_manual_transfer_runs.return_value = (
            mock_start_runs_response
        )

        # Return pending always
        self.mock_client.list_tables.side_effect = [
            # Expected destination tables
            [mock_table, mock_table_2],
            # Initial destination tables
            [mock_table, mock_table_2],
            # Pending
            [mock_table],
            # Pending
            [mock_table],
            # Pending
            [mock_table],
        ]

        self.mock_client.get_table.side_effect = [
            # Source tables
            mock_table,
            mock_table_2,
            # Destination tables, attempt 1
            updated_mock_table,
            # Destination tables, attempt 2
            updated_mock_table,
            # Destination tables, attempt 3
            updated_mock_table,
        ]

        with self.assertRaisesRegex(
            TimeoutError, "^Did not complete dataset copy before timeout"
        ):
            self.bq_client.copy_dataset_tables_across_regions(
                source_dataset_id="my_src_dataset",
                destination_dataset_id="my_dst_dataset",
                overwrite_destination_tables=True,
                timeout_sec=0.15,
            )

        mock_transfer_client.create_transfer_config.assert_called_once()
        self.mock_client.list_tables.assert_has_calls(
            [
                call("my_src_dataset"),  # Runs immediately
                call("my_dst_dataset"),  # Runs immediately
                call("my_dst_dataset"),  # Runs at 10s - timeout after this
            ]
        )
        # Important that we still delete the config
        mock_transfer_client.delete_transfer_config.assert_called_once()

    def test_copy_table(self) -> None:
        source_dataset_id = "my_source"
        destination_dataset_id = "my_destination"

        mock_table = create_autospec(bigquery.Table)
        mock_table.table_type = "TABLE"
        mock_table.table_id = "my_table"

        # Destination already exists
        self.mock_client.get_dataset.return_value = MagicMock()
        self.mock_client.get_table.return_value = mock_table

        copy_jobs: List[futures.Future] = [futures.Future(), futures.Future()]
        for job in copy_jobs:
            job.set_result(None)
        self.mock_client.copy_table.side_effect = copy_jobs

        self.bq_client.copy_table(
            source_table_address=BigQueryAddress(
                dataset_id=source_dataset_id,
                table_id=mock_table.table_id,
            ),
            destination_dataset_id=destination_dataset_id,
        )

        source_dataset_ref = bigquery.DatasetReference(
            "fake-recidiviz-project", source_dataset_id
        )
        destination_dataset_ref = bigquery.DatasetReference(
            "fake-recidiviz-project", destination_dataset_id
        )
        self.mock_client.copy_table.assert_has_calls(
            [
                call(
                    bigquery.TableReference(
                        source_dataset_ref,
                        "my_table",
                    ),
                    bigquery.TableReference(
                        destination_dataset_ref,
                        "my_table",
                    ),
                    job_config=self.copy_job_config,
                ),
            ]
        )

    def test_copy_table_schema_only(self) -> None:
        source_dataset_id = "my_source"
        destination_dataset_id = "my_destination"

        mock_table = create_autospec(bigquery.Table)
        mock_table.table_type = "TABLE"
        mock_table.table_id = "my_table"

        # Destination already exists
        self.mock_client.get_dataset.return_value = MagicMock()
        schema1 = [bigquery.schema.SchemaField("foo", "STRING")]

        def mock_get_table(table_ref: bigquery.TableReference) -> bigquery.Table:
            if table_ref.table_id == mock_table.table_id:
                return bigquery.Table(table_ref, schema1)
            raise ValueError("Unexpected table")

        self.mock_client.get_table.side_effect = mock_get_table

        self.bq_client.copy_table(
            source_table_address=BigQueryAddress(
                dataset_id=source_dataset_id,
                table_id=mock_table.table_id,
            ),
            destination_dataset_id=destination_dataset_id,
            schema_only=True,
        )

        destination_dataset_ref = bigquery.DatasetReference(
            "fake-recidiviz-project", destination_dataset_id
        )
        self.mock_client.copy_table.assert_not_called()
        self.mock_client.create_table.assert_has_calls(
            [
                call(
                    bigquery.Table(
                        bigquery.TableReference(
                            destination_dataset_ref,
                            "my_table",
                        ),
                        schema1,
                    ),
                    exists_ok=False,
                ),
            ]
        )

    def test_copy_table_schema_only_csv_external_config(self) -> None:
        source_dataset_id = "my_source"
        destination_dataset_id = "my_destination"

        mock_table = create_autospec(bigquery.Table)
        mock_table.dataset_id = "my_source"
        mock_table.table_type = "TABLE"
        mock_table.table_id = "my_table"
        mock_table.external_data_configuration = (
            bigquery.external_config.ExternalConfig("CSV")
        )
        mock_table.external_data_configuration.source_uris = ["gs://bucket/source.json"]
        mock_table.schema = [bigquery.schema.SchemaField("foo", "STRING")]

        # Destination already exists
        self.mock_client.get_dataset.return_value = MagicMock()
        schema1 = [bigquery.schema.SchemaField("foo", "STRING")]

        def mock_get_table(table_ref: bigquery.TableReference) -> bigquery.Table:
            if table_ref.table_id == mock_table.table_id:
                return mock_table
            raise ValueError("Unexpected table")

        self.mock_client.get_table.side_effect = mock_get_table

        self.bq_client.copy_table(
            source_table_address=BigQueryAddress(
                dataset_id=source_dataset_id,
                table_id=mock_table.table_id,
            ),
            destination_dataset_id=destination_dataset_id,
            schema_only=True,
        )

        destination_dataset_ref = bigquery.DatasetReference(
            "fake-recidiviz-project", destination_dataset_id
        )
        expected_table = bigquery.Table(
            bigquery.TableReference(
                destination_dataset_ref,
                "my_table",
            ),
            schema1,
        )
        expected_table.external_data_configuration = (
            bigquery.external_config.ExternalConfig("CSV")
        )
        expected_table.external_data_configuration.source_uris = [
            f"gs://{self.mock_project_id}-configs/empty.csv",
        ]
        self.mock_client.copy_table.assert_not_called()
        self.assertEqual(
            self.mock_client.create_table.call_args[0][
                0
            ].external_data_configuration.source_uris,
            [f"gs://{self.mock_project_id}-configs/empty.csv"],
        )

    def test_copy_table_schema_only_json_external_config(self) -> None:
        source_dataset_id = "my_source"
        destination_dataset_id = "my_destination"

        mock_table = create_autospec(bigquery.Table)
        mock_table.dataset_id = "my_source"
        mock_table.table_type = "TABLE"
        mock_table.table_id = "my_table"
        mock_table.external_data_configuration = (
            bigquery.external_config.ExternalConfig("NEWLINE_DELIMITED_JSON")
        )
        mock_table.external_data_configuration.source_uris = ["gs://bucket/source.json"]
        mock_table.schema = [bigquery.schema.SchemaField("foo", "STRING")]

        # Destination already exists
        self.mock_client.get_dataset.return_value = MagicMock()
        schema1 = [bigquery.schema.SchemaField("foo", "STRING")]

        def mock_get_table(table_ref: bigquery.TableReference) -> bigquery.Table:
            if table_ref.table_id == mock_table.table_id:
                return mock_table
            raise ValueError("Unexpected table")

        self.mock_client.get_table.side_effect = mock_get_table

        self.bq_client.copy_table(
            source_table_address=BigQueryAddress(
                dataset_id=source_dataset_id,
                table_id=mock_table.table_id,
            ),
            destination_dataset_id=destination_dataset_id,
            schema_only=True,
        )

        destination_dataset_ref = bigquery.DatasetReference(
            "fake-recidiviz-project", destination_dataset_id
        )
        expected_table = bigquery.Table(
            bigquery.TableReference(
                destination_dataset_ref,
                "my_table",
            ),
            schema1,
        )
        expected_table.external_data_configuration = (
            bigquery.external_config.ExternalConfig("NEWLINE_DELIMITED_JSON")
        )
        expected_table.external_data_configuration.source_uris = [
            f"gs://{self.mock_project_id}-configs/empty.json",
        ]
        self.mock_client.copy_table.assert_not_called()
        self.assertEqual(
            self.mock_client.create_table.call_args[0][
                0
            ].external_data_configuration.source_uris,
            [f"gs://{self.mock_project_id}-configs/empty.json"],
        )

    def test_copy_dataset(self) -> None:
        source_dataset_id = "my_source"
        destination_dataset_id = "my_destination"

        mock_table_address = ProjectSpecificBigQueryAddress(
            project_id=self.mock_project_id,
            dataset_id=source_dataset_id,
            table_id="my_table",
        )
        mock_table_2_address = ProjectSpecificBigQueryAddress(
            project_id=self.mock_project_id,
            dataset_id=source_dataset_id,
            table_id="my_table_2",
        )
        mock_view_address = ProjectSpecificBigQueryAddress(
            project_id=self.mock_project_id,
            dataset_id=source_dataset_id,
            table_id="my_view",
        )
        source_table_addresses = [
            mock_table_address,
            mock_view_address,
            mock_table_2_address,
        ]

        mock_table = create_autospec(bigquery.Table)
        mock_table.table_type = "TABLE"
        mock_table.table_id = mock_table_address.table_id
        mock_table_2 = create_autospec(bigquery.Table)
        mock_table_2.table_type = "TABLE"
        mock_table_2.table_id = mock_table_2_address.table_id
        mock_view = create_autospec(bigquery.Table)
        mock_view.table_type = "VIEW"

        dataset_tables = [mock_table, mock_view, mock_table_2]

        # Destination already exists
        self.mock_client.get_dataset.return_value = MagicMock()

        def mock_list_tables(dataset_id: str) -> Iterator[bigquery.table.TableListItem]:
            if dataset_id == destination_dataset_id:
                tables = []
            elif dataset_id == source_dataset_id:
                tables = [
                    bigquery.table.TableListItem.from_string(address.to_str())
                    for address in source_table_addresses
                ]
            else:
                raise ValueError(f"Unexpected dataset [{dataset_id}]")

            return iter(tables)

        self.mock_client.list_tables.side_effect = mock_list_tables
        self.mock_client.get_table.side_effect = dataset_tables
        copy_jobs: List[PollingFuture] = [
            create_autospec(CopyJob),
            create_autospec(CopyJob),
        ]
        for job in copy_jobs:
            job.set_result(None)
        self.mock_client.copy_table.side_effect = copy_jobs

        self.bq_client.copy_dataset_tables(
            source_dataset_id=source_dataset_id,
            destination_dataset_id=destination_dataset_id,
        )

        self.assertEqual(
            bigquery.job.WriteDisposition.WRITE_EMPTY,
            self.copy_job_config.write_disposition,
        )
        self.mock_client.list_tables.assert_has_calls(
            [call("my_source"), call("my_destination")]
        )
        source_dataset_ref = bigquery.DatasetReference(
            "fake-recidiviz-project", source_dataset_id
        )
        destination_dataset_ref = bigquery.DatasetReference(
            "fake-recidiviz-project", destination_dataset_id
        )

        self.mock_client.copy_table.assert_has_calls(
            [
                call(
                    bigquery.TableReference(
                        source_dataset_ref,
                        "my_table",
                    ),
                    bigquery.TableReference(
                        destination_dataset_ref,
                        "my_table",
                    ),
                    job_config=self.copy_job_config,
                ),
                call(
                    bigquery.TableReference(
                        source_dataset_ref,
                        "my_table_2",
                    ),
                    bigquery.TableReference(
                        destination_dataset_ref,
                        "my_table_2",
                    ),
                    job_config=self.copy_job_config,
                ),
            ]
        )

    def test_copy_dataset_tables_schema_only(self) -> None:
        source_dataset_id = "my_source"
        destination_dataset_id = "my_destination"

        mock_table_address = ProjectSpecificBigQueryAddress(
            project_id=self.mock_project_id,
            dataset_id=source_dataset_id,
            table_id="my_table",
        )
        mock_table_2_address = ProjectSpecificBigQueryAddress(
            project_id=self.mock_project_id,
            dataset_id=source_dataset_id,
            table_id="my_table_2",
        )
        mock_view_address = ProjectSpecificBigQueryAddress(
            project_id=self.mock_project_id,
            dataset_id=source_dataset_id,
            table_id="my_view",
        )
        source_table_addresses = [
            mock_table_address,
            mock_view_address,
            mock_table_2_address,
        ]

        mock_table = create_autospec(bigquery.Table)
        mock_table.table_type = "TABLE"
        mock_table.table_id = "my_table"
        mock_table_2 = create_autospec(bigquery.Table)
        mock_table_2.table_type = "TABLE"
        mock_table_2.table_id = "my_table_2"
        mock_view = create_autospec(bigquery.Table)
        mock_view.table_type = "VIEW"
        mock_view.table_id = "my_view"

        # Destination already exists
        self.mock_client.get_dataset.return_value = MagicMock()

        def mock_list_tables(dataset_id: str) -> Iterator[bigquery.table.TableListItem]:
            if dataset_id == destination_dataset_id:
                tables = []
            elif dataset_id == source_dataset_id:
                tables = [
                    bigquery.table.TableListItem.from_string(address.to_str())
                    for address in source_table_addresses
                ]
            else:
                raise ValueError(f"Unexpected datset [{dataset_id}]")

            return iter(tables)

        self.mock_client.list_tables.side_effect = mock_list_tables

        schema1 = [bigquery.schema.SchemaField("foo", "STRING")]
        schema2 = [bigquery.schema.SchemaField("bar", "STRING")]
        schema3 = [bigquery.schema.SchemaField("baz", "STRING")]

        def mock_get_table(table_ref: bigquery.TableReference) -> bigquery.Table:
            if table_ref.table_id == mock_table.table_id:
                return bigquery.Table(table_ref, schema1)
            if table_ref.table_id == mock_table_2.table_id:
                return bigquery.Table(table_ref, schema2)
            if table_ref.table_id == mock_view.table_id:
                return bigquery.Table(table_ref, schema3)
            raise ValueError(f"Unexpected table [{table_ref}]")

        self.mock_client.get_table.side_effect = mock_get_table

        self.bq_client.copy_dataset_tables(
            source_dataset_id=source_dataset_id,
            destination_dataset_id=destination_dataset_id,
            schema_only=True,
        )

        self.mock_client.list_tables.assert_has_calls(
            [call("my_source"), call("my_destination")]
        )
        destination_dataset_ref = bigquery.DatasetReference(
            "fake-recidiviz-project", destination_dataset_id
        )
        self.mock_client.copy_table.assert_not_called()
        self.mock_client.create_table.assert_has_calls(
            [
                call(
                    bigquery.Table(
                        bigquery.TableReference(
                            destination_dataset_ref,
                            "my_table",
                        ),
                        schema1,
                    ),
                    exists_ok=False,
                ),
                call(
                    bigquery.Table(
                        bigquery.TableReference(
                            destination_dataset_ref,
                            "my_view",
                        ),
                        schema3,
                    ),
                    exists_ok=False,
                ),
                call(
                    bigquery.Table(
                        bigquery.TableReference(
                            destination_dataset_ref,
                            "my_table_2",
                        ),
                        schema2,
                    ),
                    exists_ok=False,
                ),
            ],
            any_order=True,
        )

    def test_copy_dataset_tables_overwrite(self) -> None:
        source_dataset_id = "my_source"
        destination_dataset_id = "my_destination"

        mock_table_address = ProjectSpecificBigQueryAddress(
            project_id=self.mock_project_id,
            dataset_id=source_dataset_id,
            table_id="my_table",
        )
        mock_table_2_address = ProjectSpecificBigQueryAddress(
            project_id=self.mock_project_id,
            dataset_id=source_dataset_id,
            table_id="my_table_2",
        )
        mock_view_address = ProjectSpecificBigQueryAddress(
            project_id=self.mock_project_id,
            dataset_id=source_dataset_id,
            table_id="my_view",
        )
        source_table_addresses = [
            mock_table_address,
            mock_view_address,
            mock_table_2_address,
        ]

        mock_table = create_autospec(bigquery.Table)
        mock_table.table_type = "TABLE"
        mock_table.table_id = "my_table"
        mock_table_2 = create_autospec(bigquery.Table)
        mock_table_2.table_type = "TABLE"
        mock_table_2.table_id = "my_table_2"
        mock_view = create_autospec(bigquery.Table)
        mock_view.table_type = "VIEW"

        extra_table_in_destination_address = ProjectSpecificBigQueryAddress(
            project_id=self.mock_project_id,
            dataset_id=destination_dataset_id,
            table_id="my_extra_table",
        )

        extra_table_in_destination = create_autospec(bigquery.Table)
        extra_table_in_destination.table_type = "TABLE"
        extra_table_in_destination.table_id = "my_extra_table"

        dataset_tables = [mock_table, mock_view, mock_table_2]

        # Destination already exists
        self.mock_client.get_dataset.return_value = MagicMock()

        def mock_list_tables(dataset_id: str) -> Iterator[bigquery.table.TableListItem]:
            if dataset_id == destination_dataset_id:
                tables = [
                    bigquery.table.TableListItem.from_string(a.to_str())
                    for a in [mock_table_address, extra_table_in_destination_address]
                ]
            elif dataset_id == source_dataset_id:
                tables = [
                    bigquery.table.TableListItem.from_string(a.to_str())
                    for a in source_table_addresses
                ]
            else:
                raise ValueError(f"Unexpected dataset [{dataset_id}]")

            return iter(tables)

        self.mock_client.list_tables.side_effect = mock_list_tables
        self.mock_client.get_table.side_effect = dataset_tables
        copy_jobs: List[PollingFuture] = [
            create_autospec(CopyJob),
            create_autospec(CopyJob),
        ]
        for job in copy_jobs:
            job.set_result(None)
        self.mock_client.copy_table.side_effect = copy_jobs

        self.bq_client.copy_dataset_tables(
            source_dataset_id=source_dataset_id,
            destination_dataset_id=destination_dataset_id,
            overwrite_destination_tables=True,
        )

        self.mock_client.list_tables.assert_has_calls(
            [call("my_source"), call("my_destination")]
        )
        source_dataset_ref = bigquery.DatasetReference(
            "fake-recidiviz-project", source_dataset_id
        )
        destination_dataset_ref = bigquery.DatasetReference(
            "fake-recidiviz-project", destination_dataset_id
        )
        self.mock_client.delete_table.assert_called_with(
            bigquery.TableReference(destination_dataset_ref, "my_extra_table"),
            not_found_ok=False,
        )
        self.assertEqual(
            bigquery.WriteDisposition.WRITE_TRUNCATE,
            self.copy_job_config.write_disposition,
        )
        self.mock_client.copy_table.assert_has_calls(
            [
                call(
                    bigquery.TableReference(
                        source_dataset_ref,
                        "my_table",
                    ),
                    bigquery.TableReference(
                        destination_dataset_ref,
                        "my_table",
                    ),
                    job_config=self.copy_job_config,
                ),
                call(
                    bigquery.TableReference(
                        source_dataset_ref,
                        "my_table_2",
                    ),
                    bigquery.TableReference(
                        destination_dataset_ref,
                        "my_table_2",
                    ),
                    job_config=self.copy_job_config,
                ),
            ]
        )

    @freeze_time("2021-04-14 03:14:23.5678")
    def test_backup_dataset_if_exists(self) -> None:
        dataset_to_backup_id = "my_dataset"
        expected_backup_dataset_id = "my_dataset_backup_2021_04_14_03_14_23_567800"

        mock_table_address = ProjectSpecificBigQueryAddress(
            project_id=self.mock_project_id,
            dataset_id=dataset_to_backup_id,
            table_id="my_table",
        )
        mock_view_address = ProjectSpecificBigQueryAddress(
            project_id=self.mock_project_id,
            dataset_id=dataset_to_backup_id,
            table_id="my_view",
        )
        dataset_table_addresses = [
            mock_table_address,
            mock_view_address,
        ]

        mock_table = create_autospec(bigquery.Table)
        mock_table.table_type = "TABLE"
        mock_table.table_id = "my_table"
        mock_view = create_autospec(bigquery.Table)
        mock_view.table_type = "VIEW"

        dataset_tables = [mock_table, mock_view]

        backup_dataset_calls: List[bool] = []

        # Destination already exists
        def mock_get_dataset(
            dataset_ref: bigquery.DatasetReference,
        ) -> bigquery.Dataset:
            if dataset_ref.dataset_id == dataset_to_backup_id:
                return MagicMock()
            if dataset_ref.dataset_id == expected_backup_dataset_id:
                if len(backup_dataset_calls) == 0:
                    backup_dataset_calls.append(True)
                    raise exceptions.NotFound("This exception should be caught")
                return MagicMock()
            raise ValueError(f"Unexpected dataset [{dataset_ref.dataset_id}]")

        self.mock_client.get_dataset.side_effect = mock_get_dataset

        def mock_list_tables(dataset_id: str) -> Iterator[bigquery.table.TableListItem]:
            if dataset_id == expected_backup_dataset_id:
                tables = []
            elif dataset_id == dataset_to_backup_id:
                tables = [
                    bigquery.table.TableListItem.from_string(address.to_str())
                    for address in dataset_table_addresses
                ]
            else:
                raise ValueError(f"Unexpected dataset [{dataset_id}]")

            return iter(tables)

        self.mock_client.list_tables.side_effect = mock_list_tables
        self.mock_client.get_table.side_effect = dataset_tables
        copy_jobs: List[PollingFuture] = [create_autospec(CopyJob)]
        for job in copy_jobs:
            job.set_result(None)
        self.mock_client.copy_table.side_effect = copy_jobs

        self.bq_client.backup_dataset_tables_if_dataset_exists(dataset_to_backup_id)

        self.mock_client.create_dataset.assert_called_once()

        self.mock_client.list_tables.assert_has_calls(
            [call(dataset_to_backup_id), call(expected_backup_dataset_id)]
        )

        source_dataset_ref = bigquery.DatasetReference(
            "fake-recidiviz-project", dataset_to_backup_id
        )
        destination_dataset_ref = bigquery.DatasetReference(
            "fake-recidiviz-project", expected_backup_dataset_id
        )
        self.mock_client.copy_table.assert_has_calls(
            [
                call(
                    bigquery.TableReference(
                        source_dataset_ref,
                        "my_table",
                    ),
                    bigquery.TableReference(
                        destination_dataset_ref,
                        "my_table",
                    ),
                    job_config=self.copy_job_config,
                ),
            ]
        )

    def test_backup_dataset_if_exists_does_not_exist(self) -> None:
        dataset_to_backup_id = "my_dataset_to_backup"
        self.mock_client.get_dataset.return_value = [exceptions.NotFound]
        self.bq_client.backup_dataset_tables_if_dataset_exists(dataset_to_backup_id)
        self.mock_client.create_dataset.assert_not_called()
        self.mock_client.copy_table.assert_not_called()

    def test_wait_for_big_query_jobs(self) -> None:
        class MyRandomPollingFuture(PollingFuture):
            def __init__(self, result_str: str):
                super().__init__()
                self.start_time = datetime.datetime.now()
                self.wait_time_sec = random.uniform(0, 0.5)
                self.result_str = result_str
                self.is_cancelled = False

            def done(self, retry: Retry = DEFAULT_RETRY) -> bool:
                if (
                    self.start_time + datetime.timedelta(seconds=self.wait_time_sec)
                    > datetime.datetime.now()
                ):
                    return False
                self._result = self.result_str
                return True

            def cancel(self) -> None:
                self.is_cancelled = True

            def cancelled(self) -> bool:
                return self.is_cancelled

        jobs = [MyRandomPollingFuture("result1"), MyRandomPollingFuture("result2")]
        results = self.bq_client.wait_for_big_query_jobs(jobs)
        self.assertEqual(["result1", "result2"], sorted(results))

    def test_wait_for_big_query_jobs_empty(self) -> None:
        # Shouldn't hang
        results = self.bq_client.wait_for_big_query_jobs([])
        self.assertEqual([], results)

    @freeze_time("2021-04-14 03:14:23.5678")
    def test_add_timestamp_suffix_to_dataset_id(self) -> None:
        dataset_id_with_timestamp = self.bq_client.add_timestamp_suffix_to_dataset_id(
            dataset_id="dataset_id"
        )

        self.assertEqual(
            "dataset_id_2021_04_14_03_14_23_567800", dataset_id_with_timestamp
        )

    def test_row_level_permissions(self) -> None:
        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.mock_project_id, "us_mi_dataset"),
            "table",
        )
        table = bigquery.Table(
            table_ref, [bigquery.SchemaField("state_code", "STRING")]
        )
        self.mock_client.create_table.return_value = table

        with patch.object(
            self.bq_client, "list_row_level_permissions", return_value=[]
        ):
            self.bq_client.create_table(table)

        self.mock_client.query.assert_called_once()
        query = self.mock_client.query.mock_calls[0].kwargs["query"]
        self.assertIn("DROP ALL ROW ACCESS POLICIES", query)
        self.assertIn("CREATE OR REPLACE ROW ACCESS POLICY", query)

    def test_no_row_level_permissions(self) -> None:
        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.mock_project_id, "dataset"),
            "table",
        )
        table = bigquery.Table(table_ref)
        self.mock_client.create_table.return_value = table

        self.bq_client.create_table(table)

        # Don't apply row-level permissions for non-state specific tables
        self.mock_client.query.assert_not_called()


class MaterializeTableJobConfigMatcher:
    """Class for matching QueryJobConfig objects against expected job config for the
    materialize_view_to_table() function.
    """

    def __init__(self, expected_destination: str):
        self.expected_destination = expected_destination

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, QueryJobConfig):
            return False

        if other.write_disposition != bigquery.WriteDisposition.WRITE_TRUNCATE:
            return False

        return str(other.destination) == self.expected_destination


class BigQueryClientImplEmulatorTest(BigQueryEmulatorTestCase):
    """Emulator test cases for BigQueryClientImpl"""

    def setUp(self) -> None:
        super().setUp()

        self.simple_schema = [
            bigquery.SchemaField(mode="NULLABLE", field_type="STRING", name="col_1"),
            bigquery.SchemaField(mode="NULLABLE", field_type="STRING", name="col_2"),
        ]

    def test_insert_into_table_from_table_async(self) -> None:
        source, destination = BigQueryAddress(
            dataset_id="dataset", table_id="source"
        ), BigQueryAddress(dataset_id="datatset", table_id="destination")
        self.create_mock_table(source, self.simple_schema)
        self.create_mock_table(destination, self.simple_schema)

        fake_data = [
            {"col_1": "val_1", "col_2": "val_2"},
            {"col_1": "val_3", "col_2": "val_4"},
        ]

        self.load_rows_into_table(source, fake_data)

        job = self.bq_client.insert_into_table_from_table_async(
            source_address=source,
            destination_address=destination,
            use_query_cache=False,
        )

        job.result()

        self.run_query_test(f"select * from `{destination.to_str()}`", fake_data)

        job = self.bq_client.insert_into_table_from_table_async(
            source_address=source,
            destination_address=destination,
            use_query_cache=False,
        )

        job.result()

        self.run_query_test(f"select * from `{destination.to_str()}`", fake_data * 2)

    def test_insert_into_table_from_table_async_source_has_fewer_cols(self) -> None:
        source, destination = BigQueryAddress(
            dataset_id="dataset", table_id="source"
        ), BigQueryAddress(dataset_id="datatset", table_id="destination")
        self.create_mock_table(source, self.simple_schema)
        self.create_mock_table(
            destination,
            [
                *self.simple_schema,
                bigquery.SchemaField(
                    mode="NULLABLE", field_type="STRING", name="col_3"
                ),
            ],
        )

        fake_data = [
            {"col_1": "val_1", "col_2": "val_2"},
            {"col_1": "val_3", "col_2": "val_4"},
        ]

        self.load_rows_into_table(source, fake_data)

        job = self.bq_client.insert_into_table_from_table_async(
            source_address=source,
            destination_address=destination,
            use_query_cache=False,
        )

        job.result()

        fake_output = [{**row, "col_3": None} for row in fake_data]

        self.run_query_test(f"select * from `{destination.to_str()}`", fake_output)

    def test_insert_into_table_from_table_async_dest_has_fewer_cols(self) -> None:
        source, destination = BigQueryAddress(
            dataset_id="dataset", table_id="source"
        ), BigQueryAddress(dataset_id="datatset", table_id="destination")
        self.create_mock_table(
            source,
            [
                *self.simple_schema,
                bigquery.SchemaField(
                    mode="NULLABLE", field_type="STRING", name="col_3"
                ),
            ],
        )
        self.create_mock_table(destination, self.simple_schema)

        fake_data = [
            {"col_1": "val_1", "col_2": "val_2", "col_3": "watch me"},
            {"col_1": "val_3", "col_2": "val_4", "col_3": "dissapear"},
        ]

        self.load_rows_into_table(source, fake_data)

        job = self.bq_client.insert_into_table_from_table_async(
            source_address=source,
            destination_address=destination,
            use_query_cache=False,
        )

        job.result()

        fake_output = [
            {"col_1": "val_1", "col_2": "val_2"},
            {"col_1": "val_3", "col_2": "val_4"},
        ]

        self.run_query_test(f"select * from `{destination.to_str()}`", fake_output)

    def test_insert_into_table_from_table_async_where_clause(self) -> None:
        source, destination = BigQueryAddress(
            dataset_id="dataset", table_id="source"
        ), BigQueryAddress(dataset_id="datatset", table_id="destination")
        self.create_mock_table(
            source,
            [
                *self.simple_schema,
                bigquery.SchemaField(
                    mode="NULLABLE", field_type="STRING", name="col_3"
                ),
            ],
        )
        self.create_mock_table(destination, self.simple_schema)

        fake_data = [
            {"col_1": "val_1", "col_2": "val_2", "col_3": "watch me"},
            {"col_1": "val_3", "col_2": "val_4", "col_3": "dissapear"},
        ]

        self.load_rows_into_table(source, fake_data)

        job = self.bq_client.insert_into_table_from_table_async(
            source_address=source,
            destination_address=destination,
            source_data_filter_clause="WHERE col_3 != 'dissapear'",
            use_query_cache=False,
        )

        job.result()

        fake_output = [{"col_1": "val_1", "col_2": "val_2"}]

        self.run_query_test(f"select * from `{destination.to_str()}`", fake_output)

    def test_valid_field_types(self) -> None:
        # STRING doesn't map to any alias
        valid_field_types = BigQueryClientImpl._valid_field_types(
            bigquery.SchemaField(field_type="STRING", name="col_1")
        )
        self.assertSetEqual({"STRING"}, valid_field_types)

        valid_field_types = BigQueryClientImpl._valid_field_types(
            bigquery.SchemaField(field_type="BOOL", name="col_1")
        )
        self.assertSetEqual({"BOOLEAN", "BOOL"}, valid_field_types)

        valid_field_types = BigQueryClientImpl._valid_field_types(
            bigquery.SchemaField(field_type="FLOAT64", name="col_1")
        )
        self.assertSetEqual({"FLOAT", "FLOAT64"}, valid_field_types)

        valid_field_types = BigQueryClientImpl._valid_field_types(
            bigquery.SchemaField(field_type="INT64", name="col_1")
        )
        self.assertSetEqual({"INTEGER", "INT64"}, valid_field_types)

        valid_field_types = BigQueryClientImpl._valid_field_types(
            bigquery.SchemaField(field_type="STRUCT", name="col_1")
        )
        self.assertSetEqual({"RECORD", "STRUCT"}, valid_field_types)

    # TODO(#31105) un-underscore when emulator rejects null values in required fields
    def _test_insert_into_table_from_table_async_dest_required(self) -> None:
        """Test that should fail as the rows being loaded are nullable in the source
        schema (and are null) but are required in the destination schema
        """
        source, destination = BigQueryAddress(
            dataset_id="dataset", table_id="source"
        ), BigQueryAddress(dataset_id="datatset", table_id="destination")

        self.create_mock_table(source, self.simple_schema)
        self.create_mock_table(
            destination,
            [
                bigquery.SchemaField(
                    mode="REQUIRED", field_type="STRING", name="col_1"
                ),
                bigquery.SchemaField(
                    mode="REQUIRED", field_type="STRING", name="col_2"
                ),
            ],
        )

        fake_data = [
            {"col_1": "val_1", "col_2": "val_2"},
            {"col_1": "val_3", "col_2": "val_4"},
        ]

        self.load_rows_into_table(source, fake_data)

        job = self.bq_client.insert_into_table_from_table_async(
            source_address=source,
            destination_address=destination,
            use_query_cache=False,
        )

        job.result()

        fake_output = [
            {"col_1": "val_1", "col_2": "val_2"},
            {"col_1": "val_3", "col_2": "val_4"},
        ]

        self.run_query_test(f"select * from `{destination.to_str()}`", fake_output)

        fake_data_fail = [{"col_1": None, "col_2": "val_2"}]

        self.load_rows_into_table(source, fake_data_fail)

        job = self.bq_client.insert_into_table_from_table_async(
            source_address=source,
            destination_address=destination,
            use_query_cache=False,
        )

        # should fail
        job.result()

        # if one row fails to be inserted, the whole statement is rolled back so we
        # should have the same table as before
        fake_output = [
            {"col_1": "val_1", "col_2": "val_2"},
            {"col_1": "val_3", "col_2": "val_4"},
        ]

        self.run_query_test(f"select * from `{destination.to_str()}`", fake_output)

    # TODO(#31105) un-underscore when emulator rejects null values in required fields
    def _test_insert_into_table_from_table_async_dest_required_but_missing(
        self,
    ) -> None:
        """Test that should fail as a required schema field that will always is missing
        from the source schema and thus will always be null
        """
        source, destination = BigQueryAddress(
            dataset_id="dataset", table_id="source"
        ), BigQueryAddress(dataset_id="datatset", table_id="destination")

        self.create_mock_table(source, self.simple_schema)
        self.create_mock_table(
            destination,
            [
                bigquery.SchemaField(
                    mode="REQUIRED", field_type="STRING", name="col_1"
                ),
                bigquery.SchemaField(
                    mode="REQUIRED", field_type="STRING", name="col_2"
                ),
                bigquery.SchemaField(
                    mode="REQUIRED", field_type="STRING", name="col_3"
                ),
            ],
        )

        fake_data = [
            {"col_1": "val_1", "col_2": "val_2"},
            {"col_1": "val_3", "col_2": "val_4"},
        ]

        self.load_rows_into_table(source, fake_data)

        job = self.bq_client.insert_into_table_from_table_async(
            source_address=source,
            destination_address=destination,
            use_query_cache=False,
        )

        # this will always fail
        job.result()

        self.run_query_test(f"select * from `{destination.to_str()}`", [])
