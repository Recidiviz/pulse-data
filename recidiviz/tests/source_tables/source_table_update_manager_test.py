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
"""Tests capabilities of the SourceTableUpdateManager"""
import tempfile
from typing import Any
from unittest.mock import patch

import attr
import pytest
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableCollectionValidationConfig,
    SourceTableConfig,
)
from recidiviz.source_tables.source_table_update_manager import (
    SourceTableDryRunResult,
    SourceTableFailedToUpdateError,
    SourceTableUpdateManager,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

_DATASET_1 = "dataset_1"
_TABLE_1 = "table_1"


class TestSourceTableUpdateManager(BigQueryEmulatorTestCase):
    """Tests the SourceTableUpdateManager using raw data tables as fixtures."""

    source_table_update_manager: SourceTableUpdateManager

    def setUp(self) -> None:
        super().setUp()

        self.source_table_update_manager = SourceTableUpdateManager(
            client=self.bq_client
        )
        self.repository = build_source_table_repository_for_collected_schemata(
            project_id=self.bq_client.project_id
        )
        self.source_table_collection = one(
            [
                collection
                for collection in self.repository.source_table_collections
                if collection.dataset_id
                == raw_tables_dataset_for_region(
                    state_code=StateCode.US_PA,
                    instance=DirectIngestInstance.PRIMARY,
                )
            ]
        )

    def test_base_case(self) -> None:
        """Creates tables for all raw data tables in the collection"""
        self.source_table_update_manager.update(
            source_table_collection=self.source_table_collection
        )

        self.assertSetEqual(
            {dataset.dataset_id for dataset in self.bq_client.list_datasets()},
            {self.source_table_collection.dataset_id},
        )

        self.assertSetEqual(
            {
                table.table_id
                for table in self.bq_client.list_tables(
                    self.source_table_collection.dataset_id
                )
            },
            {
                address.table_id
                for address in self.source_table_collection.source_tables_by_address.keys()
            },
        )

    def test_allow_field_deletion_false_raises(self) -> None:
        table_address = BigQueryAddress(
            dataset_id="test_dataset", table_id="test_table"
        )

        extraneous_field = bigquery.SchemaField("age", "INT64")
        with_extraneous_field = SourceTableCollection(
            dataset_id="test_dataset",
            source_tables_by_address={
                table_address: SourceTableConfig(
                    address=table_address,
                    schema_fields=[
                        bigquery.SchemaField("name", "STRING"),
                        extraneous_field,
                    ],
                    description="Raw data table",
                )
            },
            description="Description for dataset test_dataset",
        )

        self.source_table_update_manager.update(
            source_table_collection=with_extraneous_field,
        )

        without_extraneous_field = SourceTableCollection(
            dataset_id="test_dataset",
            source_tables_by_address={
                table_address: SourceTableConfig(
                    address=table_address,
                    schema_fields=[
                        bigquery.SchemaField("name", "STRING"),
                    ],
                    description="Raw data table",
                )
            },
            description="Description for dataset test_dataset",
        )

        with self.assertRaises(ValueError):
            self.source_table_update_manager.update(
                source_table_collection=without_extraneous_field,
            )

    def test_clustering_fields_changed_raises(self) -> None:
        table_address = BigQueryAddress(
            dataset_id="test_dataset", table_id="test_table"
        )

        table_config = SourceTableConfig(
            address=table_address,
            schema_fields=[
                bigquery.SchemaField("name", "STRING"),
            ],
            description="Raw data table",
            clustering_fields=["name"],
        )
        with_clustering = SourceTableCollection(
            dataset_id="test_dataset",
            source_tables_by_address={table_address: table_config},
            description="Description for dataset test_dataset",
        )
        self.source_table_update_manager.update(source_table_collection=with_clustering)

        with self.assertRaisesRegex(
            SourceTableFailedToUpdateError, "has clustering fields.+that do not match"
        ):
            self.source_table_update_manager.update(
                source_table_collection=SourceTableCollection(
                    dataset_id="test_dataset",
                    source_tables_by_address={
                        table_address: attr.evolve(
                            table_config,
                            clustering_fields=None,
                        )
                    },
                    description="Description for dataset test_dataset",
                )
            )

    def test_clustering_fields_null(self) -> None:
        """If a table's clustering_fields changes between empty / null but remains unset, no error is raised"""
        table_address = BigQueryAddress(
            dataset_id="test_dataset", table_id="test_table"
        )

        table_config = SourceTableConfig(
            address=table_address,
            schema_fields=[
                bigquery.SchemaField("name", "STRING"),
            ],
            description="Raw data table",
            clustering_fields=None,
        )
        collection = SourceTableCollection(
            dataset_id="test_dataset",
            source_tables_by_address={table_address: table_config},
            description="Description for dataset test_dataset",
        )
        self.source_table_update_manager.update(source_table_collection=collection)

        # Assert no errors
        self.source_table_update_manager.update(
            source_table_collection=SourceTableCollection(
                dataset_id="test_dataset",
                source_tables_by_address={
                    table_address: attr.evolve(table_config, clustering_fields=[])
                },
                description="Description for dataset test_dataset",
            ),
        )


class TestSourceTableUpdateManagerRecreateOnError(BigQueryEmulatorTestCase):
    """Tests SourceTableUpdateManager recreate_on_error handling"""

    source_table_update_manager: SourceTableUpdateManager

    dataset_id = "test_dataset"

    table_address = BigQueryAddress(dataset_id="test_dataset", table_id="test_table")

    existing_table_config = SourceTableConfig(
        address=table_address,
        description="pre-update table",
        schema_fields=[SchemaField("id", "INTEGER")],
    )

    updated_table_config = SourceTableConfig(
        address=table_address,
        description="pre-update table",
        schema_fields=[
            SchemaField("id", "INTEGER"),
            SchemaField("test_column", "STRING"),
        ],
    )

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        return [
            SourceTableCollection(
                dataset_id=cls.dataset_id,
                source_tables_by_address={cls.table_address: cls.existing_table_config},
                description=f"Description for dataset {cls.dataset_id}",
            )
        ]

    def setUp(self) -> None:
        super().setUp()

        def _update_schema_mock(*args: Any, **kwargs: Any) -> None:
            raise ValueError("error updating")

        self.update_schema_patcher = patch.object(
            self.bq_client, attribute="update_schema", new=_update_schema_mock
        )
        self.update_schema_patcher.start()

        self.source_table_update_manager = SourceTableUpdateManager(
            client=self.bq_client
        )

    def tearDown(self) -> None:
        super().tearDown()
        self.update_schema_patcher.stop()

    def test_recreate_false_raises(self) -> None:
        with self.assertRaisesRegex(
            SourceTableFailedToUpdateError,
            expected_regex="Failed to update schema for `test_dataset.test_table`",
        ):
            self.source_table_update_manager.update(
                SourceTableCollection(
                    dataset_id="test_dataset",
                    source_tables_by_address={
                        self.table_address: self.updated_table_config
                    },
                    update_config=SourceTableCollectionUpdateConfig(
                        attempt_to_manage=True,
                        recreate_on_update_error=False,
                        allow_field_deletions=False,
                    ),
                    description="Description for dataset test_dataset",
                )
            )

    def test_recreate_true_recreates(self) -> None:
        self.source_table_update_manager.update(
            SourceTableCollection(
                dataset_id="test_dataset",
                update_config=SourceTableCollectionUpdateConfig(
                    attempt_to_manage=True,
                    recreate_on_update_error=True,
                    allow_field_deletions=False,
                ),
                source_tables_by_address={
                    self.table_address: self.updated_table_config
                },
                description="Description for dataset test_dataset",
            ),
        )

        table = self.bq_client.get_table(self.table_address)
        self.assertEqual(table.schema, self.updated_table_config.schema_fields)


@pytest.mark.uses_bq_emulator
class SourceTableUpdateManagerDryRunTest(BigQueryEmulatorTestCase):
    """Tests basic functionality of the SourceTableUpdateManager.dry_run function"""

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        collection = SourceTableCollection(
            dataset_id="test_dataset",
            description="Description for dataset test_dataset",
        )
        collection.add_source_table(
            "test_table_unmodified",
            schema_fields=[
                SchemaField("id", "INTEGER"),
                SchemaField("unused_column", "STRING"),
            ],
        )
        collection.add_source_table(
            "test_table_modified", schema_fields=[SchemaField("id", "INTEGER")]
        )
        return [collection]

    def test_dry_run_results(self) -> None:
        """Tests that the source tables changes are reflected in the dry run results"""
        collection = self.get_source_tables()[0]

        # Modify a table to test changes
        collection.add_source_table(
            "test_table_modified",
            [
                SchemaField("id", "INTEGER"),
                SchemaField("new_field", "STRING"),
            ],
        )
        update_manager = SourceTableUpdateManager(client=self.bq_client)

        with tempfile.NamedTemporaryFile() as file:
            changes = update_manager.dry_run(
                source_table_collections=[collection],
                log_file=file.name,
            )

        self.assertEqual(
            dict(changes),
            {
                SourceTableDryRunResult.UPDATE_SCHEMA_WITH_ADDITIONS: [
                    BigQueryAddress(
                        dataset_id="test_dataset", table_id="test_table_modified"
                    )
                ],
            },
        )

    def test_table_minimum_required_columns(self) -> None:
        collection = attr.evolve(
            self.get_source_tables()[0],
            update_config=SourceTableCollectionUpdateConfig.unmanaged(),
            validation_config=SourceTableCollectionValidationConfig(
                only_check_required_columns=True,
            ),
        )

        # Update test_table_unmodified to only require the id column
        collection.add_source_table(
            "test_table_unmodified",
            [
                SchemaField("id", "INTEGER"),
            ],
        )

        # Modify a table to test changes
        collection.add_source_table(
            "test_table_modified",
            [
                SchemaField("id", "INTEGER"),
                SchemaField("new_field", "STRING"),
            ],
        )
        update_manager = SourceTableUpdateManager(client=self.bq_client)

        with tempfile.NamedTemporaryFile() as file:
            changes = update_manager.dry_run(
                source_table_collections=[collection],
                log_file=file.name,
            )

            self.assertEqual(
                dict(changes),
                {
                    SourceTableDryRunResult.UPDATE_SCHEMA_WITH_CHANGES: [
                        BigQueryAddress(
                            dataset_id="test_dataset", table_id="test_table_modified"
                        )
                    ]
                    # test_table_unmodified had no changes as all required columns existed
                },
            )
