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
import unittest
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
    SourceTableFailedToUpdateError,
    SourceTableUpdateManager,
    SourceTableUpdateType,
    SourceTableWithRequiredUpdateTypes,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)

_DATASET_1 = "dataset_1"
_TABLE_1 = "table_1"


class TestSourceTableUpdateType(unittest.TestCase):
    """Tests for SourceTableUpdateType"""

    def test_is_single_existing_field_update_type(self) -> None:
        for t in SourceTableUpdateType:
            self.assertIsInstance(t.is_single_existing_field_update_type, bool)

    def test_is_allowed_update_for_config(self) -> None:
        possible_update_configs = [
            SourceTableCollectionUpdateConfig.regenerable(),
            SourceTableCollectionUpdateConfig.externally_managed(),
            SourceTableCollectionUpdateConfig.protected(),
        ]

        for update_type in SourceTableUpdateType:
            for update_config in possible_update_configs:
                update_type.is_allowed_update_for_config(update_config)


class TestSourceTableWithRequiredUpdateTypes(unittest.TestCase):
    """Tests for SourceTableWithRequiredUpdateTypes"""

    def _make_schema_field(
        self,
        name: str,
        field_type: str = "STRING",
        mode: str = "NULLABLE",
        description: str | None = None,
    ) -> bigquery.SchemaField:
        if description is not None:
            return bigquery.SchemaField(
                name=name, field_type=field_type, mode=mode, description=description
            )
        return bigquery.SchemaField(name=name, field_type=field_type, mode=mode)

    def _make_table(
        self, address: BigQueryAddress, schema: list[bigquery.SchemaField]
    ) -> bigquery.Table:
        table = bigquery.Table(
            address.to_project_specific_address(project_id="recidiviz-456").to_str()
        )
        table.schema = schema
        return table

    def test_no_updates(self) -> None:
        address = BigQueryAddress.from_str("dataset.table")
        deployed_schema = [self._make_schema_field("id")]
        new_schema = [self._make_schema_field("id")]

        update_info = SourceTableWithRequiredUpdateTypes(
            deployed_table=self._make_table(address, deployed_schema),
            table_level_update_types=set(),
            existing_field_update_types={},
            source_table_config=SourceTableConfig(
                address=BigQueryAddress.from_str("dataset.table"),
                description="",
                schema_fields=new_schema,
                clustering_fields=None,
            ),
        )

        with self.assertRaisesRegex(
            ValueError, r"Did not expect to build results without any update_types"
        ):
            update_info.build_updates_message()

        self.assertFalse(update_info.has_updates_to_make)
        self.assertEqual(update_info.all_update_types, set())

    def test_mismatch_clustering_fields(self) -> None:
        address = BigQueryAddress.from_str("dataset.table")
        deployed_schema = [
            self._make_schema_field("id"),
            self._make_schema_field("category"),
        ]
        new_schema = deployed_schema

        deployed_table = self._make_table(address, deployed_schema)
        deployed_table.clustering_fields = ["id"]
        new_clustering_fields = ["category"]

        update_info = SourceTableWithRequiredUpdateTypes(
            deployed_table=deployed_table,
            table_level_update_types={SourceTableUpdateType.MISMATCH_CLUSTERING_FIELDS},
            existing_field_update_types={},
            source_table_config=SourceTableConfig(
                address=BigQueryAddress.from_str("dataset.table"),
                description="",
                schema_fields=new_schema,
                clustering_fields=new_clustering_fields,
            ),
        )

        expected_message = "* dataset.table (MISMATCH_CLUSTERING_FIELDS)"
        self.assertEqual(update_info.build_updates_message(), expected_message)

        self.assertTrue(update_info.has_updates_to_make)
        self.assertEqual(
            update_info.all_update_types,
            {SourceTableUpdateType.MISMATCH_CLUSTERING_FIELDS},
        )

    def test_create_table(self) -> None:
        new_schema = [self._make_schema_field("id")]

        update_info = SourceTableWithRequiredUpdateTypes(
            deployed_table=None,
            table_level_update_types={SourceTableUpdateType.CREATE_TABLE},
            existing_field_update_types={},
            source_table_config=SourceTableConfig(
                address=BigQueryAddress.from_str("dataset.new_table"),
                description="A new table",
                schema_fields=new_schema,
                clustering_fields=None,
            ),
        )

        expected_message = "* dataset.new_table (CREATE_TABLE)"
        self.assertEqual(update_info.build_updates_message(), expected_message)

        self.assertTrue(update_info.has_updates_to_make)
        self.assertEqual(
            update_info.all_update_types, {SourceTableUpdateType.CREATE_TABLE}
        )

    def test_add_fields_exact_output(self) -> None:
        address = BigQueryAddress.from_str("dataset.table")
        deployed_schema = [self._make_schema_field("id")]
        new_schema = [
            self._make_schema_field("id"),
            self._make_schema_field("new_field"),
        ]

        update_info = SourceTableWithRequiredUpdateTypes(
            deployed_table=self._make_table(address, deployed_schema),
            table_level_update_types={
                SourceTableUpdateType.UPDATE_SCHEMA_WITH_ADDITIONS
            },
            existing_field_update_types={},
            source_table_config=SourceTableConfig(
                address=address,
                description="",
                schema_fields=new_schema,
                clustering_fields=None,
            ),
        )

        expected_message = (
            "* dataset.table (UPDATE_SCHEMA_WITH_ADDITIONS)\n"
            "  Added fields:\n"
            "    - new_field"
        )
        self.assertEqual(update_info.build_updates_message(), expected_message)

        self.assertTrue(update_info.has_updates_to_make)
        self.assertEqual(
            update_info.all_update_types,
            {SourceTableUpdateType.UPDATE_SCHEMA_WITH_ADDITIONS},
        )

    def test_delete_fields_exact_output(self) -> None:
        address = BigQueryAddress.from_str("dataset.table")

        deployed_schema = [
            self._make_schema_field("id"),
            self._make_schema_field("old_field"),
        ]
        new_schema = [self._make_schema_field("id")]

        update_info = SourceTableWithRequiredUpdateTypes(
            deployed_table=self._make_table(address, deployed_schema),
            table_level_update_types={
                SourceTableUpdateType.UPDATE_SCHEMA_WITH_DELETIONS
            },
            existing_field_update_types={},
            source_table_config=SourceTableConfig(
                address=address,
                description="",
                schema_fields=new_schema,
                clustering_fields=None,
            ),
        )

        expected_message = (
            "* dataset.table (UPDATE_SCHEMA_WITH_DELETIONS)\n"
            "  Deleted fields:\n"
            "    - old_field"
        )
        self.assertEqual(update_info.build_updates_message(), expected_message)

        self.assertTrue(update_info.has_updates_to_make)
        self.assertEqual(
            update_info.all_update_types,
            {SourceTableUpdateType.UPDATE_SCHEMA_WITH_DELETIONS},
        )

    def test_field_type_and_mode_changes(self) -> None:
        address = BigQueryAddress.from_str("dataset.table")
        deployed_schema = [
            self._make_schema_field("id", field_type="STRING", mode="REQUIRED"),
        ]
        new_schema = [
            self._make_schema_field("id", field_type="INTEGER", mode="NULLABLE"),
        ]

        update_info = SourceTableWithRequiredUpdateTypes(
            deployed_table=self._make_table(address, deployed_schema),
            table_level_update_types=set(),
            existing_field_update_types={
                "id": {
                    SourceTableUpdateType.UPDATE_SCHEMA_TYPE_CHANGES,
                    SourceTableUpdateType.UPDATE_SCHEMA_MODE_CHANGES,
                }
            },
            source_table_config=SourceTableConfig(
                address=address,
                description="",
                schema_fields=new_schema,
                clustering_fields=None,
            ),
        )

        expected_message = (
            "* dataset.table (UPDATE_SCHEMA_MODE_CHANGES, UPDATE_SCHEMA_TYPE_CHANGES)\n"
            "  Changed fields:\n"
            "    - 'id' changed MODE from REQUIRED --> NULLABLE\n"
            "    - 'id' changed TYPE from STRING --> INTEGER"
        )
        self.assertEqual(update_info.build_updates_message(), expected_message)

        self.assertTrue(update_info.has_updates_to_make)
        self.assertEqual(
            update_info.all_update_types,
            {
                SourceTableUpdateType.UPDATE_SCHEMA_TYPE_CHANGES,
                SourceTableUpdateType.UPDATE_SCHEMA_MODE_CHANGES,
            },
        )

    def test_documentation_change(self) -> None:
        address = BigQueryAddress.from_str("dataset.table")
        deployed_schema = [self._make_schema_field("id", description="Old desc")]
        new_schema = [self._make_schema_field("id", description="New desc")]

        update_info = SourceTableWithRequiredUpdateTypes(
            deployed_table=self._make_table(address, deployed_schema),
            table_level_update_types=set(),
            existing_field_update_types={
                "id": {SourceTableUpdateType.DOCUMENTATION_CHANGE}
            },
            source_table_config=SourceTableConfig(
                address=address,
                description="",
                schema_fields=new_schema,
                clustering_fields=None,
            ),
        )

        expected_message = (
            "* dataset.table (DOCUMENTATION_CHANGE)\n"
            "  Changed fields:\n"
            "    - 'id' updated its DESCRIPTION to New desc"
        )
        self.assertEqual(update_info.build_updates_message(), expected_message)

        self.assertTrue(update_info.has_updates_to_make)
        self.assertEqual(
            update_info.all_update_types, {SourceTableUpdateType.DOCUMENTATION_CHANGE}
        )

    def test_combined_addition_and_field_change(self) -> None:
        address = BigQueryAddress.from_str("dataset.table")
        deployed_schema = [
            self._make_schema_field("id", field_type="STRING"),
        ]
        new_schema = [
            self._make_schema_field("id", field_type="INTEGER"),
            self._make_schema_field("new_field"),
        ]

        update_info = SourceTableWithRequiredUpdateTypes(
            deployed_table=self._make_table(address, deployed_schema),
            table_level_update_types={
                SourceTableUpdateType.UPDATE_SCHEMA_WITH_ADDITIONS
            },
            existing_field_update_types={
                "id": {SourceTableUpdateType.UPDATE_SCHEMA_TYPE_CHANGES}
            },
            source_table_config=SourceTableConfig(
                address=address,
                description="",
                schema_fields=new_schema,
                clustering_fields=None,
            ),
        )

        expected_message = (
            "* dataset.table (UPDATE_SCHEMA_TYPE_CHANGES, UPDATE_SCHEMA_WITH_ADDITIONS)\n"
            "  Added fields:\n"
            "    - new_field\n"
            "  Changed fields:\n"
            "    - 'id' changed TYPE from STRING --> INTEGER"
        )
        self.assertEqual(update_info.build_updates_message(), expected_message)

        self.assertTrue(update_info.has_updates_to_make)
        self.assertEqual(
            update_info.all_update_types,
            {
                SourceTableUpdateType.UPDATE_SCHEMA_WITH_ADDITIONS,
                SourceTableUpdateType.UPDATE_SCHEMA_TYPE_CHANGES,
            },
        )


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
            update_config=SourceTableCollectionUpdateConfig.protected(),
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
            update_config=SourceTableCollectionUpdateConfig.protected(),
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
            update_config=SourceTableCollectionUpdateConfig.protected(),
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
                    update_config=SourceTableCollectionUpdateConfig.protected(),
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
            update_config=SourceTableCollectionUpdateConfig.protected(),
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
                update_config=SourceTableCollectionUpdateConfig.protected(),
                description="Description for dataset test_dataset",
            ),
        )

    # TODO(#37283) re-add when emulator supports partitioning
    def _test_partitioning_fields(self) -> None:
        """test for update manager's handling of time partitioning"""
        table_address = BigQueryAddress(
            dataset_id="test_dataset", table_id="test_table"
        )

        table_config = SourceTableConfig(
            address=table_address,
            schema_fields=[
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("dt", "DATETIME"),
            ],
            description="partitioned table",
            time_partitioning=bigquery.TimePartitioning(field="dt"),
        )
        with_partitioning = SourceTableCollection(
            dataset_id="test_dataset",
            source_tables_by_address={table_address: table_config},
            update_config=SourceTableCollectionUpdateConfig.protected(),
            description="Description for dataset test_dataset",
        )
        self.source_table_update_manager.update(
            source_table_collection=with_partitioning
        )

        # update a second time should be a no-op
        self.source_table_update_manager.update(
            source_table_collection=SourceTableCollection(
                dataset_id="test_dataset",
                source_tables_by_address={table_address: table_config},
                update_config=SourceTableCollectionUpdateConfig.protected(),
                description="Description for dataset test_dataset",
            )
        )

        with self.assertRaisesRegex(
            SourceTableFailedToUpdateError, "has time partitioning.+that does not match"
        ):
            self.source_table_update_manager.update(
                source_table_collection=SourceTableCollection(
                    dataset_id="test_dataset",
                    source_tables_by_address={
                        table_address: attr.evolve(
                            table_config,
                            # should fail
                            time_partitioning=bigquery.TimePartitioning(),
                        )
                    },
                    update_config=SourceTableCollectionUpdateConfig.protected(),
                    description="Description for dataset test_dataset",
                )
            )
        with self.assertRaisesRegex(
            SourceTableFailedToUpdateError, "has time partitioning.+that does not match"
        ):
            self.source_table_update_manager.update(
                source_table_collection=SourceTableCollection(
                    dataset_id="test_dataset",
                    source_tables_by_address={
                        table_address: attr.evolve(
                            table_config,
                            # should fail
                            time_partitioning=None,
                        )
                    },
                    update_config=SourceTableCollectionUpdateConfig.protected(),
                    description="Description for dataset test_dataset",
                )
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
                update_config=SourceTableCollectionUpdateConfig.protected(),
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

    wipe_emulator_data_on_teardown = False

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        collection = SourceTableCollection(
            dataset_id="test_dataset",
            description="Description for dataset test_dataset",
            update_config=SourceTableCollectionUpdateConfig.protected(),
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

        modified_table_address = BigQueryAddress(
            dataset_id="test_dataset", table_id="test_table_modified"
        )

        # Modify a table to test changes
        collection.add_source_table(
            "test_table_modified",
            [
                SchemaField("id", "INTEGER"),
                SchemaField("new_field", "STRING"),
            ],
        )

        modified_table_config = collection.source_tables_by_address[
            modified_table_address
        ]
        update_manager = SourceTableUpdateManager(client=self.bq_client)

        with tempfile.NamedTemporaryFile() as file:
            changes = update_manager.get_changes_to_apply_to_source_tables(
                source_table_collections=[collection],
                log_file=file.name,
            )

        self.assertEqual(
            changes,
            {
                modified_table_address: SourceTableWithRequiredUpdateTypes(
                    source_table_config=modified_table_config,
                    table_level_update_types={
                        SourceTableUpdateType.UPDATE_SCHEMA_WITH_ADDITIONS
                    },
                    existing_field_update_types={},
                    deployed_table=bigquery.Table(
                        table_ref=bigquery.TableReference(
                            bigquery.DatasetReference(
                                "recidiviz-bq-emulator-project", "test_dataset"
                            ),
                            "test_table_modified",
                        )
                    ),
                )
            },
        )

        address = one(changes.keys())
        expected_update_message = """* test_dataset.test_table_modified (UPDATE_SCHEMA_WITH_ADDITIONS)
  Added fields:
    - new_field"""
        self.assertEqual(
            expected_update_message, changes[address].build_updates_message()
        )

    def test_table_minimum_required_columns(self) -> None:
        collection = attr.evolve(
            self.get_source_tables()[0],
            update_config=SourceTableCollectionUpdateConfig.externally_managed(),
            validation_config=SourceTableCollectionValidationConfig(
                only_check_required_columns=True,
            ),
        )

        unmodified_table_address = BigQueryAddress(
            dataset_id="test_dataset", table_id="test_table_modified"
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

        modified_table_config = collection.source_tables_by_address[
            unmodified_table_address
        ]

        update_manager = SourceTableUpdateManager(client=self.bq_client)

        with tempfile.NamedTemporaryFile() as file:
            changes = update_manager.get_changes_to_apply_to_source_tables(
                source_table_collections=[collection],
                log_file=file.name,
            )

            self.assertEqual(
                {
                    unmodified_table_address: SourceTableWithRequiredUpdateTypes(
                        source_table_config=modified_table_config,
                        table_level_update_types={
                            SourceTableUpdateType.UPDATE_SCHEMA_WITH_ADDITIONS
                        },
                        existing_field_update_types={},
                        deployed_table=bigquery.Table(
                            bigquery.TableReference(
                                bigquery.DatasetReference(
                                    "recidiviz-bq-emulator-project", "test_dataset"
                                ),
                                "test_table_modified",
                            )
                        ),
                    )
                    # test_table_unmodified had no changes as all required columns existed
                },
                changes,
            )
            address = one(changes.keys())

            expected_update_message = """* test_dataset.test_table_modified (UPDATE_SCHEMA_WITH_ADDITIONS)
  Added fields:
    - new_field"""
            self.assertEqual(
                expected_update_message, changes[address].build_updates_message()
            )
