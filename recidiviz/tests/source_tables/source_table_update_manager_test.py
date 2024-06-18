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
from typing import Any
from unittest.mock import patch

import attr
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
    SourceTableConfig,
)
from recidiviz.source_tables.source_table_update_manager import (
    SourceTableCollectionUpdateConfig,
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
        self.repository = build_source_table_repository_for_collected_schemata()
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
            update_config=SourceTableCollectionUpdateConfig(
                source_table_collection=self.source_table_collection,
                allow_field_deletions=False,
            )
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
        )

        self.source_table_update_manager.update(
            update_config=SourceTableCollectionUpdateConfig(
                source_table_collection=with_extraneous_field,
                allow_field_deletions=False,
            )
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
        )

        with self.assertRaises(ValueError):
            self.source_table_update_manager.update(
                update_config=SourceTableCollectionUpdateConfig(
                    source_table_collection=without_extraneous_field,
                    allow_field_deletions=False,
                )
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
        )
        self.source_table_update_manager.update(
            update_config=SourceTableCollectionUpdateConfig(
                source_table_collection=with_clustering,
                allow_field_deletions=False,
            )
        )

        with self.assertRaisesRegex(
            SourceTableFailedToUpdateError, "has clustering fields.+that do not match"
        ):
            self.source_table_update_manager.update(
                SourceTableCollectionUpdateConfig(
                    source_table_collection=SourceTableCollection(
                        dataset_id="test_dataset",
                        source_tables_by_address={
                            table_address: attr.evolve(
                                table_config,
                                clustering_fields=None,
                            )
                        },
                    ),
                    allow_field_deletions=False,
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
        )
        self.source_table_update_manager.update(
            update_config=SourceTableCollectionUpdateConfig(
                source_table_collection=collection,
                allow_field_deletions=False,
            )
        )

        # Assert no errors
        self.source_table_update_manager.update(
            SourceTableCollectionUpdateConfig(
                source_table_collection=SourceTableCollection(
                    dataset_id="test_dataset",
                    source_tables_by_address={
                        table_address: attr.evolve(table_config, clustering_fields=[])
                    },
                ),
                allow_field_deletions=False,
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
                SourceTableCollectionUpdateConfig(
                    source_table_collection=SourceTableCollection(
                        dataset_id="test_dataset",
                        source_tables_by_address={
                            self.table_address: self.updated_table_config
                        },
                    ),
                    recreate_on_update_error=False,
                    allow_field_deletions=False,
                )
            )

    def test_recreate_true_recreates(self) -> None:
        self.source_table_update_manager.update(
            SourceTableCollectionUpdateConfig(
                source_table_collection=SourceTableCollection(
                    dataset_id="test_dataset",
                    source_tables_by_address={
                        self.table_address: self.updated_table_config
                    },
                ),
                recreate_on_update_error=True,
                allow_field_deletions=False,
            )
        )

        table = self.bq_client.get_table(
            self.bq_client.dataset_ref_for_id(self.table_address.dataset_id),
            self.table_address.table_id,
        )
        self.assertEqual(table.schema, self.updated_table_config.schema_fields)
