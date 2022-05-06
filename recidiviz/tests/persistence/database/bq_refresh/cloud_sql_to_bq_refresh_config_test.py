# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Tests for cloud_sql_to_bq_export_config.py."""

import string
import unittest
from typing import Dict, List
from unittest import mock

import sqlalchemy
from parameterized import parameterized

from recidiviz.big_query.big_query_utils import schema_for_sqlalchemy_table
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS


class CloudSqlToBQConfigTest(unittest.TestCase):
    """Tests for cloud_sql_to_bq_export_config.py."""

    def setUp(self) -> None:
        self.schema_types: List[SchemaType] = list(SchemaType)
        self.enabled_schema_types = [
            schema_type
            for schema_type in self.schema_types
            if CloudSqlToBQConfig.is_valid_schema_type(schema_type)
        ]
        self.mock_project_id = "fake-recidiviz-project"
        self.metadata_patcher = mock.patch(
            "recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config.metadata"
        )
        self.mock_metadata = self.metadata_patcher.start()
        self.mock_metadata.project_id.return_value = self.mock_project_id

        self.gcs_factory_patcher = mock.patch(
            "recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config.GcsfsFactory.build"
        )
        self.fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = self.fake_gcs
        self.set_config_yaml(
            """
region_codes_to_exclude:
  - US_ND
state_history_tables_to_include:
  - state_person_history
county_columns_to_exclude:
  person:
    - full_name
    - birthdate_inferred_from_age
"""
        )

    def tearDown(self) -> None:
        self.metadata_patcher.stop()
        self.gcs_factory_patcher.stop()

    def test_for_schema_type_raises_error(self) -> None:
        with self.assertRaises(ValueError):
            CloudSqlToBQConfig.for_schema_type("random-schema-type")  # type: ignore[arg-type]

    def test_incorrect_direct_ingest_instance_raises(self) -> None:
        for schema_type in self.enabled_schema_types:
            if schema_type != SchemaType.STATE:
                with self.assertRaises(ValueError):
                    _ = CloudSqlToBQConfig.for_schema_type(
                        schema_type, DirectIngestInstance.PRIMARY
                    )

    def test_for_schema_type_returns_instance(self) -> None:
        for schema_type in self.schema_types:
            if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
                with self.assertRaises(ValueError):
                    _ = CloudSqlToBQConfig.for_schema_type(schema_type)
            else:
                config = CloudSqlToBQConfig.for_schema_type(schema_type)
                self.assertIsInstance(config, CloudSqlToBQConfig)

    def test_is_state_segmented_refresh_schema(self) -> None:
        for schema_type in self.enabled_schema_types:
            config = CloudSqlToBQConfig.for_schema_type(schema_type)
            is_state_segmented = config.is_state_segmented_refresh_schema()
            if schema_type == SchemaType.STATE:
                self.assertTrue(is_state_segmented)

    def test_unioned_regional_dataset(self) -> None:
        for schema_type in self.enabled_schema_types:
            config = CloudSqlToBQConfig.for_schema_type(schema_type)
            dataset = config.unioned_regional_dataset(dataset_override_prefix=None)
            self.assertTrue(dataset.endswith("regional"))
            self.assertTrue(dataset not in VIEW_SOURCE_TABLE_DATASETS)

            dataset_with_prefix = config.unioned_regional_dataset(
                dataset_override_prefix="prefix"
            )
            self.assertTrue(dataset_with_prefix.startswith("prefix_"))
            self.assertTrue(dataset_with_prefix.endswith("regional"))
            self.assertTrue(dataset_with_prefix not in VIEW_SOURCE_TABLE_DATASETS)

    def test_unioned_multi_region_dataset(self) -> None:
        for schema_type in self.enabled_schema_types:
            config = CloudSqlToBQConfig.for_schema_type(schema_type)
            dataset = config.unioned_multi_region_dataset(dataset_override_prefix=None)
            self.assertFalse(dataset.endswith("regional"))
            self.assertTrue(dataset in VIEW_SOURCE_TABLE_DATASETS)

            dataset_with_prefix = config.unioned_multi_region_dataset(
                dataset_override_prefix="prefix"
            )
            self.assertTrue(dataset_with_prefix.startswith("prefix_"))
            self.assertFalse(dataset_with_prefix.endswith("regional"))
            self.assertTrue(dataset_with_prefix not in VIEW_SOURCE_TABLE_DATASETS)

    def test_excluded_columns(self) -> None:
        for schema_type in self.enabled_schema_types:
            config = CloudSqlToBQConfig.for_schema_type(schema_type)
            for table in config.sorted_tables:
                # pylint: disable=protected-access
                columns = config._get_table_columns_to_export(table)
                for column in columns:
                    self.assertIsInstance(column, str)
                    self.assertTrue(
                        column not in config.columns_to_exclude.get(table.name, []),
                        msg=f"Column {column} should not be included. It is found in "
                        f"COUNTY_COLUMNS_TO_EXCLUDE` for this table {table.name}.",
                    )

    def test_get_tables_to_export(self) -> None:
        """Assertions for the method get_tables_to_export
        1. Assert that it returns a list of type sqlalchemy.Table
        2. For the StateBase schema, assert that included history tables are included
        3. For the StateBase schema, assert that other history tables are excluded
        """
        for schema_type in self.enabled_schema_types:
            config = CloudSqlToBQConfig.for_schema_type(schema_type)
            assert config is not None
            tables_to_export = config.get_tables_to_export()

            self.assertIsInstance(tables_to_export, List)

            for table in tables_to_export:
                self.assertIsInstance(table, sqlalchemy.Table)

    def test_dataset_id(self) -> None:
        """Make sure dataset_id is defined correctly.

        Checks that it is a string, checks that it has characters,
        and checks that those characters are letters, numbers, or _.
        """
        for schema_type in self.enabled_schema_types:
            config = CloudSqlToBQConfig.for_schema_type(schema_type)
            assert config is not None
            allowed_characters = set(string.ascii_letters + string.digits + "_")

            dataset_id = config.unioned_multi_region_dataset(
                dataset_override_prefix=None
            )
            self.assertIsInstance(dataset_id, str)

            for char in dataset_id:
                self.assertIn(char, allowed_characters)

    @mock.patch(
        "recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config.metadata.project_id",
        mock.Mock(return_value="a-new-fake-id"),
    )
    def test_incorrect_environment(self) -> None:
        with self.assertRaises(ValueError):
            config = CloudSqlToBQConfig.for_schema_type(SchemaType.STATE)
            assert config is not None
            self.assertEqual(config.region_codes_to_exclude, [])

    def test_column_to_exclude(self) -> None:
        """Make sure columns_to_exclude are defined correctly in case of typos.

        1) Check that all tables are defined in tables to export.

        2) Check that all columns are defined in their respective tables.
        """
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.JAILS)
        assert config is not None
        tables = config.get_tables_to_export()
        table_names = list(map(lambda t: t.name, tables))

        for table, _ in config.columns_to_exclude.items():
            self.assertTrue(
                table in table_names,
                msg=f'Table "{table}" in `cloud_sql_to_bq_export_config.COUNTY_COLUMNS_TO_EXCLUDE`'
                " not found in in the JailsBase schema."
                " Did you spell it correctly?",
            )

    @parameterized.expand(
        [
            (
                SchemaType.JAILS,
                [],
                {"person": ["full_name", "birthdate_inferred_from_age"]},
                [],
            ),
            (SchemaType.OPERATIONS, ["US_ND"], {}, []),
            (SchemaType.STATE, ["US_ND"], {}, ["state_person_history"]),
        ]
    )
    def test_yaml_config_reads_correctly_JAILS(
        self,
        schema: SchemaType,
        regions_to_exclude: List[str],
        columns_to_exclude: Dict[str, List[str]],
        history_tables_to_include: List[str],
    ) -> None:
        config = CloudSqlToBQConfig.for_schema_type(schema)
        assert config is not None

        self.assertListsDistinctAndEqual(
            regions_to_exclude,
            config.region_codes_to_exclude,
            msg_prefix="Region codes",
        )
        self.assertListsDistinctAndEqual(
            history_tables_to_include,
            config.history_tables_to_include,
            msg_prefix="History tables",
        )

        self.assertListsDistinctAndEqual(
            list(columns_to_exclude.keys()),
            list(config.columns_to_exclude.keys()),
            msg_prefix="Excluded columns keys",
        )
        for k in columns_to_exclude.keys():
            self.assertListsDistinctAndEqual(
                columns_to_exclude[k],
                config.columns_to_exclude[k],
                msg_prefix=f"Excluded columsn for {k}",
            )

    def test_schema_for_sqlalchemy_table(self) -> None:
        """Assert that we will be able to manage all tables in BigQuery created by the
        CloudSQL to BQ refresh."""
        for schema_type in self.enabled_schema_types:
            config = CloudSqlToBQConfig.for_schema_type(schema_type)
            for table in config.get_tables_to_export():
                # Assert that all column types are supported for this table
                _ = schema_for_sqlalchemy_table(table)

    def assertListsDistinctAndEqual(
        self, l1: List[str], l2: List[str], msg_prefix: str
    ) -> None:
        self.assertEqual(
            len(l1), len(l2), msg=f"{msg_prefix}: Lists have differing lengths"
        )
        self.assertEqual(
            len(l1),
            len(set(l1)),
            msg=f"{msg_prefix}: First list has duplicate elements",
        )
        self.assertEqual(
            len(l2),
            len(set(l2)),
            msg=f"{msg_prefix}: Second list has duplicate elements",
        )

        for elem in l1:
            self.assertTrue(
                elem in l2,
                msg=f"{msg_prefix}: Element {elem} present in first list but not second",
            )

    def set_config_yaml(self, contents: str) -> None:
        path = GcsfsFilePath.from_absolute_path(
            f"gs://{self.mock_project_id}-configs/cloud_sql_to_bq_config.yaml"
        )
        self.fake_gcs.upload_from_string(
            path=path, contents=contents, content_type="text/yaml"
        )
