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
from typing import List

import sqlalchemy

from recidiviz.big_query.big_query_utils import schema_for_sqlalchemy_table
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_all_source_table_datasets,
)


class CloudSqlToBQConfigTest(unittest.TestCase):
    """Tests for cloud_sql_to_bq_export_config.py."""

    def setUp(self) -> None:
        self.schema_types: List[SchemaType] = list(SchemaType)
        self.enabled_schema_types = [
            schema_type
            for schema_type in self.schema_types
            if CloudSqlToBQConfig.is_valid_schema_type(schema_type)
        ]

        self.view_source_table_datasets = get_all_source_table_datasets()

    def test_for_schema_type_raises_error(self) -> None:
        with self.assertRaises(ValueError):
            CloudSqlToBQConfig.for_schema_type(
                "random-schema-type"  # type: ignore[arg-type]
            )

    def test_for_schema_type_returns_instance(self) -> None:
        for schema_type in self.schema_types:
            if not CloudSqlToBQConfig.is_valid_schema_type(schema_type):
                with self.assertRaises(ValueError):
                    _ = CloudSqlToBQConfig.for_schema_type(schema_type)
            else:
                config = CloudSqlToBQConfig.for_schema_type(schema_type)
                self.assertIsInstance(config, CloudSqlToBQConfig)

    def test_regional_dataset(self) -> None:
        for schema_type in self.enabled_schema_types:
            config = CloudSqlToBQConfig.for_schema_type(schema_type)
            dataset = config.regional_dataset(dataset_override_prefix=None)
            self.assertTrue(dataset.endswith("regional"))
            self.assertTrue(dataset not in self.view_source_table_datasets)

            dataset_with_prefix = config.regional_dataset(
                dataset_override_prefix="prefix"
            )
            self.assertTrue(dataset_with_prefix.startswith("prefix_"))
            self.assertTrue(dataset_with_prefix.endswith("regional"))
            self.assertTrue(dataset_with_prefix not in self.view_source_table_datasets)

    def test_multi_region_dataset(self) -> None:
        for schema_type in self.enabled_schema_types:
            config = CloudSqlToBQConfig.for_schema_type(schema_type)
            dataset = config.multi_region_dataset(dataset_override_prefix=None)
            self.assertFalse(dataset.endswith("regional"))
            self.assertTrue(dataset in self.view_source_table_datasets)

            dataset_with_prefix = config.multi_region_dataset(
                dataset_override_prefix="prefix"
            )
            self.assertTrue(dataset_with_prefix.startswith("prefix_"))
            self.assertFalse(dataset_with_prefix.endswith("regional"))
            self.assertTrue(dataset_with_prefix not in self.view_source_table_datasets)

    def test_get_tables_to_export(self) -> None:
        """Assert that get_tables_to_export returns a list of type sqlalchemy.Table"""
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

            dataset_id = config.multi_region_dataset(dataset_override_prefix=None)
            self.assertIsInstance(dataset_id, str)

            for char in dataset_id:
                self.assertIn(char, allowed_characters)

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
