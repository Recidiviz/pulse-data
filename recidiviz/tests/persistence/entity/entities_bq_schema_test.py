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
"""Tests for entities_bq_schema.py"""
import unittest

from google.cloud.bigquery import SchemaField
from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import (
    normalized_entities as normalized_state_entities,
)
from recidiviz.source_tables.ingest_pipeline_output_table_collector import (
    build_ingest_pipeline_output_source_table_collections,
)
from recidiviz.source_tables.normalization_pipeline_output_table_collector import (
    build_normalization_pipeline_output_source_table_collections,
)
from recidiviz.source_tables.source_table_config import (
    IngestPipelineEntitySourceTableLabel,
    NormalizedStateSpecificEntitySourceTableLabel,
)
from recidiviz.source_tables.union_tables_output_table_collector import (
    build_unioned_normalized_state_source_table_collection,
    build_unioned_state_source_table_collection,
)
from recidiviz.tests.persistence.entity import fake_entities


class TestGetBqSchemaForEntitiesModule(unittest.TestCase):
    """Tests for entities_bq_schema.py"""

    def _compare_schemas(
        self,
        expected_table_to_schema: dict[str, list[SchemaField]],
        table_to_schema: dict[str, list[SchemaField]],
    ) -> None:
        """Asserts that the two provided schemas are identical."""
        expected_tables = set(expected_table_to_schema)
        tables = set(table_to_schema)

        for missing_table in expected_tables - tables:
            raise ValueError(f"Missing expected table [{missing_table}]")

        for extra_table in tables - expected_tables:
            raise ValueError(f"Found extra unexpected table [{extra_table}]")

        for table in sorted(tables.intersection(expected_tables)):
            expected_schema = expected_table_to_schema[table]
            schema = table_to_schema[table]

            expected_cols = {c.name for c in expected_schema}
            cols = {c.name for c in schema}

            for missing_col in expected_cols - cols:
                raise ValueError(
                    f"Missing expected column [{missing_col}] in table [{table}]"
                )

            for extra_col in cols - expected_cols:
                raise ValueError(f"Found extra column [{extra_col}] in table [{table}]")

            for col in cols.intersection(expected_cols):
                expected_field = one(f for f in expected_schema if f.name == col)
                field = one(f for f in schema if f.name == col)

                self.assertEqual(
                    expected_field.field_type,
                    field.field_type,
                    f"Unexpected field type [{field.field_type}] for field "
                    f"[{field.name}] on table [{table}]",
                )
                self.assertEqual(
                    expected_field.mode,
                    field.mode,
                    f"Unexpected field mode [{field.mode}] for field "
                    f"[{field.name}] on table [{table}]",
                )

    def test_bq_schema_for_entities_module(self) -> None:
        expected_schema = {
            "fake_another_entity": [
                SchemaField("state_code", "STRING", "NULLABLE"),
                SchemaField("another_entity_id", "INTEGER", "NULLABLE"),
                SchemaField("another_name", "STRING", "NULLABLE"),
                SchemaField("fake_person_id", "INTEGER", "NULLABLE"),
            ],
            "fake_another_entity_fake_entity_association": [
                SchemaField("state_code", "STRING", "NULLABLE"),
                SchemaField("fake_another_entity_id", "INTEGER", "NULLABLE"),
                SchemaField("fake_entity_id", "INTEGER", "NULLABLE"),
            ],
            "fake_entity": [
                SchemaField("state_code", "STRING", "NULLABLE"),
                SchemaField("entity_id", "INTEGER", "NULLABLE"),
                SchemaField("name", "STRING", "NULLABLE"),
                SchemaField("fake_person_id", "INTEGER", "NULLABLE"),
            ],
            "fake_person": [
                SchemaField("state_code", "STRING", "NULLABLE"),
                SchemaField("fake_person_id", "INTEGER", "NULLABLE"),
                SchemaField("full_name", "STRING", "NULLABLE"),
            ],
        }
        schema = get_bq_schema_for_entities_module(fake_entities)
        self.assertEqual(expected_schema, schema)

    def test_bq_schema_for_entities_module_state(self) -> None:
        # Does not crash
        _ = get_bq_schema_for_entities_module(state_entities)

    def test_bq_schema_for_entities_module_normalized_state(self) -> None:
        # Does not crash
        _ = get_bq_schema_for_entities_module(normalized_state_entities)

    def test_parity_with_source_table_collection_us_xx_state(self) -> None:
        """Tests that get_bq_schema_for_entities_module() creates a schema that
        matches the current schemas defined for our `us_xx_state` ingest pipeline output
        datasets.
        """
        state_collection = one(
            c
            for c in build_ingest_pipeline_output_source_table_collections()
            if c.has_label(
                # Pick an arbitrary state's ingest pipeline output schema to test
                IngestPipelineEntitySourceTableLabel(state_code=StateCode.US_OZ)
            )
        )
        expected_table_to_schema = {
            t.address.table_id: t.schema_fields for t in state_collection.source_tables
        }
        table_to_schema = get_bq_schema_for_entities_module(state_entities)

        self._compare_schemas(expected_table_to_schema, table_to_schema)

    def test_parity_with_source_table_collection_state(self) -> None:
        """Tests that get_bq_schema_for_entities_module() creates a schema that
        matches the current schemas defined for our `state` unioned dataset.
        """
        state_collection = build_unioned_state_source_table_collection()
        expected_table_to_schema = {
            t.address.table_id: t.schema_fields for t in state_collection.source_tables
        }
        table_to_schema = get_bq_schema_for_entities_module(state_entities)

        self._compare_schemas(expected_table_to_schema, table_to_schema)

    def test_parity_with_source_table_collection_us_xx_normalized_state_legacy(
        self,
    ) -> None:
        """Tests that get_bq_schema_for_entities_module() creates a schema that
        matches the current schema defined for the `us_xx_normalized_state` legacy
        normalization pipeline outputs.

        Note: When the ingest pipeline outputs normalized entities it will write ALL
        tables to `us_xx_normalized_state`.
        """

        state_collection = one(
            c
            for c in build_normalization_pipeline_output_source_table_collections()
            if c.has_label(
                # Pick an arbitrary state's ingest pipeline output schema to test
                NormalizedStateSpecificEntitySourceTableLabel(
                    state_code=StateCode.US_OZ
                )
            )
        )

        expected_table_to_schema = {
            t.address.table_id: t.schema_fields for t in state_collection.source_tables
        }
        table_to_schema = get_bq_schema_for_entities_module(normalized_state_entities)

        # The legacy normalization pipelines only output to a subset of tables. We check
        # that for those tables the schemas are identical.
        for table, expected_schema in expected_table_to_schema.items():
            self._compare_schemas(
                {table: expected_schema}, {table: table_to_schema[table]}
            )

    def test_parity_with_source_table_collection_us_xx_normalized_state_new(
        self,
    ) -> None:
        """Tests that get_bq_schema_for_entities_module() creates a schema that
        matches the current schemas defined for our `us_xx_normalized_state*` ingest
        pipeline output datasets.
        """
        state_collection = one(
            c
            for c in build_ingest_pipeline_output_source_table_collections()
            if c.has_label(
                # Pick an arbitrary state's ingest pipeline output schema to test
                NormalizedStateSpecificEntitySourceTableLabel(
                    state_code=StateCode.US_OZ
                )
            )
        )
        expected_table_to_schema = {
            t.address.table_id: t.schema_fields for t in state_collection.source_tables
        }

        table_to_schema = get_bq_schema_for_entities_module(normalized_state_entities)

        self._compare_schemas(expected_table_to_schema, table_to_schema)

    def test_parity_with_source_table_collection_normalized_state(self) -> None:
        expected_table_to_schema = {
            t.address.table_id: t.schema_fields
            for t in build_unioned_normalized_state_source_table_collection().source_tables
        }

        table_to_schema = get_bq_schema_for_entities_module(normalized_state_entities)

        self._compare_schemas(expected_table_to_schema, table_to_schema)
