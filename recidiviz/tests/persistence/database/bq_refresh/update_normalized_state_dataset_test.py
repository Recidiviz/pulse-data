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
"""Tests for update_normalized_state_dataset.py"""
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.bigquery import SchemaField
from more_itertools import one

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.bq_refresh import update_normalized_state_dataset
from recidiviz.persistence.database.bq_refresh.update_normalized_state_dataset import (
    combine_sources_into_single_normalized_state_dataset,
    get_normalized_state_view_builders,
)
from recidiviz.source_tables import (
    ingest_pipeline_output_table_collector,
    normalization_pipeline_output_table_collector,
)
from recidiviz.source_tables.ingest_pipeline_output_table_collector import (
    build_ingest_pipeline_output_source_table_collections,
)
from recidiviz.source_tables.normalization_pipeline_output_table_collector import (
    build_normalization_pipeline_output_source_table_collections,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.source_tables.union_tables_output_table_collector import (
    build_unioned_normalized_state_source_table_collection,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.persistence.entity import fake_entities, fake_normalized_entities

UPDATE_NORMALIZED_STATE_PACKAGE_NAME = update_normalized_state_dataset.__name__
INGEST_PIPELINE_OUTPUT_TABLE_COLLECTOR_PACKAGE_NAME = (
    ingest_pipeline_output_table_collector.__name__
)
NORMALIZATION_PIPELINE_OUTPUT_TABLE_COLLECTOR_PACKAGE_NAME = (
    normalization_pipeline_output_table_collector.__name__
)


@pytest.mark.uses_bq_emulator
class UpdateNormalizedStateDatasetIntegrationTest(BigQueryEmulatorTestCase):
    """Integration tests for update_normalized_state_dataset.py"""

    # We don't load any data in these tests, so don't bother wiping source tables
    wipe_emulator_data_on_teardown = False

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        return [
            # Input collections
            *build_normalization_pipeline_output_source_table_collections(),
            *build_ingest_pipeline_output_source_table_collections(),
            # Output collections
            build_unioned_normalized_state_source_table_collection(),
        ]

    def test_combine_sources_into_single_normalized_state_dataset(self) -> None:
        """Tests that combine_sources_into_single_normalized_state_dataset() does not
        crash when run against an emulator with real source tables loaded.
        """
        combine_sources_into_single_normalized_state_dataset(
            state_codes_filter=None,
            output_sandbox_prefix=None,
            input_dataset_overrides=None,
        )

    def _copy_source_tables_to_sandbox(
        self, original_dataset_id: str, sandbox_prefix: str
    ) -> str:
        sandbox_dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
            sandbox_prefix, original_dataset_id
        )
        self.bq_client.create_dataset_if_necessary(dataset_id=sandbox_dataset_id)
        collection = one(
            c for c in self.get_source_tables() if c.dataset_id == original_dataset_id
        )
        for source_table in collection.source_tables:
            self.bq_client.create_table_with_schema(
                dataset_id=BigQueryAddressOverrides.format_sandbox_dataset(
                    sandbox_prefix, original_dataset_id
                ),
                table_id=source_table.address.table_id,
                schema_fields=source_table.schema_fields,
            )
        return sandbox_dataset_id

    def test_combine_sources_into_single_normalized_state_dataset_output_prefix(
        self,
    ) -> None:
        """Tests that combine_sources_into_single_normalized_state_dataset() does not
        crash when run with an output prefix against an emulator with real source tables
        loaded.
        """
        output_sandbox_prefix = "my_prefix"
        input_sandbox_prefix = "another_prefix"

        # Mimic loading data into sandbox input datasets
        overrides_builder = BigQueryAddressOverrides.Builder(sandbox_prefix=None)
        for dataset_id in ["us_ca_normalized_state", "us_ca_normalized_state_new"]:
            sandbox_dataset_id = self._copy_source_tables_to_sandbox(
                original_dataset_id=dataset_id, sandbox_prefix=input_sandbox_prefix
            )
            overrides_builder.register_custom_dataset_override(
                dataset_id, sandbox_dataset_id
            )
        input_dataset_overrides = overrides_builder.build()

        # Load views
        combine_sources_into_single_normalized_state_dataset(
            state_codes_filter=[StateCode.US_CA],
            output_sandbox_prefix=output_sandbox_prefix,
            input_dataset_overrides=input_dataset_overrides,
        )

        # Check that the appropriate sandbox datasets are populated
        if not list(self.bq_client.list_tables("my_prefix_normalized_state_views")):
            raise ValueError(
                "Expected to find tables in dataset my_prefix_normalized_state_views"
            )

        if not list(self.bq_client.list_tables("my_prefix_normalized_state")):
            raise ValueError(
                "Expected to find tables in dataset my_prefix_normalized_state"
            )

        # Spot-check an arbitrary view to make sure the filter worked properly
        assessment_view = self.bq_client.get_table(
            "my_prefix_normalized_state_views", "state_assessment_view"
        )
        # The overridden input dataset should be used
        self.assertTrue(
            "another_prefix_us_ca_normalized_state" in assessment_view.view_query
        )
        # No US_PA input dataset should be referenced
        self.assertFalse("us_pa_normalized_state" in assessment_view.view_query)


class GetNormalizedStateViewBuildersTest(unittest.TestCase):
    """Tests for the get_normalized_state_view_builders() helper."""

    def setUp(self) -> None:
        states = [
            StateCode.US_XX,
            StateCode.US_YY,
        ]

        # Mock that this set of tables was normalized in legacy pipelines.
        # The fake_person table is omitted.
        legacy_normalization_output_schema = {
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
        }

        self.patchers: list[Any] = [
            patch(
                "recidiviz.utils.metadata.project_id",
                MagicMock(return_value="recidiviz-456"),
            ),
            patch(
                f"{UPDATE_NORMALIZED_STATE_PACKAGE_NAME}.get_direct_ingest_states_existing_in_env",
                MagicMock(return_value=states),
            ),
            patch(
                f"{UPDATE_NORMALIZED_STATE_PACKAGE_NAME}.build_normalization_pipeline_output_table_id_to_schemas",
                MagicMock(return_value=legacy_normalization_output_schema),
            ),
            patch(
                f"{INGEST_PIPELINE_OUTPUT_TABLE_COLLECTOR_PACKAGE_NAME}.get_direct_ingest_states_existing_in_env",
                MagicMock(return_value=states),
            ),
            patch(
                f"{INGEST_PIPELINE_OUTPUT_TABLE_COLLECTOR_PACKAGE_NAME}.entities",
                fake_entities,
            ),
            patch(
                f"{INGEST_PIPELINE_OUTPUT_TABLE_COLLECTOR_PACKAGE_NAME}.normalized_entities",
                fake_normalized_entities,
            ),
            patch(
                f"{NORMALIZATION_PIPELINE_OUTPUT_TABLE_COLLECTOR_PACKAGE_NAME}.build_normalization_pipeline_output_table_id_to_schemas",
                MagicMock(return_value=legacy_normalization_output_schema),
            ),
            patch(
                f"{NORMALIZATION_PIPELINE_OUTPUT_TABLE_COLLECTOR_PACKAGE_NAME}.get_direct_ingest_states_existing_in_env",
                MagicMock(return_value=states),
            ),
            patch(
                "recidiviz.source_tables.union_tables_output_table_collector.normalized_entities",
                fake_normalized_entities,
            ),
        ]
        for patcher in self.patchers:
            patcher.start()

    def tearDown(self) -> None:
        for patcher in self.patchers:
            patcher.stop()

    @patch(
        f"{UPDATE_NORMALIZED_STATE_PACKAGE_NAME}.is_combined_ingest_and_normalization_launched_in_env",
        MagicMock(return_value=True),
    )
    def test_get_normalized_state_view_builders_combined_pipelines_launched(
        self,
    ) -> None:
        expected_address_to_query = {
            BigQueryAddress(
                dataset_id="normalized_state_views",
                table_id="fake_another_entity_fake_entity_association_view",
            ): """SELECT state_code, fake_another_entity_id, fake_entity_id
FROM `recidiviz-456.us_xx_normalized_state_new.fake_another_entity_fake_entity_association`
UNION ALL
SELECT state_code, fake_another_entity_id, fake_entity_id
FROM `recidiviz-456.us_yy_normalized_state_new.fake_another_entity_fake_entity_association`
""",
            BigQueryAddress(
                dataset_id="normalized_state_views", table_id="fake_person_view"
            ): """SELECT state_code, fake_person_id, full_name
FROM `recidiviz-456.us_xx_normalized_state_new.fake_person`
UNION ALL
SELECT state_code, fake_person_id, full_name
FROM `recidiviz-456.us_yy_normalized_state_new.fake_person`
""",
            BigQueryAddress(
                dataset_id="normalized_state_views", table_id="fake_another_entity_view"
            ): """SELECT state_code, another_entity_id, another_name, extra_normalization_only_field, fake_person_id
FROM `recidiviz-456.us_xx_normalized_state_new.fake_another_entity`
UNION ALL
SELECT state_code, another_entity_id, another_name, extra_normalization_only_field, fake_person_id
FROM `recidiviz-456.us_yy_normalized_state_new.fake_another_entity`
""",
            BigQueryAddress(
                dataset_id="normalized_state_views", table_id="fake_entity_view"
            ): """SELECT state_code, entity_id, name, fake_person_id
FROM `recidiviz-456.us_xx_normalized_state_new.fake_entity`
UNION ALL
SELECT state_code, entity_id, name, fake_person_id
FROM `recidiviz-456.us_yy_normalized_state_new.fake_entity`
""",
        }

        builders = get_normalized_state_view_builders(state_codes_filter=None)

        self.assertEqual(
            expected_address_to_query,
            {b.address: b.build().view_query for b in builders},
        )

    @patch(
        f"{UPDATE_NORMALIZED_STATE_PACKAGE_NAME}.is_combined_ingest_and_normalization_launched_in_env",
    )
    def test_get_normalized_state_view_builders_combined_pipelines_partial_launched(
        self, gating_mock: MagicMock
    ) -> None:
        def mock_is_combined_ingest_and_normalization_launched_in_env(
            state_code: StateCode,
        ) -> bool:
            if state_code is StateCode.US_XX:
                return True
            if state_code is StateCode.US_YY:
                return False
            raise ValueError(f"Unexpected state code: {state_code}")

        gating_mock.side_effect = (
            mock_is_combined_ingest_and_normalization_launched_in_env
        )
        expected_address_to_query = {
            BigQueryAddress(
                dataset_id="normalized_state_views",
                table_id="fake_another_entity_fake_entity_association_view",
            ): """SELECT state_code, fake_another_entity_id, fake_entity_id
FROM `recidiviz-456.us_xx_normalized_state_new.fake_another_entity_fake_entity_association`
UNION ALL
SELECT state_code, fake_another_entity_id, fake_entity_id
FROM `recidiviz-456.us_yy_normalized_state.fake_another_entity_fake_entity_association`
""",
            BigQueryAddress(
                dataset_id="normalized_state_views", table_id="fake_person_view"
            ): """SELECT state_code, fake_person_id, full_name
FROM `recidiviz-456.us_xx_normalized_state_new.fake_person`
UNION ALL
SELECT state_code, fake_person_id, full_name
FROM `recidiviz-456.us_yy_state.fake_person`
""",
            BigQueryAddress(
                dataset_id="normalized_state_views", table_id="fake_another_entity_view"
            ): """SELECT state_code, another_entity_id, another_name, extra_normalization_only_field, fake_person_id
FROM `recidiviz-456.us_xx_normalized_state_new.fake_another_entity`
UNION ALL
SELECT state_code, another_entity_id, another_name, CAST(NULL AS STRING) AS extra_normalization_only_field, fake_person_id
FROM `recidiviz-456.us_yy_normalized_state.fake_another_entity`
""",
            BigQueryAddress(
                dataset_id="normalized_state_views", table_id="fake_entity_view"
            ): """SELECT state_code, entity_id, name, fake_person_id
FROM `recidiviz-456.us_xx_normalized_state_new.fake_entity`
UNION ALL
SELECT state_code, entity_id, name, fake_person_id
FROM `recidiviz-456.us_yy_normalized_state.fake_entity`
""",
        }

        builders = get_normalized_state_view_builders(state_codes_filter=None)

        self.assertEqual(
            expected_address_to_query,
            {b.address: b.build().view_query for b in builders},
        )

    @patch(
        f"{UPDATE_NORMALIZED_STATE_PACKAGE_NAME}.is_combined_ingest_and_normalization_launched_in_env",
        MagicMock(return_value=True),
    )
    def test_get_normalized_state_view_builders_state_code_filter(
        self,
    ) -> None:
        expected_address_to_query = {
            BigQueryAddress(
                dataset_id="normalized_state_views",
                table_id="fake_another_entity_fake_entity_association_view",
            ): """SELECT state_code, fake_another_entity_id, fake_entity_id
FROM `recidiviz-456.us_xx_normalized_state_new.fake_another_entity_fake_entity_association`
""",
            BigQueryAddress(
                dataset_id="normalized_state_views", table_id="fake_person_view"
            ): """SELECT state_code, fake_person_id, full_name
FROM `recidiviz-456.us_xx_normalized_state_new.fake_person`
""",
            BigQueryAddress(
                dataset_id="normalized_state_views", table_id="fake_another_entity_view"
            ): """SELECT state_code, another_entity_id, another_name, extra_normalization_only_field, fake_person_id
FROM `recidiviz-456.us_xx_normalized_state_new.fake_another_entity`
""",
            BigQueryAddress(
                dataset_id="normalized_state_views", table_id="fake_entity_view"
            ): """SELECT state_code, entity_id, name, fake_person_id
FROM `recidiviz-456.us_xx_normalized_state_new.fake_entity`
""",
        }

        builders = get_normalized_state_view_builders(
            state_codes_filter=[StateCode.US_XX]
        )

        self.assertEqual(
            expected_address_to_query,
            {b.address: b.build().view_query for b in builders},
        )
