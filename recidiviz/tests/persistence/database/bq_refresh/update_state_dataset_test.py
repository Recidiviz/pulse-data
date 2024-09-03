# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for update_state_dataset.py."""


import unittest

import pytest
from mock import MagicMock, call, patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, SimpleBigQueryViewBuilder
from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.bq_refresh import update_state_dataset
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_all_source_table_datasets,
)
from recidiviz.source_tables.ingest_pipeline_output_table_collector import (
    build_ingest_pipeline_output_source_table_collections,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.source_tables.union_tables_output_table_collector import (
    build_unioned_state_source_table_collection,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.persistence.entity import fake_entities

UPDATE_STATE_DATASET_PACKAGE_NAME = update_state_dataset.__name__
TEST_PROJECT = "test-project"


@pytest.mark.uses_bq_emulator
class UpdateStateDatasetIntegrationTest(BigQueryEmulatorTestCase):
    """Integration tests for update_state_dataset.py"""

    # We don't load any data in these tests, so don't bother wiping source tables
    wipe_emulator_data_on_teardown = False

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        return [
            # Input collections
            *build_ingest_pipeline_output_source_table_collections(),
            # Output collections
            build_unioned_state_source_table_collection(),
        ]

    def test_combine_ingest_sources_into_single_state_dataset(self) -> None:
        """Tests that combine_ingest_sources_into_single_state_dataset() does not crash
        when run against an emulator with real source tables loaded.
        """
        update_state_dataset.combine_ingest_sources_into_single_state_dataset()

    def test_combine_ingest_sources_into_single_state_dataset_output_prefix(
        self,
    ) -> None:
        """Tests that combine_ingest_sources_into_single_state_dataset() does not crash
        when run with an output prefix against an emulator with real source tables
        loaded.
        """
        update_state_dataset.combine_ingest_sources_into_single_state_dataset(
            output_sandbox_prefix="my_prefix",
        )

        if not list(self.bq_client.list_tables("my_prefix_state")):
            raise ValueError("Expected to find tables in dataset my_prefix_state")


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value=TEST_PROJECT))
class UpdateStateDatasetTest(unittest.TestCase):
    """Tests for update_state_dataset.py."""

    def setUp(self) -> None:
        # TODO(#25330): Remove this custom comparison once __eq__ works for BigQueryView
        self.addTypeEqualityFunc(
            BigQueryView,
            lambda x, y, msg=None: self.assertEqual(repr(x), repr(y), msg),
        )
        self.bq_patcher = patch(
            "recidiviz.big_query.view_update_manager.BigQueryClientImpl"
        )
        self.mock_bq = self.bq_patcher.start().return_value
        self.mock_bq.dataset_exists.return_value = True
        self.mock_bq.create_or_update_view.return_value.schema = []

        self.existing_states_patcher = patch(
            f"{UPDATE_STATE_DATASET_PACKAGE_NAME}.get_direct_ingest_states_existing_in_env",
            MagicMock(
                return_value=[
                    StateCode.US_DD,
                    StateCode.US_WW,
                    StateCode.US_XX,
                    StateCode.US_YY,
                ]
            ),
        )
        self.existing_states_patcher.start()

        self.entities_module_patcher = patch(
            "recidiviz.source_tables.union_tables_output_table_collector.state_entities",
            fake_entities,
        )
        self.entities_module_patcher.start()

    def tearDown(self) -> None:
        self.existing_states_patcher.stop()
        self.bq_patcher.stop()
        self.entities_module_patcher.stop()

    def test_output_dataset_is_source(self) -> None:
        self.assertTrue(STATE_BASE_DATASET in get_all_source_table_datasets())

    def test_combine_ingest_sources(self) -> None:
        # Act
        update_state_dataset.combine_ingest_sources_into_single_state_dataset()

        # Assert
        person_view = SimpleBigQueryViewBuilder(
            dataset_id="state_views",
            view_id="fake_person_view",
            description="",
            view_query_template="SELECT state_code, fake_person_id, full_name\n"
            "FROM `test-project.us_dd_state.fake_person`\n"
            "UNION ALL\n"
            "SELECT state_code, fake_person_id, full_name\n"
            "FROM `test-project.us_ww_state.fake_person`\n"
            "UNION ALL\n"
            "SELECT state_code, fake_person_id, full_name\n"
            "FROM `test-project.us_xx_state.fake_person`\n"
            "UNION ALL\n"
            "SELECT state_code, fake_person_id, full_name\n"
            "FROM `test-project.us_yy_state.fake_person`\n",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="state", table_id="fake_person"
            ),
        ).build()
        entity_view = SimpleBigQueryViewBuilder(
            dataset_id="state_views",
            view_id="fake_entity_view",
            description="",
            view_query_template="SELECT state_code, entity_id, name, fake_person_id\n"
            "FROM `test-project.us_dd_state.fake_entity`\n"
            "UNION ALL\n"
            "SELECT state_code, entity_id, name, fake_person_id\n"
            "FROM `test-project.us_ww_state.fake_entity`\n"
            "UNION ALL\n"
            "SELECT state_code, entity_id, name, fake_person_id\n"
            "FROM `test-project.us_xx_state.fake_entity`\n"
            "UNION ALL\n"
            "SELECT state_code, entity_id, name, fake_person_id\n"
            "FROM `test-project.us_yy_state.fake_entity`\n",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="state", table_id="fake_entity"
            ),
        ).build()
        association_table_view = SimpleBigQueryViewBuilder(
            dataset_id="state_views",
            view_id="fake_another_entity_fake_entity_association_view",
            description="",
            view_query_template="SELECT state_code, fake_another_entity_id, fake_entity_id\n"
            "FROM `test-project.us_dd_state.fake_another_entity_fake_entity_association`\n"
            "UNION ALL\n"
            "SELECT state_code, fake_another_entity_id, fake_entity_id\n"
            "FROM `test-project.us_ww_state.fake_another_entity_fake_entity_association`\n"
            "UNION ALL\n"
            "SELECT state_code, fake_another_entity_id, fake_entity_id\n"
            "FROM `test-project.us_xx_state.fake_another_entity_fake_entity_association`\n"
            "UNION ALL\n"
            "SELECT state_code, fake_another_entity_id, fake_entity_id\n"
            "FROM `test-project.us_yy_state.fake_another_entity_fake_entity_association`\n",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="state",
                table_id="fake_another_entity_fake_entity_association",
            ),
        ).build()

        self.assertEqual(
            self.mock_bq.create_dataset_if_necessary.mock_calls,
            [
                call("state_views", None),
                call("state", None),
            ],
        )

        # TODO(#25330): Remove this custom comparison once __eq__ works for BigQueryView
        self.assertEqual(len(self.mock_bq.create_or_update_view.call_args_list), 4)
        (
            association_table_create_call_args,
            _another_entity_create_call_args,
            entity_create_call_args,
            person_create_call_args,
        ) = sorted(
            self.mock_bq.create_or_update_view.call_args_list, key=lambda x: x[0][0]
        )
        self.assertEqual(person_create_call_args, call(person_view, might_exist=True))
        self.assertEqual(person_create_call_args[0][0], person_view)
        self.assertEqual(entity_create_call_args, call(entity_view, might_exist=True))
        self.assertEqual(entity_create_call_args[0][0], entity_view)
        self.assertEqual(
            association_table_create_call_args,
            call(association_table_view, might_exist=True),
        )
        self.assertEqual(
            association_table_create_call_args[0][0], association_table_view
        )

        # TODO(#25330): Remove this custom comparison once __eq__ works for BigQueryView
        self.assertEqual(len(self.mock_bq.materialize_view_to_table.call_args_list), 4)
        (
            association_table_materialize_call_args,
            _another_entity_materialize_call_args,
            entity_materialize_call_args,
            person_materialize_call_args,
        ) = sorted(
            self.mock_bq.materialize_view_to_table.call_args_list,
            key=lambda x: x[1]["view"],
        )
        self.assertEqual(
            person_materialize_call_args, call(view=person_view, use_query_cache=True)
        )
        self.assertEqual(person_materialize_call_args[1]["view"], person_view)
        self.assertEqual(
            entity_materialize_call_args, call(view=entity_view, use_query_cache=True)
        )
        self.assertEqual(entity_materialize_call_args[1]["view"], entity_view)
        self.assertEqual(
            association_table_materialize_call_args,
            call(view=association_table_view, use_query_cache=True),
        )
        self.assertEqual(
            association_table_materialize_call_args[1]["view"], association_table_view
        )

    def test_combine_ingest_sources_sandbox(self) -> None:
        # Act
        update_state_dataset.combine_ingest_sources_into_single_state_dataset(
            output_sandbox_prefix="foo"
        )

        # Assert
        person_view = SimpleBigQueryViewBuilder(
            dataset_id="foo_state_views",
            view_id="fake_person_view",
            description="",
            view_query_template="SELECT state_code, fake_person_id, full_name\n"
            "FROM `test-project.us_dd_state.fake_person`\n"
            "UNION ALL\n"
            "SELECT state_code, fake_person_id, full_name\n"
            "FROM `test-project.us_ww_state.fake_person`\n"
            "UNION ALL\n"
            "SELECT state_code, fake_person_id, full_name\n"
            "FROM `test-project.us_xx_state.fake_person`\n"
            "UNION ALL\n"
            "SELECT state_code, fake_person_id, full_name\n"
            "FROM `test-project.us_yy_state.fake_person`\n",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="foo_state", table_id="fake_person"
            ),
        ).build()
        entity_view = SimpleBigQueryViewBuilder(
            dataset_id="foo_state_views",
            view_id="fake_entity_view",
            description="",
            view_query_template="SELECT state_code, entity_id, name, fake_person_id\n"
            "FROM `test-project.us_dd_state.fake_entity`\n"
            "UNION ALL\n"
            "SELECT state_code, entity_id, name, fake_person_id\n"
            "FROM `test-project.us_ww_state.fake_entity`\n"
            "UNION ALL\n"
            "SELECT state_code, entity_id, name, fake_person_id\n"
            "FROM `test-project.us_xx_state.fake_entity`\n"
            "UNION ALL\n"
            "SELECT state_code, entity_id, name, fake_person_id\n"
            "FROM `test-project.us_yy_state.fake_entity`\n",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="foo_state", table_id="fake_entity"
            ),
        ).build()
        association_table_view = SimpleBigQueryViewBuilder(
            dataset_id="foo_state_views",
            view_id="fake_another_entity_fake_entity_association_view",
            description="",
            view_query_template="SELECT state_code, fake_another_entity_id, fake_entity_id\n"
            "FROM `test-project.us_dd_state.fake_another_entity_fake_entity_association`\n"
            "UNION ALL\n"
            "SELECT state_code, fake_another_entity_id, fake_entity_id\n"
            "FROM `test-project.us_ww_state.fake_another_entity_fake_entity_association`\n"
            "UNION ALL\n"
            "SELECT state_code, fake_another_entity_id, fake_entity_id\n"
            "FROM `test-project.us_xx_state.fake_another_entity_fake_entity_association`\n"
            "UNION ALL\n"
            "SELECT state_code, fake_another_entity_id, fake_entity_id\n"
            "FROM `test-project.us_yy_state.fake_another_entity_fake_entity_association`\n",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="state",
                table_id="fake_another_entity_fake_entity_association",
            ),
        ).build()

        self.assertEqual(
            self.mock_bq.create_dataset_if_necessary.mock_calls,
            [
                call(
                    "foo_state_views",
                    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
                ),
                call(
                    "foo_state",
                    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
                ),
            ],
        )

        # TODO(#25330): Remove this custom comparison once __eq__ works for BigQueryView
        self.assertEqual(len(self.mock_bq.create_or_update_view.call_args_list), 4)
        (
            association_table_create_call_args,
            _another_entity_create_call_args,
            entity_create_call_args,
            person_create_call_args,
        ) = sorted(
            self.mock_bq.create_or_update_view.call_args_list, key=lambda x: x[0][0]
        )
        self.assertEqual(person_create_call_args, call(person_view, might_exist=True))
        self.assertEqual(person_create_call_args[0][0], person_view)
        self.assertEqual(entity_create_call_args, call(entity_view, might_exist=True))
        self.assertEqual(entity_create_call_args[0][0], entity_view)
        self.assertEqual(
            association_table_create_call_args,
            call(association_table_view, might_exist=True),
        )
        self.assertEqual(
            association_table_create_call_args[0][0], association_table_view
        )

        # TODO(#25330): Remove this custom comparison once __eq__ works for BigQueryView
        self.assertEqual(len(self.mock_bq.materialize_view_to_table.call_args_list), 4)
        (
            association_table_materialize_call_args,
            _another_entity_materialize_call_args,
            entity_materialize_call_args,
            person_materialize_call_args,
        ) = sorted(
            self.mock_bq.materialize_view_to_table.call_args_list,
            key=lambda x: x[1]["view"],
        )
        self.assertEqual(
            person_materialize_call_args, call(view=person_view, use_query_cache=True)
        )
        self.assertEqual(person_materialize_call_args[1]["view"], person_view)
        self.assertEqual(
            entity_materialize_call_args, call(view=entity_view, use_query_cache=True)
        )
        self.assertEqual(entity_materialize_call_args[1]["view"], entity_view)
        self.assertEqual(
            association_table_materialize_call_args,
            call(view=association_table_view, use_query_cache=True),
        )
        self.assertEqual(
            association_table_materialize_call_args[1]["view"], association_table_view
        )
