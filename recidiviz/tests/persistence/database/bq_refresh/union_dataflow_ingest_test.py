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
"""Tests for union_dataflow_ingest.py."""


import unittest

from google.cloud import bigquery
from mock import MagicMock, call, patch
from sqlalchemy import Column, ForeignKey, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import DeclarativeMeta, relationship

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, SimpleBigQueryViewBuilder
from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh import union_dataflow_ingest
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS

UNION_DATAFLOW_INGEST_PACKAGE_NAME = union_dataflow_ingest.__name__
TEST_PROJECT = "test-project"

FakeBase: DeclarativeMeta = declarative_base()


class FakePerson(FakeBase):
    """Represents a Child object in the test schema"""

    __tablename__ = "fake_person"

    state_code = Column(String(255))

    person_id = Column(Integer, primary_key=True)
    full_name = Column(String(255))

    entity_id = Column(Integer, ForeignKey("entity.entity_id"))
    entity = relationship("Entity", uselist=False)


class FakeEntity(FakeBase):
    __tablename__ = "fake_entity"

    state_code = Column(String(255))

    entity_id = Column(Integer, primary_key=True)
    name = Column(String(255))


class FakeAnotherEntity(FakeBase):
    __tablename__ = "fake_another_entity"

    state_code = Column(String(255))

    another_entity_id = Column(Integer, primary_key=True)
    another_name = Column(String(255))


association_table = Table(
    "state_entity_association",
    FakeBase.metadata,
    Column("entity_id", Integer, ForeignKey("fake_entity.entity_id")),
    Column(
        "another_entity_id",
        Integer,
        ForeignKey("fake_another_entity.another_entity_id"),
    ),
)

_ALL_SCHEMA_TABLES = [
    FakePerson.__table__,
    FakeEntity.__table__,
    FakeAnotherEntity.__table__,
    association_table,
]


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value=TEST_PROJECT))
class UnionDataflowIngestTest(unittest.TestCase):
    """Tests for union_dataflow_ingest.py."""

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

        # TODO(#25330): Remove this custom comparison once __eq__ works for BigQueryView
        def mock_dataset_ref_for_id(dataset_id: str) -> bigquery.DatasetReference:
            return bigquery.DatasetReference(TEST_PROJECT, dataset_id)

        self.mock_bq.dataset_ref_for_id = mock_dataset_ref_for_id
        self.mock_bq.dataset_exists.return_value = True
        self.mock_bq.create_or_update_view.return_value.schema = []

        self.existing_states_patcher = patch(
            f"{UNION_DATAFLOW_INGEST_PACKAGE_NAME}.get_direct_ingest_states_existing_in_env",
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

    def tearDown(self) -> None:
        self.existing_states_patcher.stop()
        self.bq_patcher.stop()

    def test_output_dataset_is_source(self) -> None:
        self.assertTrue(STATE_BASE_DATASET in VIEW_SOURCE_TABLE_DATASETS)

    def test_combine_ingest_sources(self) -> None:
        # Act
        union_dataflow_ingest.combine_ingest_sources_into_single_state_dataset(
            ingest_instance=DirectIngestInstance.PRIMARY,
            tables=_ALL_SCHEMA_TABLES,
        )

        # Assert
        person_view = SimpleBigQueryViewBuilder(
            dataset_id="state_views",
            view_id="fake_person_view",
            description="",
            view_query_template="SELECT state_code, person_id, full_name, entity_id\n"
            "FROM `test-project.us_dd_state_primary.fake_person`\n"
            "UNION ALL\n"
            "SELECT state_code, person_id, full_name, entity_id\n"
            "FROM `test-project.us_ww_state_primary.fake_person`\n"
            "UNION ALL\n"
            "SELECT state_code, person_id, full_name, entity_id\n"
            "FROM `test-project.us_xx_state_primary.fake_person`\n"
            "UNION ALL\n"
            "SELECT state_code, person_id, full_name, entity_id\n"
            "FROM `test-project.us_yy_state_primary.fake_person`\n",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="state", table_id="fake_person"
            ),
        ).build()
        entity_view = SimpleBigQueryViewBuilder(
            dataset_id="state_views",
            view_id="fake_entity_view",
            description="",
            view_query_template="SELECT state_code, entity_id, name\n"
            "FROM `test-project.us_dd_state_primary.fake_entity`\n"
            "UNION ALL\n"
            "SELECT state_code, entity_id, name\n"
            "FROM `test-project.us_ww_state_primary.fake_entity`\n"
            "UNION ALL\n"
            "SELECT state_code, entity_id, name\n"
            "FROM `test-project.us_xx_state_primary.fake_entity`\n"
            "UNION ALL\n"
            "SELECT state_code, entity_id, name\n"
            "FROM `test-project.us_yy_state_primary.fake_entity`\n",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="state", table_id="fake_entity"
            ),
        ).build()
        association_table_view = SimpleBigQueryViewBuilder(
            dataset_id="state_views",
            view_id="state_entity_association_view",
            description="",
            view_query_template="SELECT entity_id, another_entity_id, state_code\n"
            "FROM `test-project.us_dd_state_primary.state_entity_association`\n"
            "UNION ALL\n"
            "SELECT entity_id, another_entity_id, state_code\n"
            "FROM `test-project.us_ww_state_primary.state_entity_association`\n"
            "UNION ALL\n"
            "SELECT entity_id, another_entity_id, state_code\n"
            "FROM `test-project.us_xx_state_primary.state_entity_association`\n"
            "UNION ALL\n"
            "SELECT entity_id, another_entity_id, state_code\n"
            "FROM `test-project.us_yy_state_primary.state_entity_association`\n",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="state", table_id="state_entity_association"
            ),
        ).build()

        self.assertEqual(
            self.mock_bq.create_dataset_if_necessary.mock_calls,
            [
                call(bigquery.DatasetReference(TEST_PROJECT, "state_views"), None),
                call(bigquery.DatasetReference(TEST_PROJECT, "state"), None),
            ],
        )

        # TODO(#25330): Remove this custom comparison once __eq__ works for BigQueryView
        self.assertEqual(len(self.mock_bq.create_or_update_view.call_args_list), 4)
        (
            _another_entity_create_call_args,
            entity_create_call_args,
            person_create_call_args,
            association_table_create_call_args,
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
            _another_entity_materialize_call_args,
            entity_materialize_call_args,
            person_materialize_call_args,
            association_table_materialize_call_args,
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

    def test_combine_ingest_sources_secondary_no_sandbox(self) -> None:
        # Act
        with self.assertRaisesRegex(
            ValueError,
            "Refresh can only proceed for secondary databases into a sandbox.",
        ):
            union_dataflow_ingest.combine_ingest_sources_into_single_state_dataset(
                ingest_instance=DirectIngestInstance.SECONDARY,
                tables=_ALL_SCHEMA_TABLES,
            )

    def test_combine_ingest_sources_secondary_sandbox(self) -> None:
        # Act
        union_dataflow_ingest.combine_ingest_sources_into_single_state_dataset(
            ingest_instance=DirectIngestInstance.SECONDARY,
            tables=_ALL_SCHEMA_TABLES,
            output_sandbox_prefix="foo",
        )

        # Assert
        person_view = SimpleBigQueryViewBuilder(
            dataset_id="foo_state_views",
            view_id="fake_person_view",
            description="",
            view_query_template="SELECT state_code, person_id, full_name, entity_id\n"
            "FROM `test-project.us_dd_state_secondary.fake_person`\n"
            "UNION ALL\n"
            "SELECT state_code, person_id, full_name, entity_id\n"
            "FROM `test-project.us_ww_state_secondary.fake_person`\n"
            "UNION ALL\n"
            "SELECT state_code, person_id, full_name, entity_id\n"
            "FROM `test-project.us_xx_state_secondary.fake_person`\n"
            "UNION ALL\n"
            "SELECT state_code, person_id, full_name, entity_id\n"
            "FROM `test-project.us_yy_state_secondary.fake_person`\n",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="foo_state", table_id="fake_person"
            ),
        ).build()
        entity_view = SimpleBigQueryViewBuilder(
            dataset_id="foo_state_views",
            view_id="fake_entity_view",
            description="",
            view_query_template="SELECT state_code, entity_id, name\n"
            "FROM `test-project.us_dd_state_secondary.fake_entity`\n"
            "UNION ALL\n"
            "SELECT state_code, entity_id, name\n"
            "FROM `test-project.us_ww_state_secondary.fake_entity`\n"
            "UNION ALL\n"
            "SELECT state_code, entity_id, name\n"
            "FROM `test-project.us_xx_state_secondary.fake_entity`\n"
            "UNION ALL\n"
            "SELECT state_code, entity_id, name\n"
            "FROM `test-project.us_yy_state_secondary.fake_entity`\n",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="foo_state", table_id="fake_entity"
            ),
        ).build()
        association_table_view = SimpleBigQueryViewBuilder(
            dataset_id="foo_state_views",
            view_id="state_entity_association_view",
            description="",
            view_query_template="SELECT entity_id, another_entity_id, state_code\n"
            "FROM `test-project.us_dd_state_secondary.state_entity_association`\n"
            "UNION ALL\n"
            "SELECT entity_id, another_entity_id, state_code\n"
            "FROM `test-project.us_ww_state_secondary.state_entity_association`\n"
            "UNION ALL\n"
            "SELECT entity_id, another_entity_id, state_code\n"
            "FROM `test-project.us_xx_state_secondary.state_entity_association`\n"
            "UNION ALL\n"
            "SELECT entity_id, another_entity_id, state_code\n"
            "FROM `test-project.us_yy_state_secondary.state_entity_association`\n",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id="state", table_id="state_entity_association"
            ),
        ).build()

        self.assertEqual(
            self.mock_bq.create_dataset_if_necessary.mock_calls,
            [
                call(
                    bigquery.DatasetReference(TEST_PROJECT, "foo_state_views"),
                    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
                ),
                call(
                    bigquery.DatasetReference(TEST_PROJECT, "foo_state"),
                    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
                ),
            ],
        )

        # TODO(#25330): Remove this custom comparison once __eq__ works for BigQueryView
        self.assertEqual(len(self.mock_bq.create_or_update_view.call_args_list), 4)
        (
            _another_entity_create_call_args,
            entity_create_call_args,
            person_create_call_args,
            association_table_create_call_args,
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
            _another_entity_materialize_call_args,
            entity_materialize_call_args,
            person_materialize_call_args,
            association_table_materialize_call_args,
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
