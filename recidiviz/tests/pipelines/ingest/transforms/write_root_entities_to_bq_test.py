# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests the WriteRootEntitiesToBQ PTransform."""
import datetime
from unittest.mock import patch

import apache_beam as beam

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.state.state_person import StateGender
from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.activity import entities as state_entities
from recidiviz.persistence.entity.activity import normalized_entities
from recidiviz.persistence.entity.activity.entities import (
    StatePerson,
    StatePersonExternalId,
)
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
)
from recidiviz.persistence.entity.entity_utils import set_backedges
from recidiviz.persistence.entity.identity import identity_cluster_entities
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterExternalId,
)
from recidiviz.pipelines.ingest.identity.dataset_config import (
    identity_cluster_dataset_for_tenant,
)
from recidiviz.pipelines.ingest.transforms import write_root_entities_to_bq
from recidiviz.pipelines.ingest.transforms.write_root_entities_to_bq import (
    WriteRootEntitiesToBQ,
)
from recidiviz.pipelines.utils.execution_utils import TableRow
from recidiviz.source_tables.activity_pipeline_output_table_collector import (
    build_normalized_state_output_source_table_collection,
    build_state_output_source_table_collection,
)
from recidiviz.source_tables.identity_pipeline_output_table_collector import (
    build_identity_cluster_output_source_table_collection,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.persistence.entity.activity.entities_test_utils import (
    generate_full_graph_normalized_state_person,
    generate_full_graph_normalized_state_staff,
    generate_full_graph_state_person,
    generate_full_graph_state_staff,
)
from recidiviz.tests.persistence.entity.identity.entities_test_utils import (
    generate_full_graph_identity_cluster,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline
from recidiviz.tests.pipelines.fake_bigquery import FakeWriteToBigQueryEmulator
from recidiviz.utils import metadata
from recidiviz.utils.types import assert_type

_TENANT = Tenant.US_XX
_SANDBOX_PREFIX = "my_prefix"


class TestWriteRootEntitiesToBQ(BigQueryEmulatorTestCase):
    """Tests the WriteRootEntitiesToBQ PTransform."""

    wipe_emulator_data_on_teardown = False

    def setUp(self) -> None:
        super().setUp()
        self.write_to_bq_patcher = patch(
            f"{write_root_entities_to_bq.__name__}.WriteToBigQuery",
            FakeWriteToBigQueryEmulator.get_mock_write_to_big_query_constructor(self),
        )
        self.write_to_bq_patcher.start()

        self.test_pipeline = create_test_pipeline()

    def tearDown(self) -> None:
        super().tearDown()
        self._clear_emulator_table_data()
        self.write_to_bq_patcher.stop()

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        collections = [
            # Output collections
            build_state_output_source_table_collection(StateCode.US_DD),
            build_normalized_state_output_source_table_collection(StateCode.US_DD),
            build_identity_cluster_output_source_table_collection(_TENANT.value),
        ]

        return [c.as_sandbox_collection(_SANDBOX_PREFIX) for c in collections]

    def _get_rows_by_table(self, dataset_id: str) -> dict[str, list[TableRow]]:
        result = {}
        for t in self.bq_client.list_tables(dataset_id=dataset_id):
            address = ProjectSpecificBigQueryAddress(
                project_id=metadata.project_id(),
                dataset_id=dataset_id,
                table_id=t.table_id,
            )
            query_job = self.bq_client.run_query_async(
                query_str=address.select_query(), use_query_cache=False
            )
            result[t.table_id] = [dict(row) for row in query_job]
        return result

    def test_write_entities_to_bq_state(self) -> None:
        output_dataset_id = "my_prefix_us_dd_state"

        person = StatePerson(
            person_id=123,
            state_code=StateCode.US_DD.value,
            gender=StateGender.FEMALE,
            birthdate=datetime.date(2020, 1, 1),
            external_ids=[
                StatePersonExternalId(
                    person_external_id_id=456,
                    state_code=StateCode.US_DD.value,
                    external_id="ID_123",
                    id_type="US_DD_ID_TYPE",
                )
            ],
        )
        person = assert_type(
            set_backedges(person, entities_module_context_for_entity(person)),
            StatePerson,
        )

        _ = (
            self.test_pipeline
            | beam.Create([person])
            | WriteRootEntitiesToBQ(
                output_dataset=output_dataset_id,
                output_table_ids=["state_person", "state_person_external_id"],
                entities_module=state_entities,
            )
        )
        self.test_pipeline.run()

        persisted_rows_by_table = self._get_rows_by_table(output_dataset_id)

        for table, table_rows in persisted_rows_by_table.items():
            if table not in {"state_person", "state_person_external_id"}:
                self.assertEqual([], table_rows)
                continue

            if table == "state_person":
                self.assertEqual(
                    [
                        {
                            "birthdate": datetime.date(2020, 1, 1),
                            "current_address": None,
                            "current_email_address": None,
                            "current_phone_number": None,
                            "ethnicity": None,
                            "ethnicity_raw_text": None,
                            "full_name": None,
                            "gender": "FEMALE",
                            "gender_raw_text": None,
                            "person_id": 123,
                            "residency_status": None,
                            "residency_status_raw_text": None,
                            "sex": None,
                            "sex_raw_text": None,
                            "state_code": "US_DD",
                        }
                    ],
                    table_rows,
                )
            if table == "state_person_external_id":
                self.assertEqual(
                    [
                        {
                            "external_id": "ID_123",
                            "id_active_from_datetime": None,
                            "id_active_to_datetime": None,
                            "id_type": "US_DD_ID_TYPE",
                            "is_current_display_id_for_type": None,
                            "is_stable_id_for_type": None,
                            "person_external_id_id": 456,
                            "person_id": 123,
                            "state_code": "US_DD",
                        }
                    ],
                    table_rows,
                )

    def test_write_entities_to_bq_state_full_trees(self) -> None:
        output_dataset_id = "my_prefix_us_dd_state"
        output_table_ids = sorted(get_bq_schema_for_entities_module(state_entities))
        _ = (
            self.test_pipeline
            | beam.Create(
                [
                    generate_full_graph_state_person(
                        set_back_edges=True,
                        include_person_back_edges=True,
                        set_ids=True,
                    ),
                    generate_full_graph_state_staff(
                        set_back_edges=True,
                        set_ids=True,
                    ),
                ]
            )
            | WriteRootEntitiesToBQ(
                output_dataset=output_dataset_id,
                output_table_ids=output_table_ids,
                entities_module=state_entities,
            )
        )
        self.test_pipeline.run()

        persisted_rows_by_table = self._get_rows_by_table(output_dataset_id)

        for table, table_rows in persisted_rows_by_table.items():
            if not table_rows:
                self.fail(f"Found table [{table}] unexpectedly empty")

    def test_write_entities_to_bq_normalized_state_full_trees(self) -> None:
        output_dataset_id = "my_prefix_us_dd_normalized_state"
        output_table_ids = sorted(
            get_bq_schema_for_entities_module(normalized_entities)
        )
        _ = (
            self.test_pipeline
            | beam.Create(
                [
                    generate_full_graph_normalized_state_person(),
                    generate_full_graph_normalized_state_staff(),
                ]
            )
            | WriteRootEntitiesToBQ(
                output_dataset=output_dataset_id,
                output_table_ids=output_table_ids,
                entities_module=normalized_entities,
            )
        )
        self.test_pipeline.run()

        persisted_rows_by_table = self._get_rows_by_table(output_dataset_id)

        for table, table_rows in persisted_rows_by_table.items():
            if not table_rows:
                self.fail(f"Found table [{table}] unexpectedly empty")

    def test_write_identity_cluster_minimal(self) -> None:
        """A minimal identity cluster (no M2M relationships in
        `identity_cluster_entities`) round-trips through WriteRootEntitiesToBQ."""
        output_dataset_id = identity_cluster_dataset_for_tenant(
            _TENANT.value, sandbox_dataset_prefix=_SANDBOX_PREFIX
        )
        output_table_ids = sorted(
            get_bq_schema_for_entities_module(identity_cluster_entities)
        )

        cluster = IdentityCluster(
            tenant=_TENANT,
            person_type=PersonType.JII,
            external_ids=(
                IdentityClusterExternalId(
                    tenant=_TENANT,
                    external_id="EXT_001",
                    id_type=f"{_TENANT.value}_ID_TYPE",
                ),
            ),
        )

        _ = (
            self.test_pipeline
            | beam.Create([cluster])
            | WriteRootEntitiesToBQ(
                output_dataset=output_dataset_id,
                output_table_ids=output_table_ids,
                entities_module=identity_cluster_entities,
            )
        )
        self.test_pipeline.run()

        rows_by_table = self._get_rows_by_table(output_dataset_id)

        self.assertEqual(len(rows_by_table["identity_cluster"]), 1)
        self.assertEqual(rows_by_table["identity_cluster"][0]["tenant"], _TENANT.value)
        self.assertEqual(
            rows_by_table["identity_cluster_external_id"],
            [
                {
                    "tenant": _TENANT.value,
                    "external_id": "EXT_001",
                    "id_type": f"{_TENANT.value}_ID_TYPE",
                    "identity_cluster_id": cluster.identity_cluster_id,
                }
            ],
        )
        for empty_table in (
            "identity_cluster_email",
            "identity_cluster_ethnicity",
            "identity_cluster_gender",
            "identity_cluster_name",
            "identity_cluster_phone_number",
            "identity_cluster_race",
            "identity_cluster_sex",
        ):
            self.assertEqual(rows_by_table[empty_table], [])

    def test_write_identity_cluster_full_tree(self) -> None:
        output_dataset_id = identity_cluster_dataset_for_tenant(
            _TENANT.value, sandbox_dataset_prefix=_SANDBOX_PREFIX
        )
        output_table_ids = sorted(
            get_bq_schema_for_entities_module(identity_cluster_entities)
        )

        _ = (
            self.test_pipeline
            | beam.Create([generate_full_graph_identity_cluster()])
            | WriteRootEntitiesToBQ(
                output_dataset=output_dataset_id,
                output_table_ids=output_table_ids,
                entities_module=identity_cluster_entities,
            )
        )
        self.test_pipeline.run()

        rows_by_table = self._get_rows_by_table(output_dataset_id)
        for table_id in output_table_ids:
            self.assertGreaterEqual(
                len(rows_by_table[table_id]),
                1,
                msg=f"Expected at least one row in [{table_id}]",
            )
