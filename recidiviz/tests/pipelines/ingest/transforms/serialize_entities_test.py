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
"""Tests the SerializeEntities DoFn."""
import unittest

import apache_beam as beam
from apache_beam.pipeline_test import assert_that
from apache_beam.testing.util import is_not_empty

from recidiviz.persistence.entity.activity import entities as state_entities
from recidiviz.persistence.entity.activity import normalized_entities
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.identity import identity_cluster_entities
from recidiviz.pipelines.ingest.transforms.serialize_entities import SerializeEntities
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


class TestSerializeEntities(BigQueryEmulatorTestCase):
    """Tests the SerializeEntities DoFn."""

    def setUp(self) -> None:
        super().setUp()
        self.test_pipeline = create_test_pipeline()

    def test_serialize_state_entities(self) -> None:
        root_entities = [
            generate_full_graph_state_person(
                set_back_edges=True, include_person_back_edges=True, set_ids=True
            ),
            generate_full_graph_state_staff(set_back_edges=True, set_ids=True),
        ]
        table_ids = sorted(get_bq_schema_for_entities_module(state_entities))

        output = (
            self.test_pipeline
            | beam.Create(root_entities)
            | beam.ParDo(
                SerializeEntities(
                    entities_module_context=entities_module_context_for_module(
                        state_entities
                    ),
                )
            ).with_outputs(*table_ids)
        )

        # Checks that we produced output for every single table in the schema
        for state_table in table_ids:
            assert_that(
                getattr(output, state_table),
                is_not_empty(),
                label=f"{state_table} is not empty",
            )

        self.test_pipeline.run()

    def test_serialize_normalized_state_entities(self) -> None:
        root_entities = [
            generate_full_graph_normalized_state_person(),
            generate_full_graph_normalized_state_staff(),
        ]
        table_ids = sorted(get_bq_schema_for_entities_module(normalized_entities))

        output = (
            self.test_pipeline
            | beam.Create(root_entities)
            | beam.ParDo(
                SerializeEntities(
                    entities_module_context=entities_module_context_for_module(
                        normalized_entities
                    ),
                )
            ).with_outputs(*table_ids)
        )

        # Checks that we produced output for every single table in the schema
        for state_table in table_ids:
            assert_that(
                getattr(output, state_table),
                is_not_empty(),
                label=f"{state_table} is not empty",
            )

        self.test_pipeline.run()

    def test_serialize_identity_cluster_entities(self) -> None:
        """`identity_cluster_entities` has no M2M relationships, so SerializeEntities
        emits a row per table without any association rows."""
        table_ids = sorted(get_bq_schema_for_entities_module(identity_cluster_entities))

        output = (
            self.test_pipeline
            | beam.Create([generate_full_graph_identity_cluster()])
            | beam.ParDo(
                SerializeEntities(
                    entities_module_context=entities_module_context_for_module(
                        identity_cluster_entities
                    ),
                )
            ).with_outputs(*table_ids)
        )

        for table_id in table_ids:
            assert_that(
                getattr(output, table_id),
                is_not_empty(),
                label=f"{table_id} is not empty",
            )

        self.test_pipeline.run()


class TestSerializeEntitiesM2MWithoutPartitionColumn(unittest.TestCase):
    """Tests that SerializeEntities raises a descriptive error when its module
    has many-to-many relationships but declares no partition column."""

    def test_m2m_without_partition_column_raises(self) -> None:
        real_state_context = entities_module_context_for_module(state_entities)

        class NoPartitionStateContext(type(real_state_context)):  # type: ignore[misc, valid-type]
            @classmethod
            def partition_column_name(cls) -> str | None:
                return None

        dofn = SerializeEntities(entities_module_context=NoPartitionStateContext())
        person = generate_full_graph_state_person(
            set_back_edges=True, include_person_back_edges=True, set_ids=True
        )

        with self.assertRaisesRegex(
            ValueError,
            r"has many-to-many relationships but declares no partition column",
        ):
            # Consume the generator until the M2M branch fires and raises.
            for _ in dofn.process(person):
                pass
