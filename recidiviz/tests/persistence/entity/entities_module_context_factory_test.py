# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for entities_module_context_factory.py"""
import unittest
from functools import cmp_to_key

import attr

from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.activity import entities as state_entities
from recidiviz.persistence.entity.activity import normalized_entities
from recidiviz.persistence.entity.activity.entities import StatePerson
from recidiviz.persistence.entity.activity.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStateSentence,
)
from recidiviz.persistence.entity.activity.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    CONTEXT_CLASS_BY_MODULE,
    entities_module_context_for_entity_class,
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.identity import identity_cluster_entities
from recidiviz.persistence.entity.operations import entities as operations_entities
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestDataflowRawTableUpperBounds,
)
from recidiviz.tests.persistence.database.schema_entity_converter.fake_entities import (
    Root,
    RootType,
)
from recidiviz.tests.persistence.database.schema_entity_converter.fake_entities_module_context import (
    FakeEntitiesModuleContext,
)
from recidiviz.utils.types import assert_subclass


class EntitiesModuleContextFactoryTest(unittest.TestCase):
    """Tests for entities_module_context_factory.py"""

    def test_normalized_and_state_have_same_class_hierarchy(self) -> None:
        """If this test fails the class_hierarchy() definitions in
        _StateEntitiesModuleContext and _NormalizedStateEntitiesModuleContext are not
        sorted in the same way for equivalent classes.
        """
        equivalent_state_classes_list = []
        normalized_state_direction_checker = entities_module_context_for_module(
            normalized_entities
        ).direction_checker()

        def sort_fn(a: type[Entity], b: type[Entity]) -> int:
            return (
                -1
                if not normalized_state_direction_checker.is_higher_ranked(a, b)
                else 1
            )

        sorted_normalized_by_rank = sorted(
            get_all_entity_classes_in_module(normalized_state_direction_checker),
            key=cmp_to_key(sort_fn),
        )
        for normalized_entity_cls in sorted_normalized_by_rank:
            equivalent_state_class = get_entity_class_in_module_with_name(
                state_entities,
                assert_subclass(
                    normalized_entity_cls, NormalizedStateEntity
                ).base_class_name(),
            )
            if not equivalent_state_class:
                continue
            equivalent_state_classes_list.append(equivalent_state_class)
        state_direction_checker = entities_module_context_for_module(
            state_entities
        ).direction_checker()
        state_direction_checker.assert_sorted(equivalent_state_classes_list)

    def test_entities_module_context_for_entity_class(self) -> None:
        self.assertEqual(
            normalized_entities,
            entities_module_context_for_entity_class(
                NormalizedStateSentence
            ).entities_module(),
        )
        self.assertEqual(
            normalized_entities,
            entities_module_context_for_entity_class(
                NormalizedStateAssessment
            ).entities_module(),
        )
        self.assertEqual(
            state_entities,
            entities_module_context_for_entity_class(StatePerson).entities_module(),
        )

        self.assertEqual(
            operations_entities,
            entities_module_context_for_entity_class(
                DirectIngestDataflowRawTableUpperBounds
            ).entities_module(),
        )

    def test_partition_column_field_exists_on_every_entity(self) -> None:
        """For every module that declares a partition column, that column name
        must be a real attrs field on every entity class in the module.
        Modules that return None from `partition_column_name` are skipped.
        """
        for module in CONTEXT_CLASS_BY_MODULE:
            context = entities_module_context_for_module(module)
            partition_column = context.partition_column_name()
            if partition_column is None:
                continue
            for entity_cls in get_all_entity_classes_in_module(module):
                with self.subTest(module=module.__name__, entity=entity_cls.__name__):
                    self.assertIn(
                        partition_column,
                        {a.name for a in attr.fields(entity_cls)},
                        msg=(
                            f"Entity [{entity_cls.__name__}] in module "
                            f"[{module.__name__}] is missing the partition column "
                            f"field [{partition_column}] declared by its context."
                        ),
                    )

    def test_get_partition_value_raises_when_no_partition_column(self) -> None:
        """`get_partition_value` raises when the context declares no partition
        column (e.g. test fakes whose entities have no partition field)."""
        context = FakeEntitiesModuleContext()
        self.assertIsNone(context.partition_column_name())
        with self.assertRaisesRegex(ValueError, "Expected non-null value"):
            context.get_partition_value(Root(root_id=1, type=RootType.SIMPSONS))

    def test_get_partition_value_reads_field(self) -> None:
        """`get_partition_value` returns the value of the field named by
        `partition_column_name`."""
        state_context = entities_module_context_for_module(state_entities)
        self.assertEqual(
            "US_XX",
            state_context.get_partition_value(StatePerson(state_code="US_XX")),
        )

        identity_context = entities_module_context_for_module(identity_cluster_entities)
        self.assertEqual(
            "US_XX",
            identity_context.get_partition_value(
                identity_cluster_entities.IdentityCluster(
                    tenant=Tenant.US_XX,
                    person_type=identity_cluster_entities.PersonType.JII,
                    external_ids=(
                        identity_cluster_entities.IdentityClusterExternalId(
                            tenant=Tenant.US_XX,
                            external_id="EXT_001",
                            id_type="US_XX_ID_TYPE",
                        ),
                    ),
                )
            ),
        )
