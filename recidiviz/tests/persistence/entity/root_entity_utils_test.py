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
"""Tests for root_entity_utils.py"""
import unittest
from typing import Type

from recidiviz.persistence.entity.activity import entities, normalized_entities
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.identity import (
    identity_cluster_entities,
    identity_fragment_entities,
)
from recidiviz.persistence.entity.root_entity_utils import (
    entity_class_for_foreign_key_column,
    get_entity_class_to_root_entity_class,
    get_root_entity_class_for_entity,
    get_root_entity_id,
)


class RootEntityUtilsTest(unittest.TestCase):
    """Tests for root_entity_utils.py"""

    def test_get_root_entity_class(self) -> None:
        self.assertEqual(
            entities.StatePerson, get_root_entity_class_for_entity(entities.StatePerson)
        )
        self.assertEqual(
            entities.StatePerson,
            get_root_entity_class_for_entity(entities.StateAssessment),
        )
        self.assertEqual(
            entities.StateStaff,
            get_root_entity_class_for_entity(entities.StateStaffRolePeriod),
        )
        self.assertEqual(
            entities.StatePerson,
            get_root_entity_class_for_entity(
                entities.StatePersonStaffRelationshipPeriod
            ),
        )

    def test_get_root_entity_class_normalized(self) -> None:
        self.assertEqual(
            normalized_entities.NormalizedStatePerson,
            get_root_entity_class_for_entity(normalized_entities.NormalizedStatePerson),
        )
        self.assertEqual(
            normalized_entities.NormalizedStatePerson,
            get_root_entity_class_for_entity(
                normalized_entities.NormalizedStateAssessment
            ),
        )
        self.assertEqual(
            normalized_entities.NormalizedStatePerson,
            get_root_entity_class_for_entity(
                normalized_entities.NormalizedStateAssessment
            ),
        )
        self.assertEqual(
            normalized_entities.NormalizedStateStaff,
            get_root_entity_class_for_entity(
                normalized_entities.NormalizedStateStaffRolePeriod
            ),
        )

    def test_get_root_entity_class_identity_fragment(self) -> None:
        # The fragment root and entities that back-edge directly to it resolve
        # via the direct-field check.
        self.assertEqual(
            identity_fragment_entities.IdentityFragment,
            get_root_entity_class_for_entity(
                identity_fragment_entities.IdentityFragment
            ),
        )
        self.assertEqual(
            identity_fragment_entities.IdentityFragment,
            get_root_entity_class_for_entity(
                identity_fragment_entities.IdentityExternalId
            ),
        )
        self.assertEqual(
            identity_fragment_entities.IdentityFragment,
            get_root_entity_class_for_entity(
                identity_fragment_entities.IdentityAttributes
            ),
        )
        # Demographic entities back-edge to the intermediate IdentityAttributes,
        # not to the root directly, so they resolve via the back-edge walk.
        self.assertEqual(
            identity_fragment_entities.IdentityFragment,
            get_root_entity_class_for_entity(identity_fragment_entities.IdentityName),
        )
        self.assertEqual(
            identity_fragment_entities.IdentityFragment,
            get_root_entity_class_for_entity(identity_fragment_entities.IdentityRace),
        )

    def test_get_entity_class_to_root_entity_class_identity_fragment(self) -> None:
        root_entity_mapping = get_entity_class_to_root_entity_class(
            entities_module=identity_fragment_entities
        )

        # Every fragment entity (root, direct back-edge, and through-intermediate
        # alike) maps to the IdentityFragment root.
        for entity_cls in (
            identity_fragment_entities.IdentityFragment,
            identity_fragment_entities.IdentityExternalId,
            identity_fragment_entities.IdentityAttributes,
            identity_fragment_entities.IdentityName,
            identity_fragment_entities.IdentityRace,
            identity_fragment_entities.IdentityEmail,
        ):
            self.assertEqual(
                identity_fragment_entities.IdentityFragment,
                root_entity_mapping[entity_cls],
            )

    def test_get_root_entity_id_person(self) -> None:
        person = entities.StatePerson.new_with_defaults(
            person_id=123, state_code="US_XX"
        )
        external_id = entities.StatePersonExternalId.new_with_defaults(
            person_external_id_id=345,
            external_id="11111",
            id_type="US_XX_TYPE",
            state_code="US_XX",
            person=person,
        )
        person.external_ids.append(external_id)

        self.assertEqual(123, get_root_entity_id(external_id))
        self.assertEqual(123, get_root_entity_id(person))

    def test_get_root_entity_id_staff(self) -> None:
        staff = entities.StateStaff.new_with_defaults(staff_id=789, state_code="US_XX")
        staff_external_id = entities.StateStaffExternalId.new_with_defaults(
            staff_external_id_id=910,
            external_id="11111",
            id_type="US_ND_TYPE",
            state_code="US_ND",
            staff=staff,
        )
        staff.external_ids.append(staff_external_id)

        self.assertEqual(789, get_root_entity_id(staff_external_id))
        self.assertEqual(789, get_root_entity_id(staff))

    def test_get_entity_class_to_root_entity_class(self) -> None:
        root_entity_mapping = get_entity_class_to_root_entity_class(
            entities_module=entities
        )

        self.assertEqual(
            entities.StatePerson, root_entity_mapping[entities.StateAssessment]
        )
        self.assertEqual(
            entities.StatePerson, root_entity_mapping[entities.StatePerson]
        )

        self.assertEqual(
            entities.StateStaff, root_entity_mapping[entities.StateStaffRolePeriod]
        )
        self.assertEqual(entities.StateStaff, root_entity_mapping[entities.StateStaff])

    def test_get_entity_class_to_root_entity_class_normalized(self) -> None:
        root_entity_mapping = get_entity_class_to_root_entity_class(
            entities_module=normalized_entities
        )

        self.assertEqual(
            normalized_entities.NormalizedStatePerson,
            root_entity_mapping[normalized_entities.NormalizedStateAssessment],
        )
        self.assertEqual(
            normalized_entities.NormalizedStatePerson,
            root_entity_mapping[normalized_entities.NormalizedStatePerson],
        )

        self.assertEqual(
            normalized_entities.NormalizedStateStaff,
            root_entity_mapping[normalized_entities.NormalizedStateStaffRolePeriod],
        )
        self.assertEqual(
            normalized_entities.NormalizedStateStaff,
            root_entity_mapping[normalized_entities.NormalizedStateStaff],
        )


class EntityClassForForeignKeyColumnTest(unittest.TestCase):
    """Tests for entity_class_for_foreign_key_column, the shared source of
    truth for which relationship fields become id columns and whose ids they
    hold.
    """

    def setUp(self) -> None:
        self.state_context = entities_module_context_for_module(entities)
        self.fragment_context = entities_module_context_for_module(
            identity_fragment_entities
        )
        self.cluster_context = entities_module_context_for_module(
            identity_cluster_entities
        )

    def _fk(
        self,
        context: EntitiesModuleContext,
        entity_cls: Type[Entity],
        field_name: str,
    ) -> Type[Entity] | None:
        return entity_class_for_foreign_key_column(
            entities_module_context=context,
            entity_cls=entity_cls,
            field_name=field_name,
        )

    def test_flat_field_has_no_fk(self) -> None:
        self.assertIsNone(
            self._fk(
                self.fragment_context,
                identity_fragment_entities.IdentityName,
                "given_name",
            )
        )

    def test_many_to_one_to_root_with_own_pk(self) -> None:
        # StateAssessment is many-to-one with StatePerson, which has its own PK.
        self.assertEqual(
            entities.StatePerson,
            self._fk(self.state_context, entities.StateAssessment, "person"),
        )

    def test_many_to_one_to_non_root_with_own_pk(self) -> None:
        # StateSupervisionViolationResponse is many-to-one with
        # StateSupervisionViolation, which has its own PK but is not the root,
        # so the FK points at the violation itself, not at StatePerson.
        self.assertEqual(
            entities.StateSupervisionViolation,
            self._fk(
                self.state_context,
                entities.StateSupervisionViolationResponse,
                "supervision_violation",
            ),
        )

    def test_many_to_one_to_intermediate_without_pk_points_to_root(self) -> None:
        # IdentityRace is many-to-one with the intermediate IdentityAttributes,
        # which has no PK, so the FK points at the IdentityFragment root instead.
        self.assertEqual(
            identity_fragment_entities.IdentityFragment,
            self._fk(
                self.fragment_context,
                identity_fragment_entities.IdentityRace,
                "identity_attributes",
            ),
        )

    def test_one_to_one_back_edge_to_root_with_own_pk(self) -> None:
        # IdentityAttributes has a 1:1 back edge to the IdentityFragment root.
        self.assertEqual(
            identity_fragment_entities.IdentityFragment,
            self._fk(
                self.fragment_context,
                identity_fragment_entities.IdentityAttributes,
                "fragment",
            ),
        )
        # A string-PK root (identity cluster) resolves the same way.
        self.assertEqual(
            identity_cluster_entities.IdentityCluster,
            self._fk(
                self.cluster_context,
                identity_cluster_entities.IdentityClusterName,
                "identity_cluster",
            ),
        )

    def test_one_to_one_back_edge_to_intermediate_without_pk_points_to_root(
        self,
    ) -> None:
        self.assertEqual(
            identity_fragment_entities.IdentityFragment,
            self._fk(
                self.fragment_context,
                identity_fragment_entities.IdentityName,
                "identity_attributes",
            ),
        )

    def test_forward_edge_has_no_fk(self) -> None:
        # The forward 1:1 edge from IdentityAttributes to IdentityName carries no
        # FK; that lives on the child (identity_name) table's back edge.
        self.assertIsNone(
            self._fk(
                self.fragment_context,
                identity_fragment_entities.IdentityAttributes,
                "name",
            )
        )

    def test_one_to_many_has_no_fk(self) -> None:
        self.assertIsNone(
            self._fk(
                self.fragment_context,
                identity_fragment_entities.IdentityFragment,
                "external_ids",
            )
        )

    def test_many_to_many_has_no_fk(self) -> None:
        # Many-to-many relationships are encoded in association tables, not FK
        # columns.
        self.assertIsNone(
            self._fk(self.state_context, entities.StateChargeV2, "sentences")
        )

    def test_indirect_reference_to_root(self) -> None:
        # StateEarlyDischarge.person is a one-way reference: its parents in the
        # entity tree are the sentences, and StatePerson has no early_discharges
        # field back. Its table still carries person_id to join back to the root.
        self.assertEqual(
            entities.StatePerson,
            self._fk(self.state_context, entities.StateEarlyDischarge, "person"),
        )
