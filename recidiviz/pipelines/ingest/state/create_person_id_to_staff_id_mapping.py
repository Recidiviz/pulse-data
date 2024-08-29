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
"""A PTransform to collect all staff ids associated with each StatePerson."""
from typing import Iterable

import apache_beam as beam
from more_itertools import one

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.persistence.entity.state.normalized_entities import NormalizedStateStaff
from recidiviz.persistence.entity.walk_entity_dag import EntityDagEdge, walk_entity_dag
from recidiviz.utils.types import assert_type

PersonId = int
StaffId = int
StaffExternalId = tuple[
    # String external_id
    str,
    # String id type
    str,
]
StaffExternalIdToIdMap = dict[StaffExternalId, StaffId]

STAFF_IDS_KEY = "staff_ids"
PERSON_IDS_KEY = "person_ids"


class CreatePersonIdToStaffIdMapping(beam.PTransform):
    """A PTransform to collect all staff ids associated with each StatePerson."""

    def expand(
        self,
        input_or_inputs: tuple[
            beam.PCollection[NormalizedStateStaff], beam.PCollection[StatePerson]
        ],
    ) -> beam.PCollection[tuple[PersonId, StaffExternalIdToIdMap]]:
        normalized_staff, pre_normalization_persons = input_or_inputs

        # Produce map of (id_type, external_id) pair to the associated staff_id
        normalized_staff_id_by_staff_external_id: beam.PCollection[
            tuple[StaffExternalId, StaffId]
        ] = normalized_staff | beam.FlatMap(
            lambda staff: [
                (e, staff.staff_id)
                for e in self._extract_staff_external_ids_from_staff(staff)
            ]
        )

        # Produce map of (id_type, external_id) pair to the associated person_id
        # for all STAFF external ids
        person_id_by_normalized_staff_external_id: beam.PCollection[
            tuple[StaffExternalId, PersonId]
        ] = pre_normalization_persons | beam.FlatMap(
            lambda person: [
                (e, person.person_id)
                for e in self._extract_staff_external_ids_from_person(person)
            ]
        )

        # Return a PCollection of person_id to dictionaries containing
        # staff external id -> staff_id mappings for every staff external id referenced
        # on this person.
        return (
            {
                STAFF_IDS_KEY: normalized_staff_id_by_staff_external_id,
                PERSON_IDS_KEY: person_id_by_normalized_staff_external_id,
            }
            | beam.CoGroupByKey()
            | beam.FlatMap(self._map_for_single_external_id)
            | beam.GroupByKey()
            | beam.MapTuple(
                lambda person_id, staff_external_id_to_staff_id: (
                    person_id,
                    dict(staff_external_id_to_staff_id),
                )
            )
        )

    @staticmethod
    def _extract_staff_external_ids_from_staff(
        staff: NormalizedStateStaff,
    ) -> list[StaffExternalId]:
        """Extract (external_id, id_type) tuples from the given staff."""
        return [(e.external_id, e.id_type) for e in staff.external_ids]

    def _extract_staff_external_ids_from_person(
        self,
        person: StatePerson,
    ) -> list[StaffExternalId]:
        """Extract (external_id, id_type) tuples for all staff external ids referenced
        by the given person.
        """

        def _get_referenced_staff_external_id(
            entity: Entity, _dag_edges: list[EntityDagEdge]
        ) -> StaffExternalId | None:
            for field_name in attribute_field_type_reference_for_class(
                type(entity)
            ).fields:
                if field_name.endswith("staff_external_id"):
                    staff_external_id = getattr(entity, field_name)
                    staff_external_id_type = getattr(entity, f"{field_name}_type")
                    if staff_external_id:
                        return (
                            assert_type(staff_external_id, str),
                            assert_type(staff_external_id_type, str),
                        )
            return None

        external_ids = walk_entity_dag(
            dag_root_entity=person,
            node_processing_fn=_get_referenced_staff_external_id,
        )

        return list({e for e in external_ids if e is not None})

    @staticmethod
    def _map_for_single_external_id(
        staff_external_id_to_grouped_ids: tuple[
            StaffExternalId, dict[str, Iterable[PersonId] | Iterable[StaffId]]
        ]
    ) -> list[tuple[PersonId, tuple[StaffExternalId, StaffId]]]:
        """Given a staff external id and input defining the staff_id for that staff
        external id and the person_ids associated with that staff external id, returns
        a list of key value pairs where each key is a person_id and each pair is the
        (staff external id, staff_id) value.
        """
        staff_external_id = staff_external_id_to_grouped_ids[0]
        grouped_ids = staff_external_id_to_grouped_ids[1]

        staff_id = one(grouped_ids[STAFF_IDS_KEY])

        return [
            (person_id, (staff_external_id, staff_id))
            for person_id in grouped_ids[PERSON_IDS_KEY]
        ]
