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
"""A PTransform to collect all staff ids associated with each root entity
(e.g. StatePerson).
"""
from typing import Generic, Iterable, Type

import apache_beam as beam
from more_itertools import one

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
)
from recidiviz.persistence.entity.state.entities import StateStaff
from recidiviz.persistence.entity.walk_entity_dag import EntityDagEdge, walk_entity_dag
from recidiviz.persistence.persistence_utils import RootEntityT
from recidiviz.pipelines.utils.execution_utils import RootEntityId
from recidiviz.utils import environment
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
ROOT_ENTITY_IDS_KEY = "root_entity_ids"

# These states actually have issues in real ingested data where we are
# referencing supervisors that are not real. They need to be fixed before we can
# strictly hydrate supervisor_staff_id on StateStaffSupervisorPeriods
_STATES_WITH_INVALID_STAFF_SUPERVISOR_MAPPINGS: set[StateCode] = {
    # TODO(#46144): Fix invalid supervisors for US_TN and then remove this exemption.
    StateCode.US_TN,
    # TODO(#46146): Fix invalid supervisors for US_TX and then remove this exemption.
    StateCode.US_TX,
}


class CreateRootEntityIdToStaffIdMapping(beam.PTransform, Generic[RootEntityT]):
    """A PTransform to collect all staff ids associated with each root entity (e.g.
    StatePerson).
    """

    def __init__(self, root_entity_cls: Type[RootEntityT]) -> None:
        super().__init__()
        self.root_entity_cls: Type[RootEntityT] = root_entity_cls

    def expand(
        self,
        input_or_inputs: tuple[
            beam.PCollection[StateStaff], beam.PCollection[RootEntityT]
        ],
    ) -> beam.PCollection[tuple[RootEntityId, StaffExternalIdToIdMap]]:
        staff_entities, pre_normalization_root_entities = input_or_inputs

        # Produce map of (id_type, external_id) pair to the associated staff_id
        normalized_staff_id_by_staff_external_id: beam.PCollection[
            tuple[StaffExternalId, StaffId]
        ] = staff_entities | beam.FlatMap(
            lambda staff: [
                (e, staff.staff_id)
                for e in self._extract_staff_external_ids_from_staff(staff)
            ]
        )

        # Produce map of (id_type, external_id) pair to the associated root entity
        # internal id for all STAFF external ids
        root_entity_id_by_staff_external_id: beam.PCollection[
            tuple[StaffExternalId, RootEntityId]
        ] = pre_normalization_root_entities | beam.FlatMap(
            lambda root_entity: [
                (e, root_entity.get_id())
                for e in self._extract_staff_external_ids_from_root_entity(root_entity)
            ]
        )

        # Return a PCollection of root entity internal id to dictionaries containing
        # staff external id -> staff_id mappings for every staff external id referenced
        # on this root entity.
        return (
            {
                STAFF_IDS_KEY: normalized_staff_id_by_staff_external_id,
                ROOT_ENTITY_IDS_KEY: root_entity_id_by_staff_external_id,
            }
            | beam.CoGroupByKey()
            | beam.FlatMap(self._map_for_single_external_id)
            | beam.GroupByKey()
            | beam.MapTuple(
                lambda root_entity_id, staff_external_id_to_staff_id: (
                    root_entity_id,
                    dict(staff_external_id_to_staff_id),
                )
            )
        )

    @staticmethod
    def _extract_staff_external_ids_from_staff(
        staff: StateStaff,
    ) -> list[StaffExternalId]:
        """Extract (external_id, id_type) tuples from the given staff."""
        return [(e.external_id, e.id_type) for e in staff.external_ids]

    def _extract_staff_external_ids_from_root_entity(
        self,
        root_entity: RootEntityT,
    ) -> list[StaffExternalId]:
        """Extract (external_id, id_type) tuples for all staff external ids referenced
        by the given root_entity.
        """
        # TODO(#45401): Remove this exemption logic once data for all states has been
        #  corrected and _STATES_WITH_INVALID_STAFF_SUPERVISOR_MAPPINGS is empty.
        if self.root_entity_cls is StateStaff:
            if (
                not environment.in_test()
                and StateCode(root_entity.state_code)
                in _STATES_WITH_INVALID_STAFF_SUPERVISOR_MAPPINGS
            ):
                return []

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
            entities_module_context=entities_module_context_for_entity(root_entity),
            dag_root_entity=root_entity,
            node_processing_fn=_get_referenced_staff_external_id,
        )

        return list({e for e in external_ids if e is not None})

    @staticmethod
    def _map_for_single_external_id(
        staff_external_id_to_grouped_ids: tuple[
            StaffExternalId, dict[str, Iterable[RootEntityId] | Iterable[StaffId]]
        ]
    ) -> list[tuple[RootEntityId, tuple[StaffExternalId, StaffId]]]:
        """Given a staff external id and input defining the staff_id for that staff
        external id and the root_entity_ids associated with that staff external id,
        returns a list of key value pairs where each key is a root_entity_id and each
        pair is the (staff external id, staff_id) value.
        """
        staff_external_id = staff_external_id_to_grouped_ids[0]
        grouped_ids = staff_external_id_to_grouped_ids[1]
        staff_ids = list(grouped_ids[STAFF_IDS_KEY])

        staff_id = one(
            staff_ids,
            too_short=ValueError(
                f"Did not find any ingest StateStaff corresponding to "
                f"(external_id, id_type)={staff_external_id}"
            ),
            too_long=ValueError(
                f"Found more than one ingested StateStaff with "
                f"(external_id, id_type)={staff_external_id}. staff_id values: "
                f"{staff_ids}"
            ),
        )

        return [
            (root_entity_id, (staff_external_id, staff_id))
            for root_entity_id in grouped_ids[ROOT_ENTITY_IDS_KEY]
        ]
