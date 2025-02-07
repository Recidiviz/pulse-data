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
"""A PTransform that takes in a PCollection of un-normalized RootEntity (i.e.
StateStaff and StatePerson) and transforms those to normalized RootEntity (i.e.
NormalizeStateStaff and NormalizedStatePerson).
"""
import apache_beam as beam
from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStateStaff,
)
from recidiviz.pipelines.ingest.state.create_person_id_to_staff_id_mapping import (
    CreatePersonIdToStaffIdMapping,
    PersonId,
    StaffExternalIdToIdMap,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_state_person import (
    build_normalized_state_person,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_state_staff import (
    build_normalized_state_staff,
)
from recidiviz.pipelines.ingest.state.run_validations import RunValidations

PRE_NORMALIZATION_PERSONS_BY_PERSON_ID_KEY = "pre_normalization_persons_by_person_id"
PERSON_ID_TO_STAFF_EXTERNAL_IDS_MAP_KEY = "person_id_to_staff_external_ids_map"


class NormalizeRootEntities(beam.PTransform):
    """A PTransform that takes in a PCollection of un-normalized RootEntity (i.e.
    StateStaff and StatePerson) and transforms those to normalized RootEntity (i.e.
    NormalizeStateStaff and NormalizedStatePerson).
    """

    def __init__(
        self, state_code: StateCode, expected_output_entity_classes: set[type[Entity]]
    ) -> None:
        super().__init__()
        self.state_code = state_code
        self.expected_output_entity_classes = expected_output_entity_classes

    def expand(
        self,
        input_or_inputs: beam.PCollection[RootEntity],
    ) -> beam.PCollection[RootEntity]:
        (
            pre_normalization_persons,
            pre_normalization_staff,
        ) = input_or_inputs | "Partition by root entity type" >> beam.Partition(
            # apache-beam expects this type to be "WithTypeHints"
            _partition_root_entities,  # type: ignore[arg-type]
            2,
        )

        normalized_staff: beam.PCollection[
            NormalizedStateStaff
        ] = pre_normalization_staff | "Build NormalizedStateStaff" >> beam.Map(
            build_normalized_state_staff
        )

        # Map of person_id to dictionary mapping staff external id -> staff_id
        person_id_to_staff_external_ids_map: beam.PCollection[
            tuple[PersonId, StaffExternalIdToIdMap]
        ] = (
            (
                normalized_staff,
                pre_normalization_persons,
            )
            | "Create person_id to staff_id mapping" >> CreatePersonIdToStaffIdMapping()
        )

        pre_normalization_persons_by_person_id: beam.PCollection[
            tuple[PersonId, state_entities.StatePerson]
        ] = pre_normalization_persons | "Key people by person_id" >> beam.Map(
            lambda p: (p.person_id, p)
        )

        persons_with_staff_id_mappings: beam.PCollection[
            tuple[state_entities.StatePerson, StaffExternalIdToIdMap, set[type[Entity]]]
        ] = (
            {
                PERSON_ID_TO_STAFF_EXTERNAL_IDS_MAP_KEY: person_id_to_staff_external_ids_map,
                PRE_NORMALIZATION_PERSONS_BY_PERSON_ID_KEY: pre_normalization_persons_by_person_id,
            }
            | beam.CoGroupByKey()
            | "Create (person, staff_external_ids_map) tuples"
            >> beam.MapTuple(
                lambda _person_id, grouped_items: (
                    one(grouped_items[PRE_NORMALIZATION_PERSONS_BY_PERSON_ID_KEY]),
                    (
                        one(person_id_to_staff_external_ids_maps)
                        if (
                            person_id_to_staff_external_ids_maps := grouped_items[
                                PERSON_ID_TO_STAFF_EXTERNAL_IDS_MAP_KEY
                            ]
                        )
                        else {}
                    ),
                    self.expected_output_entity_classes,
                )
            )
        )

        normalized_persons: beam.PCollection[NormalizedStatePerson] = (
            persons_with_staff_id_mappings
            | "Build NormalizedStatePerson"
            >> beam.MapTuple(build_normalized_state_person)
        )
        normalized_root_entities: beam.PCollection[RootEntity] = (
            [
                normalized_staff,
                normalized_persons,
            ]
            | "Flatten all normalized root entities into single PCollection"
            >> beam.Flatten()
            | RunValidations(
                state_code=self.state_code,
                expected_output_entity_classes=self.expected_output_entity_classes,
                entities_module=normalized_entities,
            )
        )

        return normalized_root_entities


def _partition_root_entities(root_entity: RootEntity, num_partitions: int) -> int:
    if num_partitions != 2:
        raise ValueError(f"Unexpected number of partitions: {num_partitions}")
    if isinstance(root_entity, state_entities.StatePerson):
        return 0
    if isinstance(root_entity, state_entities.StateStaff):
        return 1
    raise ValueError(f"Unexpected root entity type: {type(root_entity)}")
