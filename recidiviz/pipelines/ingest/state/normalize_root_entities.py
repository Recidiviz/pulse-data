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
from typing import Callable, Generic, Type

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
from recidiviz.persistence.persistence_utils import NormalizedRootEntityT, RootEntityT
from recidiviz.pipelines.ingest.state.create_person_id_to_staff_id_mapping import (
    CreatePersonIdToStaffIdMapping,
    PersonId,
    StaffExternalIdToIdMap,
    StaffId,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_state_person import (
    build_normalized_state_person,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_state_staff import (
    build_normalized_state_staff,
)
from recidiviz.pipelines.ingest.state.run_validations import RunValidations


class _NormalizeRootEntitiesOfType(
    beam.PTransform, Generic[RootEntityT, NormalizedRootEntityT]
):
    """
    A PTransform to normalize a specific group of entities. Accepts two PCollections as
    inputs:
        * A PCollection containing the entities to normalize
        * A PCollection containing a mapping of root entity primary key to a dictionary
          with info about other staff referenced by that root entity.
    """

    PRE_NORMALIZATION_ENTITIES_BY_ROOT_ENTITY_ID_KEY = (
        "pre_normalization_entities_by_root_entity_id"
    )
    ROOT_ENTITY_ID_TO_STAFF_EXTERNAL_IDS_MAP_KEY = (
        "root_entity_id_to_staff_external_ids_map"
    )

    def __init__(
        self,
        root_entity_cls: Type[RootEntityT],
        normalized_root_entity_cls: Type[NormalizedRootEntityT],
        root_entity_normalization_fn: Callable[
            [RootEntityT, StaffExternalIdToIdMap, set[type[Entity]]],
            NormalizedRootEntityT,
        ],
        expected_output_entity_classes: set[type[Entity]],
    ) -> None:
        super().__init__()
        self.root_entity_normalization_fn: Callable[
            [RootEntityT, StaffExternalIdToIdMap, set[type[Entity]]],
            NormalizedRootEntityT,
        ] = root_entity_normalization_fn
        self.root_entity_cls: Type[RootEntityT] = root_entity_cls
        self.normalized_root_entity_cls: Type[
            NormalizedRootEntityT
        ] = normalized_root_entity_cls
        self.root_entity_pk_column_name = root_entity_cls.get_primary_key_column_name()
        self.expected_output_entity_classes: set[
            type[Entity]
        ] = expected_output_entity_classes

    def expand(
        self,
        input_or_inputs: tuple[
            beam.PCollection[RootEntityT],
            beam.PCollection[tuple[PersonId | StaffId, StaffExternalIdToIdMap]],
        ],
    ) -> beam.PCollection[RootEntityT]:
        (
            pre_normalization_root_entities,
            root_entity_to_related_staff_map,
        ) = input_or_inputs

        # Key root entities by their primary key
        pre_normalization_root_entities_by_pk = (
            pre_normalization_root_entities
            | f"Key root_entities by {self.root_entity_pk_column_name}"
            >> beam.Map(lambda e: (e.get_id(), e))
        )

        # Create tuples of (root_entity, staff_id_map, expected_output_classes)
        entities_with_id_mappings = (
            {
                self.ROOT_ENTITY_ID_TO_STAFF_EXTERNAL_IDS_MAP_KEY: root_entity_to_related_staff_map,
                self.PRE_NORMALIZATION_ENTITIES_BY_ROOT_ENTITY_ID_KEY: pre_normalization_root_entities_by_pk,
            }
            | "CoGroupByKey" >> beam.CoGroupByKey()
            | "Create (entity, staff_external_ids_map) tuples"
            >> beam.MapTuple(
                lambda _id, grouped_items: (
                    one(
                        grouped_items[
                            self.PRE_NORMALIZATION_ENTITIES_BY_ROOT_ENTITY_ID_KEY
                        ]
                    ),
                    (
                        one(id_map_values)
                        if (
                            id_map_values := grouped_items[
                                self.ROOT_ENTITY_ID_TO_STAFF_EXTERNAL_IDS_MAP_KEY
                            ]
                        )
                        else {}
                    ),
                    self.expected_output_entity_classes,
                )
            )
        )

        return (
            entities_with_id_mappings
            | f"Build {self.normalized_root_entity_cls.__name__}"
            >> beam.MapTuple(self.root_entity_normalization_fn)
        )


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
        ] = pre_normalization_staff | "Normalize StateStaff" >> beam.Map(
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

        # Normalize persons
        normalized_persons: beam.PCollection[NormalizedStatePerson] = (
            pre_normalization_persons,
            person_id_to_staff_external_ids_map,
        ) | "Normalize StatePerson" >> _NormalizeRootEntitiesOfType(
            root_entity_cls=state_entities.StatePerson,
            normalized_root_entity_cls=normalized_entities.NormalizedStatePerson,
            root_entity_normalization_fn=build_normalized_state_person,
            expected_output_entity_classes=self.expected_output_entity_classes,
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
