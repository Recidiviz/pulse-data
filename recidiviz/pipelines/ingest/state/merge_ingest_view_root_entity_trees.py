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
"""A PTransform that merges root entity trees together."""
from datetime import datetime
from typing import List, Tuple, Union, cast

import apache_beam as beam
import attr
from more_itertools import one

from recidiviz.persistence.entity.base_entity import (
    HasMultipleExternalIdsEntity,
    RootEntity,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import StatePerson, StateStaff
from recidiviz.persistence.entity_matching.ingest_view_tree_merger import (
    IngestViewTreeMerger,
)
from recidiviz.pipelines.ingest.state.constants import ExternalIdKey


@attr.s
class RootEntityWithEntityMetadata:
    external_id_key: ExternalIdKey = attr.ib()
    upperbound_date: datetime = attr.ib()
    root_entity: RootEntity = attr.ib()


class MergeIngestViewRootEntityTrees(beam.PTransform):
    """A PTransform that merges root entity trees together from a single ingest view.

    The input to this PTransform is a PCollection[Tuple[datetime, RootEntity]].
    After the FlatMap(get_entity_metadata), we have a PCollection[EntityMetadata].

    We then do a GroupBy on the external_id_key, upperbound_date to get all of the associated
    RootEntities. So the PCollection is now:
        PCollection[Tuple[Tuple[ExternalIdKey, datetime], List[RootEntity]]].
    Then we merge the RootEntities together and return a:
        PCollection[Tuple[ExternalIdKey, Tuple[datetime, RootEntity]]].

    We assume that all of the RootEntities share the same shape and therefore can be
    merged via the IngestViewTreeMerger.
    """

    def __init__(self, ingest_view_name: str):
        super().__init__()
        self.ingest_view = ingest_view_name

    def expand(
        self, input_or_inputs: beam.PCollection[Tuple[datetime, RootEntity]]
    ) -> beam.PCollection[Tuple[ExternalIdKey, Tuple[datetime, RootEntity]]]:
        return (
            input_or_inputs
            | f"Obtain {self.ingest_view} entity metadata from entities with dates"
            >> beam.FlatMap(self.get_entity_metadata)
            | f"Group {self.ingest_view} entity metadata by external_id and date"
            >> beam.GroupBy("external_id_key", "upperbound_date")
            | f"Extract entities from {self.ingest_view} entity metadata"
            >> beam.MapTuple(
                lambda key, values: (
                    (cast(ExternalIdKey, key[0]), cast(datetime, key[1])),
                    [cast(RootEntity, value.root_entity) for value in values],
                )
            )
            | f"Merge {self.ingest_view} entities" >> beam.Map(self.merge_entities)
        )

    def get_entity_metadata(
        self, element: Tuple[datetime, RootEntity]
    ) -> List[RootEntityWithEntityMetadata]:
        upperbound_date, root_entity = element
        if not isinstance(root_entity, HasMultipleExternalIdsEntity):
            raise ValueError(
                f"Unexpected root entity type that does not have multiple external ids: {type(element)}"
            )

        results: List[RootEntityWithEntityMetadata] = []
        for external_id_object in root_entity.get_external_ids():
            results.append(
                (
                    RootEntityWithEntityMetadata(
                        external_id_key=(
                            external_id_object.external_id,
                            external_id_object.id_type,
                        ),
                        upperbound_date=upperbound_date,
                        root_entity=root_entity,
                    )
                )
            )
        return results

    def merge_entities(
        self,
        element: Tuple[Tuple[ExternalIdKey, datetime], List[RootEntity]],
    ) -> Tuple[ExternalIdKey, Tuple[datetime, RootEntity]]:
        key, entities = element
        external_id_key, upperbound_date = key

        ingest_view_tree_merger = IngestViewTreeMerger(
            field_index=CoreEntityFieldIndex()
        )

        root_entities: Union[List[StatePerson], List[StateStaff]]
        if all(isinstance(e, StatePerson) for e in entities):
            root_entities = cast(List[StatePerson], entities)
        elif all(isinstance(e, StateStaff) for e in entities):
            root_entities = cast(List[StateStaff], entities)
        else:
            raise ValueError(
                f"Found unexpected top-level root entity types: "
                f"[{set(type(e) for e in entities)}]"
            )

        merged_entity: RootEntity = one(ingest_view_tree_merger.merge(root_entities))
        return (external_id_key, (upperbound_date, merged_entity))
