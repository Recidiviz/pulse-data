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
"""A PTransform that merges root entity trees from a single ingest view."""
from types import ModuleType
from typing import Generic, List, Tuple

import apache_beam as beam
import attr
from more_itertools import one

from recidiviz.persistence.entity.base_entity import (
    HasMultipleExternalIdsEntity,
    HasMultipleExternalIdsEntityT,
)
from recidiviz.persistence.entity_matching.ingest_view_tree_merger import (
    IngestViewTreeMerger,
)
from recidiviz.pipelines.ingest.transforms.types import (
    ExternalIdKey,
    IngestViewName,
    UpperBoundDate,
)


@attr.s
class _RootEntityWithMetadata:
    external_id_key: ExternalIdKey = attr.ib()
    upperbound_date: UpperBoundDate = attr.ib()
    root_entity: HasMultipleExternalIdsEntity = attr.ib()


class MergeIngestViewRootEntityTrees(
    beam.PTransform, Generic[HasMultipleExternalIdsEntityT]
):
    """A PTransform that merges root entity trees together from a single ingest view.

    The input to this PTransform is a
    PCollection[Tuple[UpperBoundDate, HasMultipleExternalIdsEntityT]].

    For each entity, we extract one metadata record per external ID. We then
    GroupBy (external_id_key, upperbound_date) to collect entities that share an
    external ID and date. Finally, we merge each group via IngestViewTreeMerger
    and emit one tuple per group keyed by ExternalIdKey:

        PCollection[Tuple[ExternalIdKey,
                          Tuple[UpperBoundDate, IngestViewName,
                                HasMultipleExternalIdsEntityT]]]

    We preserve the ingest view name in the result in order to handle ordering of
    merging later on.
    """

    def __init__(
        self,
        ingest_view_name: str,
        entities_module: ModuleType,
        should_throw_on_conflicts: bool = True,
    ):
        super().__init__()
        self.ingest_view = ingest_view_name
        self._entities_module = entities_module
        self._should_throw_on_conflicts = should_throw_on_conflicts

    def expand(
        self,
        input_or_inputs: beam.PCollection[
            Tuple[UpperBoundDate, HasMultipleExternalIdsEntityT]
        ],
    ) -> beam.PCollection[
        Tuple[
            ExternalIdKey,
            Tuple[UpperBoundDate, IngestViewName, HasMultipleExternalIdsEntityT],
        ]
    ]:
        return (
            input_or_inputs
            | f"Obtain {self.ingest_view} entity metadata from entities with dates"
            >> beam.FlatMap(self._get_entity_metadata)
            | f"Group {self.ingest_view} entity metadata by external_id and date"
            >> beam.GroupBy("external_id_key", "upperbound_date")
            | f"Extract entities from {self.ingest_view} entity metadata"
            >> beam.MapTuple(
                lambda key, values: (
                    (key[0], key[1]),
                    [value.root_entity for value in values],
                )
            )
            | f"Merge {self.ingest_view} entities" >> beam.Map(self._merge_entities)
        )

    @staticmethod
    def _get_entity_metadata(
        element: Tuple[UpperBoundDate, HasMultipleExternalIdsEntityT],
    ) -> List[_RootEntityWithMetadata]:
        upperbound_date, root_entity = element
        if not isinstance(root_entity, HasMultipleExternalIdsEntity):
            raise ValueError(
                f"Unexpected root entity type that does not have multiple "
                f"external ids: {type(element)}"
            )

        return [
            _RootEntityWithMetadata(
                external_id_key=(
                    external_id_object.external_id,
                    external_id_object.id_type,
                ),
                upperbound_date=upperbound_date,
                root_entity=root_entity,
            )
            for external_id_object in root_entity.get_external_ids()
        ]

    def _merge_entities(
        self,
        element: Tuple[
            Tuple[ExternalIdKey, UpperBoundDate],
            List[HasMultipleExternalIdsEntityT],
        ],
    ) -> Tuple[
        ExternalIdKey,
        Tuple[UpperBoundDate, IngestViewName, HasMultipleExternalIdsEntityT],
    ]:
        key, entities = element
        external_id_key, upperbound_date = key

        ingest_view_tree_merger = IngestViewTreeMerger(self._entities_module)
        merged_entity = one(
            ingest_view_tree_merger.merge(entities, self._should_throw_on_conflicts)
        )
        return (external_id_key, (upperbound_date, self.ingest_view, merged_entity))
