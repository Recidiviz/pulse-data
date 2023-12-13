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
"""A DoFn that merges entity trees together via entity matching."""
import sys
from typing import Dict, Iterable, Tuple

import apache_beam as beam

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import RootEntity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    set_backedges,
)
from recidiviz.persistence.entity_matching.root_entity_update_merger import (
    RootEntityUpdateMerger,
)
from recidiviz.pipelines.ingest.state.constants import IngestViewName, UpperBoundDate
from recidiviz.pipelines.ingest.state.exemptions import INGEST_VIEW_ORDER_EXEMPTIONS
from recidiviz.pipelines.ingest.state.generate_primary_keys import (
    generate_primary_keys_for_root_entity_tree,
)
from recidiviz.pipelines.metrics.utils.calculator_utils import safe_list_index
from recidiviz.pipelines.utils.entities.generate_primary_key import PrimaryKey


class MergeRootEntitiesAcrossDates(beam.PTransform):
    """A PTransform that merges entity trees together via entity matching and then
    properly sets backedges and then primary keys."""

    def __init__(
        self, state_code: StateCode, field_index: CoreEntityFieldIndex
    ) -> None:
        super().__init__()
        self.state_code = state_code
        self.field_index = field_index

    def expand(
        self,
        input_or_inputs: beam.PCollection[
            Tuple[
                PrimaryKey,
                Dict[Tuple[UpperBoundDate, IngestViewName], Iterable[RootEntity]],
            ]
        ],
    ) -> beam.PCollection[RootEntity]:
        return (
            input_or_inputs
            | "Merge all root entities together via entity matching"
            >> beam.Map(self._entity_match)
            | "Set backedges for all entities in root entity tree"
            >> beam.MapTuple(
                lambda primary_key, root_entity: (
                    primary_key,
                    set_backedges(root_entity, self.field_index),
                )
            )
            | "Set primary keys for all entities in root entity tree"
            >> beam.MapTuple(
                lambda primary_key, root_entity: generate_primary_keys_for_root_entity_tree(
                    primary_key, root_entity, self.state_code, self.field_index
                )
            )
        )

    def _entity_match(
        self,
        element: Tuple[
            PrimaryKey,
            Dict[Tuple[UpperBoundDate, IngestViewName], Iterable[RootEntity]],
        ],
    ) -> Tuple[PrimaryKey, RootEntity]:
        """Merges all root entities together via entity matching. If the state has a
        defined order for ingest views, then the root entities will be merged in that
        order. Otherwise, the root entities will be merged in alphabetical order."""
        root_entity_merger = RootEntityUpdateMerger(self.field_index)
        primary_key, root_entity_dictionary = element
        merged_root_entity = None
        sorted_keys = sorted(
            root_entity_dictionary.keys(),
            key=lambda key: (
                key[0],
                # TODO(#20930) Remove this once we no longer need deterministic ordering.
                safe_list_index(
                    INGEST_VIEW_ORDER_EXEMPTIONS[self.state_code], key[1], sys.maxsize
                )
                if self.state_code in INGEST_VIEW_ORDER_EXEMPTIONS
                else key[1],
            ),
        )
        for date_timestamp, ingest_view_name in sorted_keys:
            root_entities = list(
                root_entity_dictionary[(date_timestamp, ingest_view_name)]
            )
            for root_entity in root_entities:
                merged_root_entity = root_entity_merger.merge_root_entity_trees(
                    merged_root_entity, root_entity  # type: ignore
                )
        if merged_root_entity is None:
            raise ValueError(
                "Merged root entity should not be None, as there should always be at least one root entity."
            )
        return primary_key, merged_root_entity
