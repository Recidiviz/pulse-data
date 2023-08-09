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
"""A DoFn to extract external IDs from entities."""
from typing import Generator, Set

import apache_beam as beam
from more_itertools import one

from recidiviz.persistence.entity.base_entity import (
    HasMultipleExternalIdsEntity,
    RootEntity,
)
from recidiviz.pipelines.ingest.state.constants import (
    ExternalIdClusterEdge,
    ExternalIdKey,
)


# pylint: disable=arguments-differ,abstract-method
class GetRootExternalIdClusterEdges(beam.DoFn):
    """A DoFn to extract all external ids from entities. Output is in the form of tuples
    describing any associations found between an external id and other external ids.
    Each root entity with only a single external id emits a tuple of the format (external_id, None).

    For example, if we have the following entities:
        Entity A: External ID 1, External ID 2
        Entity B: External ID 1, External ID 3
        Entity C: External ID 2, External ID 3
        Entity D: External ID 4

    The output of this DoFn would be:
        Entity A: (External ID 1, External ID 2), (External ID 2, External ID 1)
        Entity B: (External ID 1, External ID 3), (External ID 3, External ID 1)
        Entity C: (External ID 2, External ID 3), (External ID 3, External ID 2)
        Entity D: (External ID 4, None)
    """

    def process(
        self, element: RootEntity
    ) -> Generator[ExternalIdClusterEdge, None, None]:
        """Extracts all external ids from the given root entity and emits tuples describing
        any associations found between an external id and other external ids."""

        if not isinstance(element, HasMultipleExternalIdsEntity):
            raise ValueError(
                f"Unexpected root entity type that does not have multiple external ids: {type(element)}"
            )

        external_id_objects = element.get_external_ids()

        external_ids: Set[ExternalIdKey] = {
            (external_id_object.external_id, external_id_object.id_type)
            for external_id_object in external_id_objects
        }

        if len(external_ids) == 1:
            yield (one(external_ids), None)
        else:
            for external_id in external_ids:
                for other_external_id in external_ids:
                    if external_id != other_external_id:
                        yield (external_id, other_external_id)
