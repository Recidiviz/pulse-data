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
"""A PTransform to cluster root entity IDs together."""
from collections import defaultdict
from typing import Dict, Generator, List, Set

import apache_beam as beam

from recidiviz.pipelines.ingest.state.constants import (
    ExternalIdCluster,
    ExternalIdClusterEdge,
    ExternalIdKey,
)


class ClusterRootExternalIds(beam.PTransform):
    """A PTransform that clusters root entity IDs together.

    Suppose the input looks like so:
        (external_id_1, external_id_2)
        (external_id_2, external_id_3)
        (external_id_2, external_id_4)
        (external_id_1, None)
        (external_id_2, external_id_1)
        (external_id_3, external_id_2)
        (external_id_4, external_id_2)
        (external_id_5, None)
        (external_id_6, external_id_7)
        (external_id_7, external_id_6)
        (external_id_8, external_id_5)
        (external_id_5, external_id_8)
        (external_id_9, None)

    The first step will be to group by key:
        (external_id_1, [external_id_2, None])
        (external_id_2, [external_id_3, external_id_4, external_id_1])
        (external_id_3, [external_id_2])
        (external_id_4, [external_id_2])
        (external_id_5, [None, external_id_8])
        (external_id_6, [external_id_7])
        (external_id_7, [external_id_6])
        (external_id_9, [None])

    The second step will be to make sure that the external_id is included in the list of
    external_ids that are linked to it:
        (external_id_1, [external_id_1, external_id_2])
        (external_id_2, [external_id_1, external_id_2, external_id_3, external_id_4])
        (external_id_3, [external_id_2, external_id_3])
        (external_id_4, [external_id_2, external_id_4])
        (external_id_5, [external_id_5, external_id_8])
        (external_id_6, [external_id_6, external_id_7])
        (external_id_7, [external_id_6, external_id_7])
        (external_id_8, [external_id_5, external_id_8])
        (external_id_9, [external_id_9])

    Then the output will look like so after combining all of the external_id clusters:
        (external_id_1, [external_id_1, external_id_2, external_id_3, external_id_4])
        (external_id_2, [external_id_1, external_id_2, external_id_3, external_id_4])
        (external_id_3, [external_id_1, external_id_2, external_id_3, external_id_4])
        (external_id_4, [external_id_1, external_id_2, external_id_3, external_id_4])
        (external_id_5, [external_id_5, external_id_8])
        (external_id_6, [external_id_6, external_id_7])
        (external_id_7, [external_id_6, external_id_7])
        (external_id_8, [external_id_5, external_id_8])
        (external_id_9, [external_id_9])
    """

    def expand(
        self,
        input_or_inputs: beam.PCollection[ExternalIdClusterEdge],
    ) -> beam.PCollection[ExternalIdCluster]:
        return (
            input_or_inputs
            | "For every external_id, groups all external ids or Nones directly linked via an edge to that external_id"
            >> beam.GroupByKey()
            | "Formats the edges to also include the external_id as an element of the set of linked external_ids"
            >> beam.MapTuple(
                lambda external_id, linked_external_ids: (
                    external_id,
                    {
                        linked_external_id
                        for linked_external_id in linked_external_ids
                        if linked_external_id
                    }
                    | {external_id},
                )
            )
            | "Combine external ID clusters across all elements."
            >> beam.CombineGlobally(CombineExternalIdClusters())
            | "Generate a PCollection of external id clusters from the accumulator"
            >> beam.ParDo(self.split_dictionary_into_elements)
        )

    def split_dictionary_into_elements(
        self, accumulator: Dict[ExternalIdKey, Set[ExternalIdKey]]
    ) -> Generator[ExternalIdCluster, None, None]:
        for external_id, external_ids_in_cluster in accumulator.items():
            yield (external_id, external_ids_in_cluster)


class CombineExternalIdClusters(beam.CombineFn):
    """A CombineFn that combines clusters of external IDs together."""

    # pylint: disable=arguments-differ,abstract-method

    def create_accumulator(self) -> Dict[ExternalIdKey, Set[ExternalIdKey]]:
        return defaultdict(set)

    def add_input(
        self,
        mutable_accumulator: Dict[ExternalIdKey, Set[ExternalIdKey]],
        element: ExternalIdCluster,
    ) -> Dict[ExternalIdKey, Set[ExternalIdKey]]:
        external_id, external_ids_in_element_cluster = element
        if external_id not in external_ids_in_element_cluster:
            raise ValueError("Require that the external_id itself be in the cluster.")

        final_cluster = set(external_ids_in_element_cluster)

        # For each external_id already associated with the current external_id, merge
        # those clusters into one.
        for element_cluster_external_id in external_ids_in_element_cluster:
            external_ids_already_in_cluster: Set[ExternalIdKey] = mutable_accumulator[
                element_cluster_external_id
            ]
            final_cluster |= external_ids_already_in_cluster

        for cluster_member_external_id in final_cluster:
            mutable_accumulator[cluster_member_external_id] = final_cluster
        return mutable_accumulator

    def merge_accumulators(
        self,
        accumulators: List[Dict[ExternalIdKey, Set[ExternalIdKey]]],
    ) -> Dict[ExternalIdKey, Set[ExternalIdKey]]:
        final_accumulator: Dict[ExternalIdKey, Set[ExternalIdKey]] = defaultdict(set)

        for accumulator in accumulators:
            for external_id_key, external_ids_in_cluster in accumulator.items():
                self.add_input(
                    final_accumulator, (external_id_key, external_ids_in_cluster)
                )

        return final_accumulator

    def extract_output(
        self,
        accumulator: Dict[ExternalIdKey, Set[ExternalIdKey]],
    ) -> Dict[ExternalIdKey, Set[ExternalIdKey]]:
        return accumulator
