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
"""PTransform that builds IdentityCluster objects from clustered external IDs
 and per-external-ID identity fragments."""
from collections.abc import Iterable, Iterator

import apache_beam as beam
from more_itertools import one

from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity_class,
)
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterExternalId,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
)
from recidiviz.pipelines.ingest.identity.cluster_entity_conversion_utils import (
    convert_attributes_to_cluster_kwargs,
)
from recidiviz.pipelines.ingest.identity.merge_identity_attributes import (
    merge_identity_attributes,
)
from recidiviz.pipelines.ingest.identity.types import SourcedIdentityFragment
from recidiviz.pipelines.ingest.transforms.types import ClusterKey
from recidiviz.pipelines.ingest.types import ExternalIdKey

CLUSTER_MEMBERSHIPS = "cluster_memberships"
FRAGMENTS_WITH_DATES = "fragments_with_dates"


class BuildIdentityClusters(beam.PTransform):
    """Joins cluster memberships with per-external-ID fragments, merges
    attributes, and produces one IdentityCluster per cluster."""

    def __init__(self, tenant: Tenant) -> None:
        super().__init__()
        self.tenant = tenant

    def expand(
        self,
        input_or_inputs: dict[
            str,
            beam.PCollection[tuple[ExternalIdKey, ClusterKey]]
            | beam.PCollection[tuple[ExternalIdKey, SourcedIdentityFragment]],
        ],
    ) -> beam.PCollection[IdentityCluster]:
        """Takes a dict with two keyed PCollections and produces one
        IdentityCluster per cluster.

        Example input::

            {
                "cluster_memberships": PCollection[
                    (("A", "T1"), (("A", "T1"), ("B", "T2"))),
                    (("B", "T2"), (("A", "T1"), ("B", "T2"))),
                ],
                "fragments_with_dates": PCollection[
                    (("A", "T1"), (100.0, "view_a", fragment{name="John"})),
                    (("B", "T2"), (200.0, "view_a", fragment{birthdate=1990-01-01})),
                ],
            }

        Example output::

            PCollection[
                IdentityCluster(
                    external_ids=(
                        IdentityClusterExternalId(external_id="A", id_type="T1"),
                        IdentityClusterExternalId(external_id="B", id_type="T2"),
                    ),
                    person_type=PersonType.JII,
                    birthdate=1990-01-01,
                    name=IdentityClusterName(given_name="John", ...),
                    ...
                ),
            ]

        Steps:
          1. CoGroupByKey joins the two inputs by ExternalIdKey.
          2. rekey_fragments_by_cluster re-keys each fragment by its cluster
             (a sorted tuple of all ExternalIdKeys in the cluster).
          3. GroupByKey groups all fragments sharing the same cluster key.
          4. build_cluster merges the cluster's fragment timeline and
             constructs an IdentityCluster.
        """
        return (
            input_or_inputs
            | "CoGroup the PCollections by ExternalIdKey" >> beam.CoGroupByKey()
            | "Rekey fragments by cluster"
            >> beam.FlatMap(self.rekey_fragments_by_cluster)
            | "Group by cluster" >> beam.GroupByKey()
            | "Build IdentityCluster" >> beam.Map(self.build_cluster)
        )

    def rekey_fragments_by_cluster(
        self,
        external_id_and_collections: tuple[
            ExternalIdKey,
            dict[str, Iterable],
        ],
    ) -> Iterator[tuple[ClusterKey, SourcedIdentityFragment]]:
        """For each fragment associated with an external ID, yields an entry
        keyed by the cluster (a sorted tuple of all external IDs in the
        cluster to which this external ID belongs).

        Example input::

            (
                ("A", "T1"),
                {
                    "cluster_memberships":   [(("A", "T1"), ("B", "T2"))],
                    "fragments_with_dates":  [
                        (100.0, "view_a", fragment_1),
                        (200.0, "view_b", fragment_2),
                    ],
                },
            )

        Example output::

            [
                ((("A", "T1"), ("B", "T2")), (100.0, "view_a", fragment_1)),
                ((("A", "T1"), ("B", "T2")), (200.0, "view_b", fragment_2)),
            ]
        """
        external_id, collections = external_id_and_collections
        cluster_memberships = list(collections[CLUSTER_MEMBERSHIPS])
        try:
            cluster_key: ClusterKey = one(cluster_memberships)
        except ValueError as e:
            raise ValueError(
                f"External id {external_id} is expected to be in one cluster "
                f"but is in {len(cluster_memberships)}."
            ) from e
        for fragment_with_date in collections[FRAGMENTS_WITH_DATES]:
            yield cluster_key, fragment_with_date

    def build_cluster(
        self,
        cluster_key_and_fragments: tuple[
            ClusterKey,
            Iterable[SourcedIdentityFragment],
        ],
    ) -> IdentityCluster:
        """Folds the cluster's full timeline of fragments into one
        IdentityCluster."""
        cluster_key, fragments_iterable = cluster_key_and_fragments

        cluster_external_ids = tuple(
            IdentityClusterExternalId(
                tenant=self.tenant, external_id=eid, id_type=id_type
            )
            for eid, id_type in cluster_key
        )

        all_fragments = [fragment for _, _, fragment in fragments_iterable]
        if not all_fragments:
            raise ValueError(f"Cluster {cluster_key} has no fragments.")

        field_index = entities_module_context_for_entity_class(
            IdentityAttributes
        ).field_index()

        try:
            cluster_attributes = merge_identity_attributes(all_fragments, field_index)
        except ValueError as e:
            raise ValueError(
                f"Failed to build cluster {cluster_key} with external ids "
                f"{cluster_external_ids}: {e}"
            ) from e

        return IdentityCluster(
            tenant=self.tenant,
            external_ids=cluster_external_ids,
            **convert_attributes_to_cluster_kwargs(cluster_attributes),
        )
