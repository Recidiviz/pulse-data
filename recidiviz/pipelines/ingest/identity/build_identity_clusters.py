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

from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity_class,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
    IdentityExternalId,
    IdentityFragment,
)
from recidiviz.pipelines.ingest.identity.identity_cluster import IdentityCluster
from recidiviz.pipelines.ingest.identity.merge_identity_attributes import (
    merge_identity_attributes,
)
from recidiviz.pipelines.ingest.transforms.types import ClusterKey
from recidiviz.pipelines.ingest.types import (
    ExternalIdKey,
    IngestViewName,
    UpperBoundDate,
)

CLUSTER_MEMBERSHIPS = "cluster_memberships"
FRAGMENTS_WITH_DATES = "fragments_with_dates"


class BuildIdentityClusters(beam.PTransform):
    """Joins cluster memberships with per-external-ID fragments, merges
    attributes, and produces one IdentityCluster per cluster."""

    def __init__(self, tenant: str) -> None:
        super().__init__()
        self.tenant = tenant

    def expand(
        self,
        input_or_inputs: dict[
            str,
            beam.PCollection[tuple[ExternalIdKey, ClusterKey]]
            | beam.PCollection[
                tuple[
                    ExternalIdKey,
                    tuple[UpperBoundDate, IngestViewName, IdentityFragment],
                ]
            ],
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
                    external_ids=[EID("A","T1"), EID("B","T2")],
                    attributes=IdentityAttributes(
                        name="John", birthdate=1990-01-01, ...
                    ),
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
    ) -> Iterator[
        tuple[
            ClusterKey,
            tuple[UpperBoundDate, IngestViewName, IdentityFragment],
        ]
    ]:
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
            Iterable[tuple[UpperBoundDate, IngestViewName, IdentityFragment]],
        ],
    ) -> IdentityCluster:
        """Folds the cluster's full timeline of fragments into one
        IdentityCluster."""
        cluster_key, fragments_iterable = cluster_key_and_fragments

        cluster_external_ids = [
            IdentityExternalId(tenant=self.tenant, external_id=eid, id_type=id_type)
            for eid, id_type in cluster_key
        ]

        all_fragments = [fragment for _, _, fragment in fragments_iterable]
        if not all_fragments:
            raise ValueError(f"Cluster {cluster_key} has no fragments.")

        field_index = entities_module_context_for_entity_class(
            IdentityAttributes
        ).field_index()

        cluster_attributes = merge_identity_attributes(all_fragments, field_index)

        if not cluster_attributes.has_at_least_one_attribute(field_index):
            raise ValueError(
                f"Cluster {cluster_key} with external ids "
                f"{cluster_external_ids} has no attributes."
            )

        return IdentityCluster(
            external_ids=cluster_external_ids,
            attributes=cluster_attributes,
        )
