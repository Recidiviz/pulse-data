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

# The candidate form of an identity cluster: the external-ID membership that
# clustering produced, paired with the sourced fragments carrying those IDs,
# before they are validated and merged into an IdentityCluster.
IdentityClusterCandidate = tuple[ClusterKey, Iterable[SourcedIdentityFragment]]


class BuildIdentityClusters(beam.PTransform):
    """Builds one IdentityCluster from each cluster of external IDs that
    clustering produced.
    """

    def __init__(self, *, tenant: Tenant, valid_id_types: frozenset[str]) -> None:
        super().__init__()
        # Tenant the pipeline is running for; every cluster belongs to it.
        self.tenant = tenant
        # External ID types the tenant's launchable identity ingest views can
        # produce; every external ID on a cluster must have one of these types.
        self.valid_id_types = valid_id_types

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
          4. build_cluster constructs an IdentityCluster from an
             IdentityClusterCandidate, first checking that the candidate
             satisfies certain structural invariants.
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

    def build_cluster(self, candidate: IdentityClusterCandidate) -> IdentityCluster:
        """Constructs an IdentityCluster from an IdentityClusterCandidate, first
        checking that the candidate satisfies certain structural invariants.
        """
        cluster_key, sourced_fragments_iterable = candidate
        sourced_fragments = tuple(sourced_fragments_iterable)
        fragments = [fragment for _, _, fragment in sourced_fragments]

        if not fragments:
            raise ValueError(f"Cluster [{cluster_key}] has no fragments.")

        self._raise_on_structural_errors(cluster_key, sourced_fragments)

        # person_type lives on the fragment, not in its attributes, so it is set
        # explicitly on the cluster here rather than carried through the merged
        # attributes. Every fragment in a cluster describes the same logical
        # person, so they must all agree on a single person type.
        person_types = {fragment.person_type for fragment in fragments}
        try:
            person_type = one(person_types)
        except ValueError as e:
            raise ValueError(
                f"Cluster [{cluster_key}] has fragments with conflicting person "
                f"types [{', '.join(sorted(pt.value for pt in person_types))}]; "
                f"every fragment in a cluster must share a single person type."
            ) from e

        field_index = entities_module_context_for_entity_class(
            IdentityAttributes
        ).field_index()

        try:
            cluster_attributes = merge_identity_attributes(fragments, field_index)
        except ValueError as e:
            raise ValueError(f"Failed to build cluster [{cluster_key}]: {e}") from e

        return IdentityCluster(
            tenant=self.tenant,
            external_ids=tuple(
                IdentityClusterExternalId(
                    tenant=self.tenant, external_id=eid, id_type=id_type
                )
                for eid, id_type in cluster_key
            ),
            person_type=person_type,
            **convert_attributes_to_cluster_kwargs(cluster_attributes),
        )

    def _raise_on_structural_errors(
        self,
        cluster_key: ClusterKey,
        sourced_fragments: tuple[SourcedIdentityFragment, ...],
    ) -> None:
        """Raises a ValueError aggregating every per-cluster structural
        violation, or returns cleanly if there are none."""
        errors = [
            *self._check_id_types_are_valid(cluster_key),
            *self._check_external_ids_match_fragments(cluster_key, sourced_fragments),
        ]
        if errors:
            errors_str = "\n  * ".join(errors)
            raise ValueError(
                f"Found errors for cluster with external ids "
                f"{self._format_values(cluster_key)}:\n  * {errors_str}"
            )

    def _check_id_types_are_valid(self, cluster_key: ClusterKey) -> list[str]:
        """Returns an error if any of the cluster's external IDs has an id_type
        that no launchable identity ingest view for the tenant can produce."""
        invalid_id_types = sorted(
            {
                id_type
                for _, id_type in cluster_key
                if id_type not in self.valid_id_types
            }
        )
        if invalid_id_types:
            return [
                f"Found external id types {self._format_values(invalid_id_types)} "
                f"that are not produced by any launchable identity ingest view "
                f"for tenant [{self.tenant.value}]. Valid types: "
                f"{self._format_values(sorted(self.valid_id_types))}."
            ]
        return []

    @staticmethod
    def _check_external_ids_match_fragments(
        cluster_key: ClusterKey,
        sourced_fragments: tuple[SourcedIdentityFragment, ...],
    ) -> list[str]:
        """Returns errors if the cluster's external IDs and the contributing
        fragments' external IDs are not the same set: an external ID on the
        cluster that appears in no fragment is a phantom ID, and a fragment
        external ID missing from the cluster indicates CoGroupByKey leakage
        upstream (the fragment belongs to a different cluster)."""
        cluster_eids = set(cluster_key)
        fragment_eids = {
            (e.external_id, e.id_type)
            for _, _, fragment in sourced_fragments
            for e in fragment.external_ids
        }
        errors = []
        if phantom_eids := sorted(cluster_eids - fragment_eids):
            errors.append(
                f"Found external ids "
                f"{BuildIdentityClusters._format_values(phantom_eids)} on "
                f"the cluster that do not appear in any contributing fragment."
            )
        if leaked_eids := sorted(fragment_eids - cluster_eids):
            errors.append(
                f"Found contributing fragments with external ids "
                f"{BuildIdentityClusters._format_values(leaked_eids)} that "
                f"are not on the cluster."
            )
        return errors

    @staticmethod
    def _format_values(values: Iterable[object]) -> str:
        """Formats the given values for an error message as a bracketed,
        comma-separated list, e.g. [T1, T2]."""
        return f"[{', '.join(str(v) for v in values)}]"
