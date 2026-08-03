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
"""PTransform that builds one IdentityCluster from each cluster of external IDs
that clustering produced. Rejects clusters whose fragments have conflicting
attributes (they do not describe one person).
"""
from collections.abc import Iterable, Iterator
from enum import Enum

import apache_beam as beam
from more_itertools import one

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterExternalId,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityFragment,
)
from recidiviz.pipelines.ingest.identity.cluster_entity_conversion_utils import (
    convert_attributes_to_cluster_kwargs,
)
from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.find_fragment_attribute_conflicts import (
    find_fragment_attribute_conflicts,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    ConflictCheckedAttributesConfig,
    IdentityIngestPipelineConfig,
)
from recidiviz.pipelines.ingest.identity.resolve_cluster_attributes import (
    resolve_cluster_attributes,
)
from recidiviz.pipelines.ingest.identity.types import (
    ConflictValue,
    SourcedIdentityFragment,
)
from recidiviz.pipelines.ingest.transforms.types import ClusterKey
from recidiviz.pipelines.ingest.types import (
    ExternalIdKey,
    IngestViewName,
    UpperBoundDate,
)

CLUSTER_MEMBERSHIPS = "cluster_memberships"
FRAGMENTS_WITH_DATES = "fragments_with_dates"

# The candidate form of an identity cluster: the external-ID membership that
# clustering produced, paired with the sourced fragments carrying those IDs,
# before they are validated and merged into an IdentityCluster.
IdentityClusterCandidate = tuple[ClusterKey, Iterable[SourcedIdentityFragment]]


class BuildIdentityClusters(beam.PTransform):
    """Builds one IdentityCluster from each cluster of external IDs that
    clustering produced."""

    def __init__(
        self,
        *,
        tenant: Tenant,
        valid_id_types: frozenset[str],
        identity_config: IdentityIngestPipelineConfig,
    ) -> None:
        super().__init__()
        # Tenant the pipeline is running for; every cluster belongs to it.
        self.tenant = tenant
        # External ID types the tenant's launchable identity ingest views can
        # produce; every external ID on a cluster must have one of these types.
        self.valid_id_types = valid_id_types
        # Loaded per-tenant config supplying each cluster's conflict-check and
        # resolution config (looked up by the cluster's person type).
        self.identity_config = identity_config

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

        Example input:

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

        Example output:

            PCollection[
                IdentityCluster(
                    tenant=Tenant.US_XX,
                    external_ids=(
                        IdentityClusterExternalId(external_id="A", id_type="T1"),
                        IdentityClusterExternalId(external_id="B", id_type="T2"),
                    ),
                    person_type=PersonType.JII,
                    name=IdentityClusterName(given_name="John", ...),
                    birthdate=1990-01-01,
                    ...
                ),
            ]

        Steps:
          1. CoGroupByKey joins the two inputs by ExternalIdKey.
          2. rekey_fragments_by_cluster re-keys each fragment by its cluster
             (a sorted tuple of all ExternalIdKeys in the cluster).
          3. GroupByKey groups all fragments sharing the same cluster key.
          4. build_cluster checks the candidate's structural invariants, checks
             its fragments for attribute conflicts, and constructs an
             IdentityCluster from the resolved attributes.
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

        Example input:

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

        Example output:

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
        """Constructs an IdentityCluster from a candidate, first checking its
        structural invariants and that its fragments are free of attribute conflicts (both
        raise on violation), then resolving the fragments' attributes."""
        cluster_key, sourced_fragments_iterable = candidate
        sourced_fragments = tuple(sourced_fragments_iterable)
        fragments = [fragment for _, _, fragment in sourced_fragments]

        if not fragments:
            raise ValueError(f"Cluster [{cluster_key}] has no fragments.")

        self._raise_on_structural_errors(cluster_key, sourced_fragments)
        person_type = self._single_person_type(cluster_key, fragments)

        # Resolution and the conflict check both consume the fragments
        # oldest-first; see _fragment_recency_sort_key.
        sorted_fragments = [
            fragment
            for _, _, fragment in sorted(
                sourced_fragments, key=_fragment_recency_sort_key
            )
        ]

        tenant_config = self.identity_config.get_tenant_clustering_config(
            self.tenant, person_type
        )

        # TODO(OBT-15461): Instead of raising (which fails the whole run on the
        # first wrongly-merged cluster), build_cluster should emit a cluster
        # with conflicts to a separate rejected_clusters output (a tagged ParDo
        # side-output) rather than the kept-clusters output, so the run
        # completes; a downstream step then writes the rejected clusters to a BQ
        # table for review.
        self._raise_on_attribute_conflicts(
            cluster_key,
            sorted_fragments,
            tenant_config.conflict_checked_attributes_config,
        )

        attributes = resolve_cluster_attributes(
            tenant=self.tenant,
            sorted_fragments=sorted_fragments,
            resolution_strategy_config=tenant_config.resolution_strategy_config,
        )
        return IdentityCluster(
            tenant=self.tenant,
            external_ids=tuple(
                IdentityClusterExternalId(
                    tenant=self.tenant, external_id=eid, id_type=id_type
                )
                for eid, id_type in cluster_key
            ),
            person_type=person_type,
            **(convert_attributes_to_cluster_kwargs(attributes) if attributes else {}),
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

    def _single_person_type(
        self, cluster_key: ClusterKey, fragments: list[IdentityFragment]
    ) -> PersonType:
        """Returns the cluster's single person type, raising if its fragments
        disagree. person_type lives on the fragment, not in its attributes, so
        it is checked and set explicitly. Every fragment in a cluster describes
        the same logical person, so they must all agree."""
        person_types = {fragment.person_type for fragment in fragments}
        try:
            return one(person_types)
        except ValueError as e:
            raise ValueError(
                f"Cluster [{cluster_key}] has fragments with conflicting person "
                f"types [{', '.join(sorted(pt.value for pt in person_types))}]; "
                f"every fragment in a cluster must share a single person type."
            ) from e

    def _raise_on_attribute_conflicts(
        self,
        cluster_key: ClusterKey,
        fragments: list[IdentityFragment],
        conflict_checked_attributes_config: ConflictCheckedAttributesConfig,
    ) -> None:
        """Raises if the cluster's fragments conflict on any conflict-checked
        attribute, meaning clustering wrongly merged two different people.
        """
        conflicts = find_fragment_attribute_conflicts(
            fragments, conflict_checked_attributes_config
        )
        if conflicts:
            conflict_summary = "; ".join(
                f"{conflict.field}: "
                f"{self._format_values(_serialize_conflict_value(v) for v in conflict.values)}"
                for conflict in conflicts
            )
            raise ValueError(
                f"Cluster with external ids {self._format_values(cluster_key)} has "
                f"fragments with conflicting attributes that do not describe one person: "
                f"{conflict_summary}."
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


def _serialize_conflict_value(value: ConflictValue) -> str:
    """Serializes a conflict value for the human-readable rejection message.
    Enums render as their value (MALE, not Sex.MALE); dates and strings render
    as themselves.
    """
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def _fragment_recency_sort_key(
    sourced_fragment: SourcedIdentityFragment,
) -> tuple[UpperBoundDate, IngestViewName, list[tuple[str, str]]]:
    """Orders fragments oldest-first by (upper-bound date, ingest view name),
    tie broken by sorted external ids.

    Latest-wins resolution and the conflict check both consume the fragments in
    this order, so their output stays deterministic. The external-id tiebreak
    matters because duplicate source records for one person share a view and
    date, and would otherwise fall back to nondeterministic GroupByKey order.
    """
    upper_bound_date, ingest_view_name, fragment = sourced_fragment
    return (
        upper_bound_date,
        ingest_view_name,
        sorted((e.external_id, e.id_type) for e in fragment.external_ids),
    )
