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
that clustering produced. Clusters whose fragments have conflicting attributes
are rejected, and emitted as RejectedIdentityClusters rather than
IdentityClusters.

A recorded override can change the outcome for a specific cluster. A BLESS
override keeps a conflicting cluster, emitting an IdentityCluster where the
cluster would otherwise be rejected, but only while the cluster's conflicts
still hash to the value recorded at review time. An EXCLUDE override drops a
cluster whether or not it conflicts, so the transform emits nothing for it.
"""
import logging
from collections.abc import Iterable, Iterator

import apache_beam as beam
import attr
from apache_beam.metrics import Metrics
from more_itertools import one

from recidiviz.common import attr_validators
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
    IdentityClusterExternalId,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityFragment,
)
from recidiviz.pipelines.ingest.identity.build_blessed_cluster_attributes import (
    build_blessed_cluster_attributes,
)
from recidiviz.pipelines.ingest.identity.cluster_entity_conversion_utils import (
    convert_attributes_to_cluster_kwargs,
)
from recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking.find_fragment_attribute_conflicts import (
    find_fragment_attribute_conflicts,
)
from recidiviz.pipelines.ingest.identity.identity_cluster_override import (
    BlessedIdentityValues,
    IdentityClusterOverride,
    IdentityClusterOverrideDisposition,
    IdentityClusterOverrides,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    ConflictCheckedAttribute,
    IdentityIngestPipelineConfig,
    IdentityIngestPipelineTenantConfig,
    ResolutionStrategyConfig,
)
from recidiviz.pipelines.ingest.identity.rejected_identity_cluster import (
    RejectedIdentityCluster,
    hash_of_conflicts,
)
from recidiviz.pipelines.ingest.identity.resolve_cluster_attributes import (
    resolve_cluster_attributes,
)
from recidiviz.pipelines.ingest.identity.types import (
    AttributeConflict,
    SourcedIdentityFragment,
)
from recidiviz.pipelines.ingest.transforms.types import ClusterKey
from recidiviz.pipelines.ingest.types import (
    ExternalIdKey,
    IngestViewName,
    UpperBoundDate,
)
from recidiviz.utils.types import assert_type

CLUSTER_MEMBERSHIPS = "cluster_memberships"
FRAGMENTS_WITH_DATES = "fragments_with_dates"

# Tags for the transform's output.
KEPT_CLUSTERS = "kept_clusters"
REJECTED_CLUSTERS = "rejected_clusters"

# Beam metrics namespace for the counts of clusters each override disposition
# affected.
_OVERRIDE_METRICS_NAMESPACE = "identity_cluster_overrides"

# The candidate form of an identity cluster: the external-ID membership that
# clustering produced, paired with the sourced fragments carrying those IDs,
# before they are validated and merged into an IdentityCluster.
IdentityClusterCandidate = tuple[ClusterKey, Iterable[SourcedIdentityFragment]]


@attr.define(frozen=True, kw_only=True)
class _ExcludedIdentityCluster:
    """A cluster an EXCLUDE override dropped. The transform emits nothing for it,
    so it reaches neither the kept-cluster tables nor the rejected-cluster table.
    build_cluster returns one so the routing step can recognize and drop it."""

    external_ids: ClusterKey = attr.ib(
        validator=[
            attr_validators.is_non_empty_tuple,
            attr_validators.is_tuple_of(tuple),
        ]
    )
    """The dropped cluster's external ids."""


class BuildIdentityClusters(beam.PTransform):
    """Builds one IdentityCluster from each cluster of external IDs that
    clustering produced, routing clusters whose fragments have conflicting
    attributes to a separate REJECTED_CLUSTERS output."""

    def __init__(
        self,
        *,
        tenant: Tenant,
        valid_id_types: frozenset[str],
        identity_config: IdentityIngestPipelineConfig,
        overrides: IdentityClusterOverrides,
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
        # Reviewer-recorded overrides that keep or drop specific clusters,
        # matched by a cluster's exact set of external ids.
        self.overrides = overrides

    def expand(
        self,
        input_or_inputs: dict[
            str,
            beam.PCollection[tuple[ExternalIdKey, ClusterKey]]
            | beam.PCollection[tuple[ExternalIdKey, SourcedIdentityFragment]],
        ],
    ) -> beam.pvalue.DoOutputsTuple:
        """Takes a dict with two keyed PCollections and produces one element
        per cluster, on one of two outputs: an IdentityCluster on KEPT_CLUSTERS,
        or a RejectedIdentityCluster on REJECTED_CLUSTERS. A cluster an EXCLUDE
        override drops produces no element.

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
          4. build_cluster checks the candidate's structural invariants
             (raising on violation), applies any recorded override, then either
             constructs an IdentityCluster from the resolved attributes or,
             when the fragments have attribute conflicts, a
             RejectedIdentityCluster.
          5. route_cluster sends each element to the output matching its type.
        """
        return (
            input_or_inputs
            | "CoGroup the PCollections by ExternalIdKey" >> beam.CoGroupByKey()
            | "Rekey fragments by cluster"
            >> beam.FlatMap(self.rekey_fragments_by_cluster)
            | "Group by cluster" >> beam.GroupByKey()
            | "Build IdentityCluster" >> beam.Map(self.build_cluster)
            | "Route kept and rejected clusters"
            # The declared output type describes the main (kept) output;
            # rejected clusters leave through the tagged output.
            >> beam.FlatMap(self.route_cluster)
            .with_output_types(IdentityCluster)
            .with_outputs(REJECTED_CLUSTERS, main=KEPT_CLUSTERS)
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

    def build_cluster(
        self, candidate: IdentityClusterCandidate
    ) -> IdentityCluster | RejectedIdentityCluster | _ExcludedIdentityCluster:
        """Builds one cluster from a candidate, first checking its structural
        invariants and raising on any violation.

        Returns an _ExcludedIdentityCluster when an EXCLUDE override drops
        the cluster, whether or not its fragments conflict. Returns a
        RejectedIdentityCluster when the fragments conflict and no recorded
        blessing applies, either because none exists or because the cluster's
        conflicts no longer hash to the value the blessing was recorded
        against. Otherwise returns an IdentityCluster built from the
        fragments' resolved attributes, with the reviewer's recorded value in
        place of each conflicting attribute when a BLESS override keeps the
        cluster."""
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

        override = self._override_for_cluster(cluster_key, person_type)

        # An EXCLUDE override drops the cluster whether or not it conflicts, so it
        # is applied before the conflict check.
        if (
            override is not None
            and override.disposition is IdentityClusterOverrideDisposition.EXCLUDE
        ):
            return self._exclude_cluster(cluster_key, person_type)

        conflicts = find_fragment_attribute_conflicts(
            sorted_fragments, tenant_config.conflict_checked_attributes_config
        )
        if conflicts:
            return self._resolve_conflicting_cluster(
                cluster_key=cluster_key,
                person_type=person_type,
                sorted_fragments=sorted_fragments,
                conflicts=conflicts,
                tenant_config=tenant_config,
                bless_override=override,
            )

        if override is not None:
            Metrics.counter(_OVERRIDE_METRICS_NAMESPACE, "obsolete_blessings").inc()
            logging.info(
                "Cluster [%s] has a BLESS override but no longer conflicts; the "
                "override had no effect and can be removed.",
                self._would_be_identity_cluster_id(cluster_key, person_type),
            )

        attributes = resolve_cluster_attributes(
            tenant=self.tenant,
            sorted_fragments=sorted_fragments,
            resolution_strategy_config=tenant_config.resolution_strategy_config,
        )
        return IdentityCluster(
            tenant=self.tenant,
            external_ids=self._cluster_external_id_entities(cluster_key),
            person_type=person_type,
            **(convert_attributes_to_cluster_kwargs(attributes) if attributes else {}),
        )

    @staticmethod
    def route_cluster(
        cluster: IdentityCluster | RejectedIdentityCluster | _ExcludedIdentityCluster,
    ) -> Iterator[IdentityCluster | beam.pvalue.TaggedOutput]:
        """Routes each built element to the output matching its type: kept
        IdentityClusters to the main KEPT_CLUSTERS output,
        RejectedIdentityClusters to REJECTED_CLUSTERS, and
        _ExcludedIdentityClusters to neither, so an excluded cluster leaves no
        trace in either table. Raises on any other type; the main output feeds
        the cluster tables the Identity Service imports, so an unrecognized
        element must fail the run rather than be imported."""
        # TODO(OBT-45433): Emit excluded clusters to a dedicated output table
        # instead of dropping them, so every cluster stays traceable to some
        # table.
        if isinstance(cluster, _ExcludedIdentityCluster):
            return
        if isinstance(cluster, RejectedIdentityCluster):
            yield beam.pvalue.TaggedOutput(REJECTED_CLUSTERS, cluster)
        elif isinstance(cluster, IdentityCluster):
            yield cluster
        else:
            raise ValueError(f"Unexpected built cluster type [{type(cluster)}]")

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

    def _override_for_cluster(
        self, cluster_key: ClusterKey, person_type: PersonType
    ) -> IdentityClusterOverride | None:
        """Returns the override matching the cluster's external ids, or None.
        Raises if a matching override records a different person type, which
        means the override was recorded against a different cluster."""
        override = self.overrides.get_override(cluster_key)
        if override is not None and override.person_type is not person_type:
            raise ValueError(
                f"Override for cluster [{cluster_key}] records person type "
                f"[{override.person_type.value}], but the cluster's person type "
                f"is [{person_type.value}]."
            )
        return override

    def _exclude_cluster(
        self, cluster_key: ClusterKey, person_type: PersonType
    ) -> _ExcludedIdentityCluster:
        """Drops the cluster per an EXCLUDE override, logging and counting the
        firing."""
        Metrics.counter(_OVERRIDE_METRICS_NAMESPACE, "excluded_clusters").inc()
        logging.info(
            "Excluding cluster [%s]; dropping it per a recorded override.",
            self._would_be_identity_cluster_id(cluster_key, person_type),
        )
        return _ExcludedIdentityCluster(external_ids=cluster_key)

    def _resolve_conflicting_cluster(
        self,
        *,
        cluster_key: ClusterKey,
        person_type: PersonType,
        sorted_fragments: list[IdentityFragment],
        conflicts: tuple[AttributeConflict, ...],
        tenant_config: IdentityIngestPipelineTenantConfig,
        bless_override: IdentityClusterOverride | None,
    ) -> IdentityCluster | RejectedIdentityCluster:
        """Returns an IdentityCluster when a BLESS override keeps the cluster,
        otherwise a RejectedIdentityCluster. Only a BLESS override reaches here;
        build_cluster applies EXCLUDE overrides before the conflict check. A
        BLESS override sets each conflicting attribute to the value the reviewer
        recorded for it, or to null when the reviewer recorded none, and applies
        only while the cluster's conflicts still hash to the value recorded at
        review time; a cluster whose conflict evidence has since changed is
        rejected for re-review rather than kept on a stale blessing."""
        if bless_override is not None:
            blessed_values = assert_type(
                bless_override.blessed_values, BlessedIdentityValues
            )
            if bless_override.conflicts_hash == hash_of_conflicts(conflicts):
                return self._build_blessed_cluster(
                    cluster_key=cluster_key,
                    person_type=person_type,
                    sorted_fragments=sorted_fragments,
                    resolution_strategy_config=tenant_config.resolution_strategy_config,
                    blessed_values=blessed_values,
                    conflicting_attributes={
                        ConflictCheckedAttribute(conflict.field)
                        for conflict in conflicts
                    },
                )
            Metrics.counter(_OVERRIDE_METRICS_NAMESPACE, "stale_blessings").inc()
            logging.info(
                "Cluster [%s] has a BLESS override recorded against different "
                "conflicts than it has now; rejecting it for re-review.",
                self._would_be_identity_cluster_id(cluster_key, person_type),
            )
        return self._build_rejected_cluster(
            cluster_key=cluster_key,
            person_type=person_type,
            sorted_fragments=sorted_fragments,
            conflicts=conflicts,
        )

    def _build_blessed_cluster(
        self,
        *,
        cluster_key: ClusterKey,
        person_type: PersonType,
        sorted_fragments: list[IdentityFragment],
        resolution_strategy_config: ResolutionStrategyConfig,
        blessed_values: BlessedIdentityValues,
        conflicting_attributes: set[ConflictCheckedAttribute],
    ) -> IdentityCluster:
        """Keeps a conflicting cluster per a BLESS override, using the recorded
        value for each conflicting attribute (null when the reviewer left it
        unset) and normal resolution for the rest."""
        Metrics.counter(_OVERRIDE_METRICS_NAMESPACE, "blessed_clusters").inc()
        logging.info(
            "Blessing cluster [%s], keeping it despite conflicts on %s.",
            self._would_be_identity_cluster_id(cluster_key, person_type),
            sorted(attribute.value for attribute in conflicting_attributes),
        )
        resolved_attributes = resolve_cluster_attributes(
            tenant=self.tenant,
            sorted_fragments=sorted_fragments,
            resolution_strategy_config=resolution_strategy_config,
        )
        attributes = build_blessed_cluster_attributes(
            tenant=self.tenant,
            resolved_attributes=resolved_attributes,
            blessed_values=blessed_values,
            conflicting_attributes=conflicting_attributes,
        )
        return IdentityCluster(
            tenant=self.tenant,
            external_ids=self._cluster_external_id_entities(cluster_key),
            person_type=person_type,
            **(convert_attributes_to_cluster_kwargs(attributes) if attributes else {}),
        )

    def _build_rejected_cluster(
        self,
        *,
        cluster_key: ClusterKey,
        person_type: PersonType,
        sorted_fragments: list[IdentityFragment],
        conflicts: tuple[AttributeConflict, ...],
    ) -> RejectedIdentityCluster:
        """Builds a RejectedIdentityCluster recording why this cluster was
        rejected. The fragment ids match rows in the debug
        {tenant}_identity_fragment.* tables, deduplicated because the upstream
        fan-out emits a fragment once per external ID it carries."""
        contributing_fragment_ids: list[str] = []
        for fragment in sorted_fragments:
            fragment_id = assert_type(fragment.identity_fragment_id, str)
            if fragment_id not in contributing_fragment_ids:
                contributing_fragment_ids.append(fragment_id)
        return RejectedIdentityCluster(
            tenant=self.tenant,
            person_type=person_type,
            external_ids=cluster_key,
            conflicts=conflicts,
            contributing_fragment_ids=tuple(contributing_fragment_ids),
        )

    def _cluster_external_id_entities(
        self, cluster_key: ClusterKey
    ) -> tuple[IdentityClusterExternalId, ...]:
        """Returns the IdentityClusterExternalId entities for the cluster's
        external ids."""
        return tuple(
            IdentityClusterExternalId(
                tenant=self.tenant, external_id=external_id, id_type=id_type
            )
            for external_id, id_type in cluster_key
        )

    def _would_be_identity_cluster_id(
        self, cluster_key: ClusterKey, person_type: PersonType
    ) -> str:
        """Returns the identity_cluster_id the cluster would carry if kept, used
        to reference the cluster in logs without its external ids."""
        return IdentityCluster.cluster_id_for_external_ids(
            tenant=self.tenant,
            external_ids=self._cluster_external_id_entities(cluster_key),
            person_type=person_type,
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


def _fragment_recency_sort_key(
    sourced_fragment: SourcedIdentityFragment,
) -> tuple[UpperBoundDate, IngestViewName, list[tuple[str, str]]]:
    """Orders fragments oldest-first by (upper-bound date, ingest view name),
    tie-broken by sorted external ids.

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
