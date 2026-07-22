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
"""PTransform that validates cross-cluster invariants—invariants that
span the whole set of clusters and so cannot be checked while building an
individual cluster—on IdentityClusters before they are written to BigQuery.
Per-cluster structural checks happen earlier, in BuildIdentityClusters.

These invariants should always hold when the upstream PTransforms are wired
correctly; a violation indicates a pipeline bug (e.g. CoGroupByKey leakage),
not a data-quality problem, so any violation fails the pipeline loudly.

This is the identity pipeline's counterpart to the activity pipeline's
RunValidations, scoped to IdentityCluster.
"""
from collections.abc import Iterable

import apache_beam as beam

from recidiviz.persistence.entity.identity.identity_cluster_entities import (
    IdentityCluster,
)
from recidiviz.pipelines.ingest.types import ExternalIdKey


class ValidateIdentityClusterCollection(beam.PTransform):
    """PTransform that validates cross-cluster invariants on IdentityClusters,
    raising on any violation or emitting the clusters unchanged."""

    def expand(
        self, input_or_inputs: beam.PCollection[IdentityCluster]
    ) -> beam.PCollection[IdentityCluster]:
        """Validates the cross-cluster invariants, raising on any violation or
        emitting the clusters unchanged."""
        # This branch raises directly rather than blocking the output below:
        # Count.Globally emits a single 0 even for an empty input, so this is
        # the one check that still fires when the pipeline produces no clusters
        # (per-element transforms never run on an empty PCollection).
        _ = (
            input_or_inputs
            | "Count IdentityClusters" >> beam.combiners.Count.Globally()
            | "Validate at least one IdentityCluster"
            >> beam.Map(self._validate_cluster_count)
        )

        duplicate_eid_errors = (
            input_or_inputs
            | "Key cluster ids by external id" >> beam.FlatMap(self._cluster_id_by_eid)
            | "Group cluster ids by external id" >> beam.GroupByKey()
            | "Find external ids in multiple clusters"
            >> beam.FlatMap(self._duplicate_eid_error)
            | "Collect duplicate external id errors" >> beam.combiners.ToList()
        )

        # Consuming the duplicate-external-id errors as a side input forces
        # every cluster to wait for the full cross-cluster check before it can
        # be emitted, so nothing reaches the write step if any external ID
        # appears in more than one cluster.
        return input_or_inputs | "Block output on cross-cluster checks" >> beam.Map(
            self._raise_or_return_cluster,
            duplicate_eid_errors=beam.pvalue.AsSingleton(duplicate_eid_errors),
        )

    @staticmethod
    def _validate_cluster_count(cluster_count: int) -> int:
        """Raises if the pipeline produced no IdentityClusters; a tenant that
        runs the identity pipeline is expected to produce at least one. Returns
        the count otherwise."""
        if not cluster_count:
            raise ValueError(
                "Expected the pipeline to produce at least one IdentityCluster, "
                "but none were produced."
            )
        return cluster_count

    @staticmethod
    def _cluster_id_by_eid(
        cluster: IdentityCluster,
    ) -> list[tuple[ExternalIdKey, str]]:
        """Returns one (external ID key, identity_cluster_id) pair for each of
        the cluster's external IDs."""
        return [
            ((e.external_id, e.id_type), cluster.identity_cluster_id)
            for e in cluster.external_ids
        ]

    @staticmethod
    def _duplicate_eid_error(
        eid_with_cluster_ids: tuple[ExternalIdKey, Iterable[str]]
    ) -> list[str]:
        """Returns an error if the external ID appears in more than one
        cluster. Clustering assigns each external ID to exactly one cluster, so
        a duplicate means an upstream wiring bug duplicated or split a
        cluster."""
        eid, cluster_ids_iterable = eid_with_cluster_ids
        cluster_ids = sorted(cluster_ids_iterable)
        if len(cluster_ids) > 1:
            return [
                f"External id [{eid}] appears in [{len(cluster_ids)}] clusters: "
                f"{ValidateIdentityClusterCollection._format_values(cluster_ids)}."
            ]
        return []

    @staticmethod
    def _raise_or_return_cluster(
        cluster: IdentityCluster, duplicate_eid_errors: list[str]
    ) -> IdentityCluster:
        """Raises if any external ID appears in more than one cluster;
        otherwise returns the cluster unchanged. Applied per cluster so that no
        cluster is emitted to the write step until the cross-cluster check
        passes."""
        if duplicate_eid_errors:
            errors_str = "\n  * ".join(sorted(duplicate_eid_errors))
            raise ValueError(
                f"Found external ids that appear in more than one "
                f"IdentityCluster:\n  * {errors_str}"
            )
        return cluster

    @staticmethod
    def _format_values(values: Iterable[object]) -> str:
        """Formats the given values for an error message as a bracketed,
        comma-separated list, e.g. [T1, T2]."""
        return f"[{', '.join(str(v) for v in values)}]"
