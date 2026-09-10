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
"""Helpers for getting identity ingest pipeline output datasets."""

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.pipelines.ingest.identity.dataset_names import (
    identity_cluster_dataset_name,
    identity_fragment_dataset_name,
    identity_ingest_view_results_dataset_name,
    identity_overrides_dataset_name,
    identity_rejections_dataset_name,
)


def identity_ingest_view_results_dataset_for_tenant(
    tenant: str, sandbox_dataset_prefix: str | None = None
) -> str:
    """Returns the BigQuery dataset where the identity ingest pipeline writes
    the given tenant's ingest view results.
    """
    return _apply_sandbox_prefix(
        identity_ingest_view_results_dataset_name(tenant), sandbox_dataset_prefix
    )


def identity_fragment_dataset_for_tenant(
    tenant: str, sandbox_dataset_prefix: str | None = None
) -> str:
    """Returns the BigQuery dataset where the identity ingest pipeline writes
    the given tenant's pre-clustering IdentityFragment output.
    """
    return _apply_sandbox_prefix(
        identity_fragment_dataset_name(tenant), sandbox_dataset_prefix
    )


def identity_cluster_dataset_for_tenant(
    tenant: str, sandbox_dataset_prefix: str | None = None
) -> str:
    """Returns the BigQuery dataset where the identity ingest pipeline writes
    the given tenant's clustering results.
    """
    return _apply_sandbox_prefix(
        identity_cluster_dataset_name(tenant), sandbox_dataset_prefix
    )


def identity_rejections_dataset_for_tenant(
    tenant: str, sandbox_dataset_prefix: str | None = None
) -> str:
    """Returns the BigQuery dataset where the identity ingest pipeline writes
    the given tenant's rejection tables, the pipeline-written record of any
    fragments or clusters it dropped and why, for human review.
    """
    return _apply_sandbox_prefix(
        identity_rejections_dataset_name(tenant), sandbox_dataset_prefix
    )


def identity_overrides_dataset_for_tenant(
    tenant: str, sandbox_dataset_prefix: str | None = None
) -> str:
    """Returns the BigQuery dataset holding the given tenant's identity cluster
    overrides, the reviewer-recorded decisions to keep or drop specific clusters
    that the identity ingest pipeline reads.
    """
    return _apply_sandbox_prefix(
        identity_overrides_dataset_name(tenant), sandbox_dataset_prefix
    )


def _apply_sandbox_prefix(base_dataset: str, sandbox_dataset_prefix: str | None) -> str:
    if not sandbox_dataset_prefix:
        return base_dataset
    return BigQueryAddressOverrides.format_sandbox_dataset(
        sandbox_dataset_prefix, base_dataset
    )
