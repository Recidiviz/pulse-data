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
"""Base BigQuery dataset names for the identity ingest pipeline's per-tenant
output datasets.

These names are the contract between the identity ingest pipeline, which writes
the datasets, and the Identity Service, which reads them. Callers should reach
them through the sandbox-aware wrappers in dataset_config rather than calling
these directly.
"""


def identity_ingest_view_results_dataset_name(tenant: str) -> str:
    """Returns the base dataset name for the tenant's identity ingest view results."""
    return f"{tenant.lower()}_identity_ingest_view_results"


def identity_fragment_dataset_name(tenant: str) -> str:
    """Returns the base dataset name for the tenant's pre-clustering fragment output."""
    return f"{tenant.lower()}_identity_fragment"


def identity_cluster_dataset_name(tenant: str) -> str:
    """Returns the base dataset name for the tenant's clustering results."""
    return f"{tenant.lower()}_identity_cluster"


def identity_rejections_dataset_name(tenant: str) -> str:
    """Returns the base dataset name for the tenant's rejection tables."""
    return f"{tenant.lower()}_identity_rejections"


def identity_overrides_dataset_name(tenant: str) -> str:
    """Returns the base dataset name for the tenant's identity cluster overrides."""
    return f"{tenant.lower()}_identity_overrides"
