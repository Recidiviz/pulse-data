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


def identity_cluster_dataset_for_tenant(
    tenant: str, sandbox_dataset_prefix: str | None = None
) -> str:
    """Returns the BigQuery dataset where the identity ingest pipeline writes
    the given tenant's clustering results.
    """
    base_dataset = f"{tenant.lower()}_identity_cluster"
    if not sandbox_dataset_prefix:
        return base_dataset
    return BigQueryAddressOverrides.format_sandbox_dataset(
        sandbox_dataset_prefix, base_dataset
    )
