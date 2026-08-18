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
"""Reads a tenant's identity_cluster_override table into an in-memory
IdentityClusterOverrides.

The identity pipeline calls this once when it builds its graph, so every cluster
consults the same loaded overrides. The table is small (one row per reviewed
cluster), so the whole table loads into memory rather than joining as a Beam
input.
"""
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.tenants import Tenant
from recidiviz.pipelines.ingest.identity.identity_cluster_override import (
    IDENTITY_CLUSTER_OVERRIDE_TABLE_ID,
    IdentityClusterOverride,
    IdentityClusterOverrides,
)


def read_identity_cluster_overrides(
    *, project_id: str, dataset_id: str, tenant: Tenant
) -> IdentityClusterOverrides:
    """Reads the tenant's recorded overrides from its identity_cluster_override
    table, returning empty overrides when the table has no rows."""
    client = BigQueryClientImpl(project_id=project_id)
    query = (
        f"SELECT * FROM `{project_id}.{dataset_id}."
        f"{IDENTITY_CLUSTER_OVERRIDE_TABLE_ID}`"
    )
    query_job = client.run_query_async(query_str=query, use_query_cache=False)
    overrides = [IdentityClusterOverride.from_bq_row(dict(row)) for row in query_job]
    return IdentityClusterOverrides.for_overrides(tenant=tenant, overrides=overrides)
