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
"""Checks whether the identity service export for a particular tenant has
usable rows."""
from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.source_tables.identity_service_export_names import (
    LEGACY_PERSON_ID_COLUMN,
    LEGACY_STAFF_ID_COLUMN,
)
from recidiviz.utils.types import assert_type


def check_identity_service_export_has_usable_rows(
    *, project_id: str, dataset_id: str, tenant: Tenant
) -> None:
    """Checks whether the identities table for the given tenant in the given
    export dataset has a populated legacy_person_id or legacy_staff_id.
    Raises a ValueError if not."""
    identities_address = ProjectSpecificBigQueryAddress(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=assert_type(schema.Identity.__tablename__, str),
    )
    tenant_column = assert_type(schema.Identity.tenant.name, str)

    client = BigQueryClientImpl(project_id=project_id)
    query = f"""
SELECT 1
FROM {identities_address.format_address_for_query()}
WHERE {tenant_column} = '{tenant.value}'
    AND ({LEGACY_PERSON_ID_COLUMN} IS NOT NULL OR {LEGACY_STAFF_ID_COLUMN} IS NOT NULL)
LIMIT 1
"""
    query_job = client.run_query_async(query_str=query, use_query_cache=False)
    if not list(query_job):
        raise ValueError(
            f"The activity pipeline for tenant [{tenant.value}] is set to use "
            f"the Identity Service export, but [{identities_address.to_str()}] has no "
            f"rows for that tenant with a populated {LEGACY_PERSON_ID_COLUMN} "
            f"or {LEGACY_STAFF_ID_COLUMN}. Populate the export first by running "
            f"`python -m recidiviz.tools.identity.run_identity_service_export "
            f"--project-id {project_id} --tenant {tenant.value}`, or for a "
            f"sandbox run, point the pipeline at a populated copy with "
            f'`--input_dataset_overrides_json \'{{"{dataset_id}": '
            f'"<populated copy>"}}\'`.'
        )
