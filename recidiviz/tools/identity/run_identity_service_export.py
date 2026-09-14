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
"""Runs the Identity Service export from a local machine, launching the Cloud
SQL Proxy for the identity database so no manual proxy or secret setup is
needed.

Usage:
    uv run python -m recidiviz.tools.identity.run_identity_service_export \\
        --project-id recidiviz-staging [--tenant US_OZ]
"""
import argparse

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.tenants import Tenant
from recidiviz.entrypoints.identity.identity_service_export import (
    export_identity_service_to_bigquery,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def parse_arguments() -> argparse.Namespace:
    """Parses arguments for the local Identity Service export run."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-id",
        help="The project whose Identity Service database and export dataset to use.",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--tenant",
        help=(
            "When set, refresh only this tenant's rows in the export dataset. "
            "When omitted, rewrite the full dataset."
        ),
        type=Tenant,
        choices=list(Tenant),
        metavar="TENANT",
    )
    return parser.parse_args()


def main(*, project_id: str, tenant: Tenant | None) -> None:
    """Runs the export against the given project through a local Cloud SQL
    Proxy."""
    if project_id == GCP_PROJECT_PRODUCTION:
        prompt_for_confirmation(
            f"This will rewrite {'the ' + tenant.value + ' rows in ' if tenant else ''}"
            f"the identity_service_export dataset in [{project_id}]. Proceed?"
        )
    with local_project_id_override(project_id):
        with cloudsql_proxy_control.connection(schema_type=SchemaType.IDENTITY):
            export_identity_service_to_bigquery(
                bq_client=BigQueryClientImpl(),
                tenant=tenant,
                use_proxy_session=True,
            )


if __name__ == "__main__":
    args = parse_arguments()
    main(project_id=args.project_id, tenant=args.tenant)
