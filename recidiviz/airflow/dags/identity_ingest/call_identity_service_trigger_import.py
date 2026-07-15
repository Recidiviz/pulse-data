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
"""Airflow task that notifies the Identity Service that a tenant's identity
clustering results have been written to BigQuery, by making an IAP-authenticated
POST /trigger_import request.

The request is fire-and-forget: the Identity Service responds 202 as soon as it
has accepted the import request, and we do not wait for import processing to
complete. Any non-202 response fails the task.
"""
import logging
from http import HTTPStatus

import requests
from airflow.decorators import task
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token

from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.services.identity.constants import (
    TRIGGER_IMPORT_ROUTE,
    get_identity_service_domain,
)
from recidiviz.utils.secrets import get_secret

# OAuth client id used by the IAP protecting the Identity Service load
# balancer. An OIDC token minted with this client id as the audience
# authenticates the caller to IAP.
IAP_CLIENT_ID_SECRET_NAME = "iap_client_id"  # nosec B105

CALL_IDENTITY_SERVICE_TRIGGER_IMPORT_TASK_ID = "call_identity_service_trigger_import"

_TRIGGER_IMPORT_REQUEST_TIMEOUT_SECONDS = 60


def call_identity_service_trigger_import(tenant: str) -> None:
    """Makes an IAP-authenticated POST /trigger_import request to the Identity
    Service for the given tenant. Treats a 202 response as success and raises
    for any other response, failing the Airflow task.
    """
    project_id = get_project_id()
    url = f"https://{get_identity_service_domain(project_id)}{TRIGGER_IMPORT_ROUTE}"

    iap_client_id = get_secret(IAP_CLIENT_ID_SECRET_NAME, project_id)
    if iap_client_id is None:
        raise ValueError(
            f"Could not find secret [{IAP_CLIENT_ID_SECRET_NAME}] in project "
            f"[{project_id}]"
        )
    id_token = fetch_id_token(request=Request(), audience=iap_client_id)

    response = requests.post(
        url,
        json={"tenant": tenant},
        headers={"Authorization": f"Bearer {id_token}"},
        timeout=_TRIGGER_IMPORT_REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code != HTTPStatus.ACCEPTED:
        raise ValueError(
            f"Identity Service trigger_import request for tenant [{tenant}] failed with "
            f"status [{response.status_code}]: [{response.text}]"
        )
    logging.info(
        "Identity Service accepted the trigger_import request for tenant [%s].", tenant
    )


@task(
    task_id=CALL_IDENTITY_SERVICE_TRIGGER_IMPORT_TASK_ID,
    # Unlike most tasks in this DAG, the trigger_import request is cheap and
    # its failures are often transient (network blips, IAP token issues), so
    # retrying is worthwhile.
    retries=2,
)
def call_identity_service_trigger_import_task(tenant: str) -> None:
    call_identity_service_trigger_import(tenant=tenant)
