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
"""Constants for the Identity Service."""
import os

import yaml

from recidiviz.common.constants.identity import ProductApp
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.service_accounts import (
    get_default_compute_engine_service_account_email,
)

IAP_BACKEND_SERVICE_ID_SECRET_NAME = (
    "iap_identity_service_load_balancer_service_id"  # nosec
)

# Single source of truth for the domain the Identity Service is served at in
# each GCP project, read both here and by cloud-run-identity-service.tf.
SERVICE_DOMAINS_YAML_PATH = os.path.join(
    os.path.dirname(__file__), "service_domains.yaml"
)


def get_identity_service_domain(project_id: str) -> str:
    """Returns the domain the Identity Service is served at in the given project."""
    with open(SERVICE_DOMAINS_YAML_PATH, encoding="utf-8") as f:
        domain_by_project = yaml.safe_load(f)
    if project_id not in domain_by_project:
        raise ValueError(
            f"No Identity Service domain configured for project [{project_id}] "
            f"in [{SERVICE_DOMAINS_YAML_PATH}]"
        )
    return domain_by_project[project_id]


# Route of the trigger_import endpoint, shared between the service's blueprint
# and the batch identity clustering DAG task that calls it.
TRIGGER_IMPORT_ROUTE = "/trigger_import"

DEV_CALLER_SERVICE_ACCOUNT = "fake-acct@fake-project.iam.gserviceaccount.com"

# Maps a caller's authenticated service account email (extracted from the
# `email` claim of the IAP JWT) to the `source_product_app` value it is
# allowed to write under. A `None` value means the caller is permitted to
# call the service but does not write PRODUCT_APP-sourced attributes (e.g.
# the batch identity clustering DAG, which writes EXTERNAL_DATA_SYSTEM
# records). Callers whose email is not in this mapping are rejected with 403.
PRODUCT_APP_BY_SERVICE_ACCOUNT: dict[str, ProductApp | None] = {
    DEV_CALLER_SERVICE_ACCOUNT: ProductApp.ADMIN_PANEL,
    # The staging Cloud Composer environment (and thus the batch identity
    # clustering DAG) runs as the project's default Compute Engine service
    # account.
    get_default_compute_engine_service_account_email(GCP_PROJECT_STAGING): None,
}
