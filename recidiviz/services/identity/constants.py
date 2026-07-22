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


IDENTITY_SERVICE_API_VERSION = "v1"

# Blueprint-relative routes, kept version-free: the API version is applied as a
# url_prefix when a blueprint is registered on the app, so each version is its
# own registration and versions can coexist, rather than every route naming its
# version and a version bump renaming them all.
IDENTITIES_BLUEPRINT_ROUTE = "/identities"
TRIGGER_IMPORT_BLUEPRINT_ROUTE = "/trigger_import"

# Full client-facing paths, for callers outside the service (e.g. the batch
# identity clustering DAG task that POSTs to trigger_import).
IDENTITIES_ROUTE = f"/{IDENTITY_SERVICE_API_VERSION}{IDENTITIES_BLUEPRINT_ROUTE}"
TRIGGER_IMPORT_ROUTE = (
    f"/{IDENTITY_SERVICE_API_VERSION}{TRIGGER_IMPORT_BLUEPRINT_ROUTE}"
)

# Dev-only callers, one per role, for exercising each role locally (e.g. with
# curl). A local request selects one via DEV_CALLER_EMAIL_HEADER.
DEV_READER_SERVICE_ACCOUNT = "fake-reader@fake-project.iam.gserviceaccount.com"
DEV_EDITOR_SERVICE_ACCOUNT = "fake-editor@fake-project.iam.gserviceaccount.com"
DEV_IMPORTER_SERVICE_ACCOUNT = "fake-importer@fake-project.iam.gserviceaccount.com"

# Header a local request may set to be treated as a specific caller. Honored
# only in development, where the IAP JWT check is bypassed; in GCP the caller
# email always comes from the validated JWT and this header is ignored.
DEV_CALLER_EMAIL_HEADER = "X-Dev-Caller-Email"
