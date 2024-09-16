# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Deployment related constants"""

from recidiviz.tools.deploy.cloud_build.artifact_registry_repository import ImageKind

TERRAFORM_VERSION = "1.7.0"
TERRAFORM_WORKDIR = "/workspace/recidiviz/tools/deploy/terraform/"

# Builder images
BUILDER_GCLOUD = "gcr.io/google.com/cloudsdktool/cloud-sdk:slim"
BUILDER_DOCKER = "gcr.io/cloud-builders/docker"
BUILDER_ALPINE = "alpine"
BUILDER_TERRAFORM = f"hashicorp/terraform:{TERRAFORM_VERSION}"
BUILDER_GIT = "gcr.io/cloud-builders/git"

# Binaries
CLOUD_SQL_PROXY_URI = "https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.11.4/cloud-sql-proxy.linux.amd64"

DOCKER_CREDENTIAL_HELPER = (
    "https://github.com/GoogleCloudPlatform/docker-credential-gcr"
    "/releases/download/v2.1.22/docker-credential-gcr_linux_amd64-2.1.22.tar.gz"
)


# Map of ImageKind to docker files and Docker build stage targets
IMAGE_DOCKERFILES: dict[ImageKind, tuple[str, str | None]] = {
    ImageKind.APP_ENGINE: ("Dockerfile", "recidiviz-app"),
    ImageKind.RECIDIVIZ_BASE: ("Dockerfile.recidiviz-base", None),
    ImageKind.ASSET_GENERATION: ("Dockerfile.asset-generation", None),
    ImageKind.CASE_TRIAGE_PATHWAYS: ("Dockerfile.case-triage-pathways", None),
    ImageKind.DATAFLOW: ("recidiviz/pipelines/Dockerfile.pipelines", None),
    ImageKind.DATAFLOW_DEV: ("recidiviz/pipelines/Dockerfile.pipelines", None),
}
