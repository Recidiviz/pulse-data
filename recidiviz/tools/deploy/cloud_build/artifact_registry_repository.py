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
"""Data objects for Artifact Registry repositories"""
import enum
import os
from functools import cached_property

import attr

import recidiviz
from recidiviz.utils.metadata import project_id
from recidiviz.utils.yaml_dict import YAMLDict

ARTIFACT_REGISTRY_CONFIG_PATH = os.path.join(
    os.path.dirname(recidiviz.__file__),
    "tools/deploy/terraform/config/artifact_registry_repositories.yaml",
)


class ImageKind(enum.StrEnum):
    APP_ENGINE = "appengine"
    CASE_TRIAGE_PATHWAYS = "case-triage-pathways"
    ASSET_GENERATION = "asset-generation"
    RECIDIVIZ_BASE = "recidiviz-base"
    DATAFLOW = "dataflow"
    DATAFLOW_DEV = "dataflow-dev"


@attr.define
class ArtifactRegistryDockerImageRepository:
    """Data object representing an Artifact Registry repository that contains our Docker images.
    For more info, see: https://cloud.google.com/artifact-registry/docs/repositories
    """

    # The following fields correspond to our Terraform configuration.
    # See artifact_registry_repositories.yaml and artifact-registries.tf
    # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/artifact_registry_repository
    project_id: str
    location: str
    repository_id: str
    description: str
    format: str
    cleanup_policy_dry_run: bool

    @cached_property
    def image_kind(self) -> ImageKind:
        return ImageKind(self.repository_id)

    @cached_property
    def host_name(self) -> str:
        return f"{self.location}-docker.pkg.dev"

    @cached_property
    def _base_url(self) -> str:
        return f"{self.host_name}/{self.project_id}/{self.repository_id}"

    def build_url(self, commit_ref: str) -> str:
        return f"{self._base_url}/build/{commit_ref}"

    def version_url(self, version_tag: str) -> str:
        return f"{self._base_url}/default:{version_tag}"

    def latest_url(self) -> str:
        return f"{self._base_url}/default:latest"

    @classmethod
    def from_file(cls) -> dict[ImageKind, "ArtifactRegistryDockerImageRepository"]:
        yaml_dict = YAMLDict.from_path(yaml_path=ARTIFACT_REGISTRY_CONFIG_PATH)

        return {
            ImageKind(
                repository_config["repository_id"]
            ): ArtifactRegistryDockerImageRepository(
                project_id=project_id(), **repository_config
            )
            for repository_config in yaml_dict.pop("repositories", dict).values()
            if repository_config["format"] == "DOCKER"
        }
