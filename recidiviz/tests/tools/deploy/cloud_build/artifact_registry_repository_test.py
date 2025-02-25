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
"""Tests for ArtifactRegistryRepository"""
import os
import unittest

import recidiviz
from recidiviz.tools.deploy.cloud_build.artifact_registry_repository import (
    ArtifactRegistryDockerImageRepository,
    ImageKind,
)
from recidiviz.tools.deploy.cloud_build.constants import IMAGE_DOCKERFILES
from recidiviz.utils.metadata import local_project_id_override


class ArtifactRegistryDockerImageRepositoryTest(unittest.TestCase):
    """Test case for ArtifactRegistryRepository"""

    def test_parse(self) -> None:
        with local_project_id_override("test-project"):
            ArtifactRegistryDockerImageRepository.from_file()

    def test_all_images_have_registered_dockerfile(self) -> None:
        self.assertSetEqual(
            set(image for image in ImageKind), set(IMAGE_DOCKERFILES.keys())
        )

        for image_kind, dockerfile_stage in IMAGE_DOCKERFILES.items():
            dockerfile, _ = dockerfile_stage
            if not os.path.exists(
                os.path.join(os.path.dirname(recidiviz.__file__), "../", dockerfile)
            ):
                raise ValueError(
                    f"Expected to find Dockerfile for {image_kind} at repository root with relative path {dockerfile}"
                    ", but could not find it."
                )
