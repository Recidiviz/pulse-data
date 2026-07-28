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
"""Tests the packaging wiring that lets recidiviz/pipelines/dataflow_flex_setup.py
read its install_requires out of dataflow_flex_requirements.txt."""
import os
import unittest

import recidiviz.pipelines

PIPELINES_DIR = os.path.dirname(recidiviz.pipelines.__file__)

REQUIREMENTS_FILE_NAME = "dataflow_flex_requirements.txt"

MANIFEST_IN_FILE_NAME = "dataflow_flex_MANIFEST.in"

DOCKERFILE_PATH = os.path.join(PIPELINES_DIR, "Dockerfile.pipelines")

MANIFEST_IN_PATH = os.path.join(PIPELINES_DIR, MANIFEST_IN_FILE_NAME)


class TestDataflowFlexSetupPackaging(unittest.TestCase):
    """Guards the invariant that dataflow_flex_requirements.txt sits next to
    dataflow_flex_setup.py everywhere that file is executed.

    dataflow_flex_setup.py reads the requirements file as a sibling of itself, and it
    runs in three places: this repo, the flex template image (where Dockerfile.pipelines
    copies it in as setup.py), and the sdist Beam builds from it and stages to Dataflow
    workers (where MANIFEST.in is what gets the requirements file included). A break in
    either of the latter two would only surface as a failed pipeline at worker boot, so
    assert the wiring here instead.
    """

    def test_requirements_file_is_sibling_of_setup_file(self) -> None:
        self.assertTrue(
            os.path.exists(os.path.join(PIPELINES_DIR, "dataflow_flex_setup.py"))
        )
        self.assertTrue(
            os.path.exists(os.path.join(PIPELINES_DIR, REQUIREMENTS_FILE_NAME))
        )

    def test_dockerfile_copies_requirements_next_to_setup_file(self) -> None:
        with open(DOCKERFILE_PATH, "r", encoding="utf-8") as dockerfile:
            contents = dockerfile.read()

        self.assertIn(
            f"COPY ./recidiviz/pipelines/{REQUIREMENTS_FILE_NAME} "
            f"${{WORKDIR}}/{REQUIREMENTS_FILE_NAME}",
            contents,
            f"[{DOCKERFILE_PATH}] must copy [{REQUIREMENTS_FILE_NAME}] alongside "
            f"setup.py so that `pip install` can read install_requires from it.",
        )

    def test_dockerfile_copies_manifest_to_sdist_root(self) -> None:
        with open(DOCKERFILE_PATH, "r", encoding="utf-8") as dockerfile:
            contents = dockerfile.read()

        self.assertIn(
            f"COPY ./recidiviz/pipelines/{MANIFEST_IN_FILE_NAME} "
            f"${{WORKDIR}}/MANIFEST.in",
            contents,
            f"[{DOCKERFILE_PATH}] must copy [{MANIFEST_IN_FILE_NAME}] to "
            f"${{WORKDIR}}/MANIFEST.in -- setuptools only reads a MANIFEST.in from "
            f"the directory it builds the sdist in.",
        )

    def test_manifest_in_includes_requirements_file(self) -> None:
        with open(MANIFEST_IN_PATH, "r", encoding="utf-8") as manifest_file:
            directives = [
                line.strip()
                for line in manifest_file
                if line.strip() and not line.strip().startswith("#")
            ]

        self.assertIn(
            f"include {REQUIREMENTS_FILE_NAME}",
            directives,
            f"[{MANIFEST_IN_PATH}] must include [{REQUIREMENTS_FILE_NAME}] so that it "
            f"lands in the sdist Beam stages to Dataflow workers.",
        )
