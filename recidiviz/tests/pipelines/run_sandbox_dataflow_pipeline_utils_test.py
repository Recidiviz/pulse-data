# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for run_sandbox_calculation_pipeline.py"""

import os
import unittest

from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    get_cloudbuild_path,
    get_template_path,
)


class FlexPipelineSandboxPathsTest(unittest.TestCase):
    """Tests to verify paths used in the sandbox script exist."""

    def test_template_paths_exist_for_all_pipelines(self) -> None:
        for pipeline_type in ["metrics", "supplemental"]:
            template_path = get_template_path(pipeline_type)
            self.assertEqual(os.path.isfile(template_path), True)

    def test_cloudbuild_files_exist(self) -> None:
        cloudbuild_path = get_cloudbuild_path()
        self.assertEqual(os.path.isfile(cloudbuild_path), True)
