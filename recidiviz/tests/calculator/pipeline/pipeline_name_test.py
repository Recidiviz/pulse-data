# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests the pipeline names."""
import unittest

from recidiviz.tools.pipeline_launch_util import collect_all_pipeline_names


class TestPipelineNames(unittest.TestCase):
    """Tests the names of all pipelines that can be run."""

    def test_collect_all_pipeline_names(self) -> None:
        """Tests that each pipeline run delegate has a config with a unique
        pipeline_name."""
        pipeline_names = collect_all_pipeline_names()

        self.assertCountEqual(set(pipeline_names), pipeline_names)
