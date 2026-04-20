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
"""Tests for generate_recidiviz_data_post_commit_docker_build_steps"""
import unittest

import yaml

from recidiviz.tools.deploy.cloud_build.generate_recidiviz_data_post_commit_docker_build_steps import (
    OUTPUT_PATH,
    generate,
)


class GenerateRecidivizDataPostCommitDockerBuildStepsTest(unittest.TestCase):
    def test_generated_yaml_is_up_to_date(self) -> None:
        expected = generate()
        with open(OUTPUT_PATH, encoding="utf-8") as f:
            actual = yaml.safe_load(f)
        self.assertEqual(
            expected,
            actual,
            "The checked-in YAML is out of date. Run:\n"
            "  python -m recidiviz.tools.deploy.cloud_build"
            ".generate_recidiviz_data_post_commit_docker_build_steps\n"
            "and commit the updated file.",
        )

    def test_every_step_has_name_and_id(self) -> None:
        config = generate()
        for step in config["steps"]:
            self.assertIn("name", step, f"Step missing 'name': {step}")
            self.assertIn("id", step, f"Step missing 'id': {step}")
