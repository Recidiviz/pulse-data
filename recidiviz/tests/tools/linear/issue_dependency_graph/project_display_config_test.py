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
"""Tests for ProjectDisplayConfig."""
import unittest

from recidiviz.tests.ingest import fixtures
from recidiviz.tools.linear.generate_issue_dependency_graph_html import (
    available_config_names,
)
from recidiviz.tools.linear.issue_dependency_graph.project_display_config import (
    MilestoneDisplayConfig,
    ProjectDisplayConfig,
    config_path_for_name,
)


class TestProjectDisplayConfig(unittest.TestCase):
    """Tests for parsing a project display config out of YAML."""

    def test_parse_valid_config(self) -> None:
        config = ProjectDisplayConfig.from_yaml(
            fixtures.as_filepath("valid_config.yaml")
        )

        self.assertEqual(
            ProjectDisplayConfig(
                project_name="Test Project",
                milestones=[
                    MilestoneDisplayConfig(name="Phase 1", short_name="P1"),
                    MilestoneDisplayConfig(name="Phase 2", short_name="P2"),
                ],
                exclude_parent_issues=True,
                labels_excluded_by_default=["Region: US_XX"],
            ),
            config,
        )

    def test_parse_config_with_no_excluded_labels(self) -> None:
        config = ProjectDisplayConfig.from_yaml(
            fixtures.as_filepath("config_no_excluded_labels.yaml")
        )

        self.assertEqual(
            ProjectDisplayConfig(
                project_name="Test Project",
                milestones=[MilestoneDisplayConfig(name="Phase 1", short_name="P1")],
                exclude_parent_issues=False,
                labels_excluded_by_default=[],
            ),
            config,
        )

    def test_parse_missing_short_name(self) -> None:
        with self.assertRaisesRegex(KeyError, r"short_name"):
            ProjectDisplayConfig.from_yaml(
                fixtures.as_filepath("config_missing_short_name.yaml")
            )

    def test_parse_missing_exclude_parent_issues(self) -> None:
        with self.assertRaisesRegex(KeyError, r"exclude_parent_issues"):
            ProjectDisplayConfig.from_yaml(
                fixtures.as_filepath("config_missing_exclude_parent_issues.yaml")
            )

    def test_parse_empty_milestones(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Field \[milestones\] on \[ProjectDisplayConfig\] must be a non-empty "
            r"list\. Found value \[\[\]\]$",
        ):
            ProjectDisplayConfig.from_yaml(
                fixtures.as_filepath("config_empty_milestones.yaml")
            )

    def test_parse_duplicate_milestone_short_names(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Project display config for \[Test Project\] lists duplicate milestone "
            r"short names: \['P1'\]\.$",
        ):
            ProjectDisplayConfig.from_yaml(
                fixtures.as_filepath("config_duplicate_short_names.yaml")
            )

    def test_parse_unexpected_key(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Found unexpected config values in .*labels_to_exclude.*$"
        ):
            ProjectDisplayConfig.from_yaml(
                fixtures.as_filepath("config_unexpected_key.yaml")
            )

    def test_load_all_checked_in_configs(self) -> None:
        config_names = available_config_names()
        self.assertTrue(config_names, "Expected at least one checked-in config.")
        for config_name in config_names:
            with self.subTest(config_name=config_name):
                # Raises if the checked-in config fails to parse or validate.
                ProjectDisplayConfig.from_yaml(config_path_for_name(config_name))
