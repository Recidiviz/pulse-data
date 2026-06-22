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
"""Tests for refresh_scratch_views."""

import os
import unittest

import recidiviz.research.notebooks.policy as policy_module
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.utils.metadata import local_project_id_override


def get_all_scratch_view_dirs() -> list[tuple[str, str]]:
    """Returns (state_code, policy_project) pairs for all scratch view directories."""
    policy_dir = os.path.dirname(policy_module.__file__)
    pairs = []
    for state in os.listdir(policy_dir):
        state_path = os.path.join(policy_dir, state)
        if not os.path.isdir(state_path):
            continue
        for project in os.listdir(state_path):
            if os.path.isdir(os.path.join(state_path, project, "views")):
                pairs.append((state, project))
    return pairs


class ScratchViewsTest(unittest.TestCase):
    def test_all_views_build(self) -> None:
        view_dirs = get_all_scratch_view_dirs()
        self.assertGreater(len(view_dirs), 0, "Expected at least one scratch view dir")
        for state_code, policy_project in view_dirs:
            builders = BigQueryViewCollector.collect_view_builders_in_module(
                builder_type=SimpleBigQueryViewBuilder,
                view_dir_module=ModuleCollectorMixin.get_relative_module(
                    policy_module, [state_code, policy_project, "views"]
                ),
                recurse=True,
            )
            self.assertGreater(
                len(builders),
                0,
                f"Expected at least one view in {state_code}/{policy_project}",
            )
            for builder in builders:
                with self.subTest(view_id=builder.view_id):
                    with local_project_id_override("recidiviz-staging"):
                        view = builder.build()
                    self.assertIn("SELECT", view.view_query)
