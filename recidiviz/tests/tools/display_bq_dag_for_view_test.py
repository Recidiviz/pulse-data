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
"""Tests for functionality used by display_bq_dag_for_view.py."""
import unittest

from recidiviz.tools.display_bq_dag_for_view import print_dfs_tree
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


class TestDisplayBQDAGForView(unittest.TestCase):
    """Tests for functionality used by display_bq_dag_for_view.py."""

    def test_print_dfs_tree_downstream(self) -> None:
        # Should run without crashing
        with local_project_id_override(GCP_PROJECT_STAGING):
            print_dfs_tree("sessions", "dataflow_sessions", print_downstream_tree=True)

    def test_print_dfs_tree_upstream(self) -> None:
        # Should run without crashing
        with local_project_id_override(GCP_PROJECT_STAGING):
            print_dfs_tree("sessions", "dataflow_sessions", print_downstream_tree=False)

    def test_print_dfs_tree_invalid_view(self) -> None:
        with local_project_id_override(GCP_PROJECT_STAGING):
            with self.assertRaisesRegex(ValueError, r"invalid view foo\.bar"):
                print_dfs_tree("foo", "bar", print_downstream_tree=False)
