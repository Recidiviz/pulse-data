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
"""Tests for find_unused_bq_views.py."""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.tools.find_unused_bq_views import get_unused_addresses_from_all_views_dag
from recidiviz.view_registry.deployed_views import build_all_deployed_views_dag_walker


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="recidiviz-456"))
class TestFindUnusedBQViews(unittest.TestCase):
    def test_find_unused_views_does_not_crash(self) -> None:
        dag_walker = build_all_deployed_views_dag_walker()
        self.assertTrue(len(dag_walker.views) > 0)

        unused_view_addresses = get_unused_addresses_from_all_views_dag(dag_walker)

        self.assertTrue(
            all(isinstance(a, BigQueryAddress) for a in unused_view_addresses)
        )
        # TODO(#25808): Make the unused_view_addresses list empty by deleting views /
        #  adding appropriate exemptions and then enforce that it is empty here.
