# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests deployed_views"""

from typing import Set
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_table_checker import BigQueryTableChecker
from recidiviz.big_query.big_query_view import BigQueryAddress
from recidiviz.view_registry.deployed_views import DEPLOYED_VIEW_BUILDERS


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
class DeployedViewsTest(unittest.TestCase):
    def test_unique_addresses(self) -> None:
        view_addresses: Set[BigQueryAddress] = set()
        with patch.object(
            BigQueryTableChecker, "_table_has_column"
        ) as mock_table_has_column, patch.object(
            BigQueryTableChecker, "_table_exists"
        ) as mock_table_exists:
            mock_table_has_column.return_value = True
            mock_table_exists.return_value = True

            for view_builder in DEPLOYED_VIEW_BUILDERS:
                address = view_builder.build().address
                self.assertNotIn(address, view_addresses)
                view_addresses.add(address)
