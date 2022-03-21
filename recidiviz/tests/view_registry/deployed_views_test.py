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

import unittest
from typing import Set
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_table_checker import BigQueryTableChecker
from recidiviz.big_query.big_query_view import BigQueryAddress
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.view_registry.deployed_views import (
    all_deployed_view_builders,
    deployed_view_builders,
)


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
class DeployedViewsTest(unittest.TestCase):
    """Tests the deployed views configuration"""

    def test_unique_addresses(self) -> None:
        view_addresses: Set[BigQueryAddress] = set()
        with patch.object(
            BigQueryTableChecker, "_table_has_column"
        ) as mock_table_has_column, patch.object(
            BigQueryTableChecker, "_table_exists"
        ) as mock_table_exists:
            mock_table_has_column.return_value = True
            mock_table_exists.return_value = True

            for view_builder in all_deployed_view_builders():
                address = view_builder.build().address
                self.assertNotIn(address, view_addresses)
                view_addresses.add(address)

    def test_deployed_views(self) -> None:
        with patch.object(
            BigQueryTableChecker, "_table_has_column"
        ) as mock_table_has_column, patch.object(
            BigQueryTableChecker, "_table_exists"
        ) as mock_table_exists:
            mock_table_has_column.return_value = True
            mock_table_exists.return_value = True

            all_view_builders = all_deployed_view_builders()
            staging_view_builders = deployed_view_builders(GCP_PROJECT_STAGING)
            prod_view_builders = deployed_view_builders(GCP_PROJECT_PRODUCTION)

            self.assertGreater(len(all_view_builders), 0)

            combined_view_builder_view_ids = {
                builder.view_id
                for builder in staging_view_builders + prod_view_builders
            }

            all_view_builder_view_ids = {
                builder.view_id for builder in all_view_builders
            }

            self.assertSetEqual(
                combined_view_builder_view_ids,
                all_view_builder_view_ids,
            )

            self.assertGreater(len(staging_view_builders), 0)
            self.assertLessEqual(len(staging_view_builders), len(all_view_builders))

            self.assertGreater(len(prod_view_builders), 0)
            self.assertLessEqual(len(prod_view_builders), len(all_view_builders))
