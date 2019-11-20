# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for dashboard_export_config.py."""

import unittest

from recidiviz.calculator.query.state import dashboard_export_config
from recidiviz.calculator.query import bqview


class DashboardExportConfigTest(unittest.TestCase):
    """Tests for dashboard_export_config.py."""

    def test_VIEWS_TO_EXPORT_types(self):
        """Make sure that all VIEWS_TO_EXPORT are of type BigQueryView."""
        for view in dashboard_export_config.VIEWS_TO_EXPORT:
            self.assertIsInstance(view, bqview.BigQueryView)

    def test_VIEWS_TO_EXCLUDE_FROM_EXPORT_all_excluded(self):
        """Make sure a view is excluded from VIEWS_TO_EXPORT if listed in
        VIEWS_TO_EXCLUDE_FROM_EXPORT.
        """

        for view in dashboard_export_config.VIEWS_TO_EXCLUDE_FROM_EXPORT:
            self.assertNotIn(view, dashboard_export_config.VIEWS_TO_EXPORT)
