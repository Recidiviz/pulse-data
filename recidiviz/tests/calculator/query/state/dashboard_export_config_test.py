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

from mock import patch

from recidiviz.calculator.query.state import dashboard_export_config, view_config
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.persistence.database.base_schema import JailsBase
from recidiviz.tests.utils import fakes


class DashboardExportConfigTest(unittest.TestCase):
    """Tests for dashboard_export_config.py."""

    def setUp(self) -> None:
        self.metadata_patcher = patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = 'recidiviz-456'

        fakes.use_in_memory_sqlite_database(JailsBase)

    def tearDown(self):
        self.metadata_patcher.stop()

    def test_VIEWS_TO_EXPORT_types(self):
        """Make sure that all VIEWS_TO_EXPORT are of type BigQueryView."""
        for view in dashboard_export_config.VIEWS_TO_EXPORT:
            self.assertIsInstance(view, BigQueryView)

    def test_VIEWS_TO_EXCLUDE_FROM_EXPORT_all_excluded(self):
        """Make sure a view is excluded from VIEWS_TO_EXPORT if listed in
        VIEWS_TO_EXCLUDE_FROM_EXPORT.
        """

        for view in dashboard_export_config.VIEWS_TO_EXCLUDE_FROM_EXPORT:
            self.assertNotIn(view, dashboard_export_config.VIEWS_TO_EXPORT)

    def test_view_dataset_ids(self):
        for dataset_id, views in view_config.VIEWS_TO_UPDATE.items():
            for view in views:
                if view.dataset_id != dataset_id:
                    self.fail(f'{view.view_id} has dataset id {view.dataset_id} that does not match '
                              f'VIEWS_TO_UPDATE id {dataset_id}')
