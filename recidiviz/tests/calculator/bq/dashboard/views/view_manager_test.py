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

"""Tests for view_manager.py."""
# pylint: disable=line-too-long

import unittest
from unittest import mock

from google.cloud import bigquery

from recidiviz.calculator.bq import bqview


class ViewManagerTest(unittest.TestCase):
    """Tests for view_manager.py."""

    def setUp(self):

        sample_views = [
            {'view_id': 'my_fake_view',
             'view_query': 'SELECT NULL LIMIT 0'},
            {'view_id': 'my_other_fake_view',
             'view_query': 'SELECT NULL LIMIT 0'},
        ]
        self.mock_views = [bqview.BigQueryView(**view) for view in sample_views]
        self.mock_project_id = 'fake-recidiviz-project'
        self.mock_view_dataset_name = 'my_views_dataset'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_view_dataset_name)

        self.bq_utils_patcher = mock.patch(
            'recidiviz.calculator.bq.dashboard.views.view_manager.bq_utils')
        self.mock_bq_utils = self.bq_utils_patcher.start()

        self.client_patcher = mock.patch(
            'recidiviz.calculator.bq.dashboard.views.view_manager.bq_utils.client')
        self.mock_client = self.client_patcher.start().return_value


    def tearDown(self):
        self.bq_utils_patcher.stop()
        self.client_patcher.stop()


    def test_create_dataset_and_update_views(self):
        """Test that create_dataset_and_update_views creates a dataset
            if necessary, and updates all views.
        """
        self.mock_client.dataset.return_value = self.mock_dataset

        # Note: This is a hack because view_manager imports global code that
        # attempts to use the SessionFactory to create a Session object before
        # we have initialized the database engine for that schema in setUp. In
        # production code, server db engines are initialized globally as part of
        # startup, so this will generally not be an issue.
        from recidiviz.calculator.bq.dashboard.views import view_manager

        view_manager.create_dataset_and_update_views(
            self.mock_view_dataset_name,
            self.mock_views
        )

        self.mock_bq_utils.create_dataset_if_necessary.assert_called_with(
            self.mock_dataset)

        self.mock_bq_utils.create_or_update_view.assert_has_calls(
            [mock.call(self.mock_dataset, view) for view in self.mock_views]
        )
