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

import unittest
from unittest import mock

from google.cloud import bigquery

from recidiviz.big_query.big_query_view import BigQueryView
class ViewManagerTest(unittest.TestCase):
    """Tests for view_manager.py."""

    def setUp(self):
        project_id = 'fake-recidiviz-project'
        self.mock_view_dataset_name = 'my_views_dataset'
        self.mock_dataset = bigquery.dataset.DatasetReference(
            project_id, self.mock_view_dataset_name)

        sample_views = [
            {'dataset_id': self.mock_dataset.dataset_id,
             'view_id': 'my_fake_view',
             'view_query_template': 'SELECT NULL LIMIT 0'},
            {'dataset_id': self.mock_dataset.dataset_id,
             'view_id': 'my_other_fake_view',
             'view_query_template': 'SELECT NULL LIMIT 0'},
        ]
        self.mock_views = [BigQueryView(**view) for view in sample_views]

        self.metadata_patcher = mock.patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = project_id

        self.bq_client_patcher = mock.patch(
            'recidiviz.calculator.query.state.view_manager.BigQueryClientImpl')
        self.mock_client = self.bq_client_patcher.start().return_value

        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset

    def tearDown(self):
        self.bq_client_patcher.stop()
        self.metadata_patcher.stop()

    def test_create_dataset_and_update_views(self):
        """Test that create_dataset_and_update_views creates a dataset if necessary, and updates all views."""
        self.mock_client.dataset.return_value = self.mock_dataset

        # Note: This is a hack because view_manager imports global code that
        # attempts to use the SessionFactory to create a Session object before
        # we have initialized the database engine for that schema in setUp. In
        # production code, server db engines are initialized globally as part of
        # startup, so this will generally not be an issue.
        # pylint: disable=import-outside-toplevel
        from recidiviz.calculator.query.state import view_manager

        view_manager.create_dataset_and_update_views({self.mock_view_dataset_name: self.mock_views})

        self.mock_client.create_dataset_if_necessary.assert_called_with(self.mock_dataset)

        self.mock_client.create_or_update_view.assert_has_calls(
            [mock.call(self.mock_dataset, view) for view in self.mock_views]
        )
