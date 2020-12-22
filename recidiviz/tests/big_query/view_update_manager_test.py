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

"""Tests for view_update_manager.py."""

import unittest
from unittest import mock
from unittest.mock import patch

from google.cloud import bigquery

from recidiviz.big_query import view_update_manager
from recidiviz.big_query.big_query_view import BigQueryView, SimpleBigQueryViewBuilder
from recidiviz.big_query.view_update_manager import BigQueryViewNamespace

_PROJECT_ID = 'fake-recidiviz-project'
_DATASET_NAME = 'my_views_dataset'


class ViewManagerTest(unittest.TestCase):
    """Tests for view_update_manager.py."""

    def setUp(self) -> None:
        self.metadata_patcher = mock.patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = _PROJECT_ID

        self.client_patcher = patch(
            'recidiviz.big_query.view_update_manager.BigQueryClientImpl')
        self.mock_client = self.client_patcher.start().return_value

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_create_dataset_and_update_views_for_view_builders(self) -> None:
        """Test that create_dataset_and_update_views_for_view_builders creates a dataset if necessary,
        and updates all views built by the view builders."""
        dataset = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME)

        sample_views = [
            {'view_id': 'my_fake_view', 'view_query': 'SELECT NULL LIMIT 0', 'materialized_view_table_id': 'table_id'},
            {'view_id': 'my_other_fake_view', 'view_query': 'SELECT NULL LIMIT 0'},
        ]
        mock_view_builders = [SimpleBigQueryViewBuilder(
            dataset_id=_DATASET_NAME, view_query_template='a', should_materialize=False, **view)
            for view in sample_views]

        self.mock_client.dataset_ref_for_id.return_value = dataset

        view_update_manager.create_dataset_and_update_views_for_view_builders(
            BigQueryViewNamespace.VALIDATION, mock_view_builders)

        self.mock_client.dataset_ref_for_id.assert_called_with(_DATASET_NAME)
        self.mock_client.create_dataset_if_necessary.assert_called_with(dataset, None)
        self.mock_client.create_or_update_view.assert_has_calls(
            [mock.call(dataset, view_builder.build()) for view_builder in mock_view_builders])

    def test_create_dataset_and_update_views_for_view_builders_materialized_views_only(self) -> None:
        """Test that create_dataset_and_update_views_for_view_builders only updates views that have a set
        materialized_view_table_id when the materialized_views_only flag is set to True."""
        dataset = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME)

        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id='my_fake_view',
                view_query_template='SELECT NULL LIMIT 0',
                should_materialize=True
            ),
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id='my_fake_view_2',
                view_query_template='SELECT NULL LIMIT 0',
                should_materialize=False
            )
        ]

        self.mock_client.dataset_ref_for_id.return_value = dataset

        view_update_manager.create_dataset_and_update_views_for_view_builders(
            BigQueryViewNamespace.VALIDATION,
            mock_view_builders,
            materialized_views_only=True
        )

        self.mock_client.dataset_ref_for_id.assert_called_with(_DATASET_NAME)
        self.mock_client.create_dataset_if_necessary.assert_called_with(dataset, None)
        self.mock_client.create_or_update_view.assert_has_calls(
            [mock.call(dataset, mock_view_builders[0].build())])

    def test_create_dataset_and_update_views_for_view_builders_dataset_override(self) -> None:
        """Test that create_dataset_and_update_views_for_view_builders creates new datasets with a set table expiration
        for all datasets specified in dataset_overrides."""
        temp_dataset_id = 'test_prefix_' + _DATASET_NAME
        dataset = bigquery.dataset.DatasetReference(_PROJECT_ID, temp_dataset_id)

        dataset_overrides = {
            _DATASET_NAME: temp_dataset_id
        }

        sample_views = [
            {'view_id': 'my_fake_view', 'view_query': 'SELECT NULL LIMIT 0', 'materialized_view_table_id': 'table_id'},
            {'view_id': 'my_other_fake_view', 'view_query': 'SELECT NULL LIMIT 0'},
        ]
        mock_view_builders = [SimpleBigQueryViewBuilder(
            dataset_id=_DATASET_NAME, view_query_template='a', should_materialize=False, **view)
            for view in sample_views]

        self.mock_client.dataset_ref_for_id.return_value = dataset

        view_update_manager.create_dataset_and_update_views_for_view_builders(
            BigQueryViewNamespace.VALIDATION, mock_view_builders, dataset_overrides)

        self.mock_client.dataset_ref_for_id.assert_called_with(temp_dataset_id)
        self.mock_client.create_dataset_if_necessary.assert_called_with(
            dataset, view_update_manager.TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS)
        self.mock_client.create_or_update_view.assert_has_calls(
            [mock.call(dataset, view_builder.build(dataset_overrides=dataset_overrides))
             for view_builder in mock_view_builders])

    def test_create_dataset_and_update_views(self) -> None:
        """Test that create_dataset_and_update_views creates a dataset if necessary, and updates all views."""
        dataset = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME)

        sample_views = [
            {'view_id': 'my_fake_view', 'view_query': 'SELECT NULL LIMIT 0'},
            {'view_id': 'my_other_fake_view', 'view_query': 'SELECT NULL LIMIT 0'},
        ]
        mock_views = [BigQueryView(dataset_id=_DATASET_NAME,
                                   view_id=view['view_id'],
                                   view_query_template='a',
                                   should_materialize=False)
                      for view in sample_views]

        self.mock_client.dataset_ref_for_id.return_value = dataset

        # pylint: disable=protected-access
        view_update_manager._create_dataset_and_update_views(mock_views)

        self.mock_client.dataset_ref_for_id.assert_called_with(_DATASET_NAME)
        self.mock_client.create_dataset_if_necessary.assert_called_with(dataset, None)
        self.mock_client.create_or_update_view.assert_has_calls([mock.call(dataset, view) for view in mock_views])
