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
from typing import Tuple, Set
from unittest import mock
from unittest.mock import patch

from google.cloud import bigquery

from recidiviz.big_query import view_update_manager
from recidiviz.big_query.big_query_view import BigQueryView, SimpleBigQueryViewBuilder
from recidiviz.big_query.view_update_manager import BigQueryViewNamespace, VIEW_BUILDERS_BY_NAMESPACE, \
    VIEW_SOURCE_TABLE_DATASETS
from recidiviz.ingest.views.metadata_helpers import BigQueryTableColumnChecker

_PROJECT_ID = 'fake-recidiviz-project'
_DATASET_NAME = 'my_views_dataset'
_DATASET_NAME_2 = 'my_views_dataset_2'


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
        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_query_template='a',
                should_materialize=False,
                should_build_predicate=None,
                **view,
            )
            for view in sample_views
        ]

        self.mock_client.dataset_ref_for_id.return_value = dataset

        view_update_manager.create_dataset_and_update_views_for_view_builders(
            BigQueryViewNamespace.VALIDATION, mock_view_builders)

        self.mock_client.dataset_ref_for_id.assert_called_with(_DATASET_NAME)
        self.mock_client.create_dataset_if_necessary.assert_called_with(dataset, None)
        self.mock_client.create_or_update_view.assert_has_calls(
            [mock.call(dataset, view_builder.build()) for view_builder in mock_view_builders], any_order=True)

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
            [mock.call(dataset, mock_view_builders[0].build())], any_order=True)

    def test_create_dataset_and_update_views_for_view_builders_different_datasets(self) -> None:
        """Test that create_dataset_and_update_views_for_view_builders only updates views that have a set
        materialized_view_table_id when the materialized_views_only flag is set to True."""
        dataset = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME)
        dataset_2 = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME_2)

        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id='my_fake_view',
                view_query_template='SELECT NULL LIMIT 0',
                should_materialize=True
            ),
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME_2,
                view_id='my_fake_view_2',
                view_query_template='SELECT NULL LIMIT 0',
                should_materialize=False
            )
        ]

        def get_dataset_ref(dataset_id: str) -> bigquery.dataset.DatasetReference:
            if dataset_id == dataset.dataset_id:
                return dataset
            if dataset_id == dataset_2.dataset_id:
                return dataset_2
            raise ValueError(f'No dataset for id: {dataset_id}')

        self.mock_client.dataset_ref_for_id.side_effect = get_dataset_ref

        view_update_manager.create_dataset_and_update_views_for_view_builders(
            BigQueryViewNamespace.VALIDATION,
            mock_view_builders,
        )

        self.mock_client.dataset_ref_for_id.assert_has_calls([
            # One set of calls to create datasets and one to update views
            mock.call(_DATASET_NAME), mock.call(_DATASET_NAME_2),
            mock.call(_DATASET_NAME), mock.call(_DATASET_NAME_2)], any_order=True)
        self.mock_client.create_dataset_if_necessary.assert_has_calls([
            mock.call(dataset, None),
            mock.call(dataset_2, None)
        ])
        self.mock_client.create_or_update_view.assert_has_calls(
            [mock.call(dataset, mock_view_builders[0].build()),
             mock.call(dataset_2, mock_view_builders[1].build())], any_order=True)

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
        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_query_template='a',
                should_materialize=False,
                should_build_predicate=None,
                **view,
            )
            for view in sample_views
        ]

        self.mock_client.dataset_ref_for_id.return_value = dataset

        view_update_manager.create_dataset_and_update_views_for_view_builders(
            BigQueryViewNamespace.VALIDATION, mock_view_builders, dataset_overrides)

        self.mock_client.dataset_ref_for_id.assert_called_with(temp_dataset_id)
        self.mock_client.create_dataset_if_necessary.assert_called_with(
            dataset, view_update_manager.TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS)
        self.mock_client.create_or_update_view.assert_has_calls(
            [mock.call(dataset, view_builder.build(dataset_overrides=dataset_overrides))
             for view_builder in mock_view_builders], any_order=True)

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
        self.mock_client.create_or_update_view.assert_has_calls([mock.call(dataset, view) for view in mock_views],
                                                                any_order=True)

    def test_no_duplicate_views_in_update_list(self) -> None:
        with patch.object(BigQueryTableColumnChecker, '_table_has_column') as mock_table_has_column:
            mock_table_has_column.return_value = True
            all_views = []
            for view_builder_list in VIEW_BUILDERS_BY_NAMESPACE.values():
                for view_builder in view_builder_list:
                    all_views.append(view_builder.build())

        expected_keys: Set[Tuple[str, str]] = set()
        for view in all_views:
            dag_key = (view.dataset_id, view.table_id)
            self.assertNotIn(dag_key, expected_keys)
            expected_keys.add(dag_key)

    def test_no_views_in_source_data_datasets(self) -> None:
        for view_builder_list in VIEW_BUILDERS_BY_NAMESPACE.values():
            for view_builder in view_builder_list:
                self.assertNotIn(
                    view_builder.dataset_id, VIEW_SOURCE_TABLE_DATASETS,
                    f'Found view [{view_builder.view_id}] in source-table-only dataset [{view_builder.dataset_id}]')
