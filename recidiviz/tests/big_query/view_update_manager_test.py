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

"""Tests for view_update_manager.py."""

import unittest
from typing import Set, Tuple
from unittest import mock
from unittest.mock import call, patch

from google.cloud import bigquery

from recidiviz.big_query import view_update_manager
from recidiviz.big_query.big_query_table_checker import BigQueryTableChecker
from recidiviz.big_query.big_query_view import (
    BigQueryAddress,
    BigQueryView,
    SimpleBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.view_registry.dataset_overrides import (
    dataset_overrides_for_view_builders,
)
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS
from recidiviz.view_registry.deployed_views import (
    DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
    all_deployed_view_builders,
)

_PROJECT_ID = "fake-recidiviz-project"
_DATASET_NAME = "my_views_dataset"
_DATASET_NAME_2 = "my_views_dataset_2"


class ViewManagerTest(unittest.TestCase):
    """Tests for view_update_manager.py."""

    def setUp(self) -> None:
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = _PROJECT_ID
        self.mock_dataset_ref_ds_1 = bigquery.dataset.DatasetReference(
            _PROJECT_ID, "dataset_1"
        )

        self.client_patcher = patch(
            "recidiviz.big_query.view_update_manager.BigQueryClientImpl"
        )

        self.mock_client_constructor = self.client_patcher.start()
        self.mock_client = self.mock_client_constructor.return_value

        self.client_patcher_2 = mock.patch(
            "recidiviz.big_query.big_query_table_checker.BigQueryClientImpl"
        )
        self.client_patcher_2.start()

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.client_patcher_2.stop()
        self.metadata_patcher.stop()

    def test_create_managed_dataset_and_deploy_views_for_view_builders_simple(
        self,
    ) -> None:
        """Test that create_managed_dataset_and_deploy_views_for_view_builders creates
        a dataset if necessary, and updates all views built by the view builders. No
        |historically_managed_datasets_to_clean| provided, so nothing should be
        cleaned up. The only deleted table calls should be the ones from recreating
        views so changes can be reflected in schema."""
        dataset = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME)

        sample_views = [
            {
                "view_id": "my_fake_view",
                "view_query_template": "SELECT NULL LIMIT 0",
            },
            {
                "view_id": "my_other_fake_view",
                "view_query_template": "SELECT NULL LIMIT 0",
            },
        ]
        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                description=f"{view['view_id']} description",
                should_materialize=False,
                projects_to_deploy=None,
                materialized_address_override=None,
                should_build_predicate=None,
                **view,
            )
            for view in sample_views
        ]

        self.mock_client.dataset_ref_for_id.return_value = dataset

        view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            view_builders_to_update=mock_view_builders,
            historically_managed_datasets_to_clean=None,
        )

        self.mock_client.dataset_ref_for_id.assert_called_with(_DATASET_NAME)
        self.mock_client.create_dataset_if_necessary.assert_called_with(dataset, None)
        self.mock_client.create_or_update_view.assert_has_calls(
            [mock.call(view_builder.build()) for view_builder in mock_view_builders],
            any_order=True,
        )
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(_DATASET_NAME, "my_fake_view"),
                mock.call(_DATASET_NAME, "my_other_fake_view"),
            ],
            any_order=True,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.assertEqual(self.mock_client.delete_table.call_count, 2)

    def test_rematerialize_views(self) -> None:
        """Test that create_managed_dataset_and_deploy_views_for_view_builders only
        updates views that have a set materialized_address when the
        materialized_views_only flag is set to True.
        """
        dataset = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME)

        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id="my_fake_view",
                description="my_fake_view description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=True,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id="my_fake_view_2",
                description="my_fake_view_2 description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=False,
            ),
        ]

        self.mock_client.dataset_ref_for_id.return_value = dataset

        view_update_manager.rematerialize_views_for_view_builders(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            views_to_update_builders=mock_view_builders,
            all_view_builders=mock_view_builders,
        )

        self.mock_client.materialize_view_to_table.assert_has_calls(
            [mock.call(mock_view_builders[0].build())], any_order=True
        )

    def test_create_managed_dataset_and_deploy_views_for_view_builders_no_materialize_no_update(
        self,
    ) -> None:
        """Tests that we don't materialize any views if they have not been updated"""
        dataset = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME)

        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id="my_fake_view",
                description="my_fake_view description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=True,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id="my_fake_view_2",
                description="my_fake_view_2 description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=True,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id="my_fake_view_3",
                description="my_fake_view_3 description",
                view_query_template=f"SELECT * FROM `{{project_id}}.{_DATASET_NAME}.my_fake_view` "
                f"JOIN `{{project_id}}.{_DATASET_NAME}.my_fake_view_2`;",
                should_materialize=True,
            ),
        ]

        def mock_get_table(
            _dataset_ref: bigquery.DatasetReference, view_id: str
        ) -> bigquery.Table:
            if mock_view_builders[0].view_id == view_id:
                view_builder = mock_view_builders[0]
            elif mock_view_builders[1].view_id == view_id:
                view_builder = mock_view_builders[1]
            elif mock_view_builders[2].view_id == view_id:
                view_builder = mock_view_builders[2]
            else:
                raise ValueError(f"Unexpected view id [{view_id}]")
            view = view_builder.build()
            return mock.MagicMock(
                view_query=view.view_query,
                schema=[bigquery.SchemaField("some_field", "STRING", "REQUIRED")],
            )

        # Create/Update returns the table that was already there
        def mock_create_or_update(view: BigQueryView) -> bigquery.Table:
            dataset_ref = bigquery.dataset.DatasetReference(
                _PROJECT_ID, view.dataset_id
            )
            return mock_get_table(dataset_ref, view.view_id)

        self.mock_client.dataset_ref_for_id.return_value = dataset

        self.mock_client.get_table = mock_get_table
        self.mock_client.create_or_update_view.side_effect = mock_create_or_update

        view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            view_builders_to_update=mock_view_builders,
            historically_managed_datasets_to_clean=None,
        )

        self.mock_client.dataset_ref_for_id.assert_called_with(_DATASET_NAME)
        self.mock_client.create_dataset_if_necessary.assert_called_with(dataset, None)
        self.mock_client.create_or_update_view.assert_has_calls(
            [
                mock.call(mock_view_builders[0].build()),
                mock.call(mock_view_builders[1].build()),
                mock.call(mock_view_builders[2].build()),
            ],
            any_order=True,
        )

        # Materialize is not called!
        self.mock_client.materialize_view_to_table.assert_not_called()

        # Only delete calls should be from recreating views to have changes updating
        # in schema from the dag walker
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(_DATASET_NAME, "my_fake_view"),
                mock.call(_DATASET_NAME, "my_fake_view_2"),
                mock.call(_DATASET_NAME, "my_fake_view_3"),
            ],
            any_order=True,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.assertEqual(self.mock_client.delete_table.call_count, 3)

    def test_create_managed_dataset_and_deploy_views_for_view_builders_materialize_children(
        self,
    ) -> None:
        """Tests that we don't materialize any views if they have not been updated"""
        dataset = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME)
        materialized_dataset = bigquery.dataset.DatasetReference(
            _PROJECT_ID, _DATASET_NAME_2
        )

        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id="my_fake_view",
                description="my_fake_view description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=True,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id="my_fake_view_2",
                description="my_fake_view_2 description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=True,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id="my_fake_view_3",
                description="my_fake_view_3 description",
                view_query_template=f"SELECT * FROM `{{project_id}}.{_DATASET_NAME}.my_fake_view` "
                f"JOIN `{{project_id}}.{_DATASET_NAME}.my_fake_view_2`;",
                should_materialize=True,
                materialized_address_override=BigQueryAddress(
                    dataset_id=_DATASET_NAME_2, table_id="materialized_table"
                ),
            ),
        ]

        def mock_get_table(
            _dataset_ref: bigquery.DatasetReference, view_id: str
        ) -> bigquery.Table:
            if mock_view_builders[0].view_id == view_id:
                view_builder = mock_view_builders[0]
            elif mock_view_builders[1].view_id == view_id:
                view_builder = mock_view_builders[1]
            elif mock_view_builders[2].view_id == view_id:
                view_builder = mock_view_builders[2]
            else:
                raise ValueError(f"Unexpected view id [{view_id}]")
            view = view_builder.build()
            if view.view_id != "my_fake_view_2":
                view_query = view.view_query
            else:
                # Old view query is different for this view!
                view_query = "SELECT 1 LIMIT 0"
            return mock.MagicMock(
                view_query=view_query,
                schema=[bigquery.SchemaField("some_field", "STRING", "REQUIRED")],
            )

        # Create/Update returns the table that was already there
        def mock_create_or_update(view: BigQueryView) -> bigquery.Table:
            dataset_ref = bigquery.dataset.DatasetReference(
                _PROJECT_ID, view.dataset_id
            )
            table = mock_get_table(dataset_ref, view.view_id)
            if view.view_id == "my_fake_view_2":
                table.view_query = mock_view_builders[1].build().view_query
            return table

        def mock_get_dataset_ref(dataset_id: str) -> bigquery.dataset.DatasetReference:
            if dataset_id == dataset.dataset_id:
                return dataset
            if dataset_id == materialized_dataset.dataset_id:
                return materialized_dataset
            raise ValueError(f"No dataset for id: {dataset_id}")

        self.mock_client.dataset_ref_for_id.side_effect = mock_get_dataset_ref

        self.mock_client.get_table = mock_get_table
        self.mock_client.create_or_update_view.side_effect = mock_create_or_update

        view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            view_builders_to_update=mock_view_builders,
            historically_managed_datasets_to_clean=None,
        )

        self.mock_client.dataset_ref_for_id.assert_has_calls(
            [mock.call(_DATASET_NAME), mock.call(_DATASET_NAME_2)]
        )
        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [mock.call(dataset, None), mock.call(materialized_dataset, None)]
        )
        self.mock_client.create_or_update_view.assert_has_calls(
            [
                mock.call(mock_view_builders[0].build()),
                mock.call(mock_view_builders[1].build()),
                mock.call(mock_view_builders[2].build()),
            ],
            any_order=True,
        )

        # Materialize is called where appropriate!
        self.mock_client.materialize_view_to_table.assert_has_calls(
            [
                # This view was updated
                mock.call(mock_view_builders[1].build()),
                # Child view of updated view is also materialized
                mock.call(mock_view_builders[2].build()),
            ],
            any_order=True,
        )
        # Only delete calls should be from recreating views to have changes updating in
        # schema from the dag walker
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(_DATASET_NAME, "my_fake_view"),
                mock.call(_DATASET_NAME, "my_fake_view_2"),
                mock.call(_DATASET_NAME, "my_fake_view_3"),
            ],
            any_order=True,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.assertEqual(self.mock_client.delete_table.call_count, 3)

    def test_create_managed_dataset_and_deploy_views_for_view_builders_different_datasets(
        self,
    ) -> None:
        """Test that create_managed_dataset_and_deploy_views_for_view_builders only
        updates views that have a set materialized_address when the
        materialized_views_only flag is set to True.
        """
        dataset = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME)
        dataset_2 = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME_2)

        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id="my_fake_view",
                description="my_fake_view description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=True,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME_2,
                view_id="my_fake_view_2",
                description="my_fake_view_2 description",
                view_query_template="SELECT NULL LIMIT 0",
                should_materialize=False,
            ),
        ]

        def get_dataset_ref(dataset_id: str) -> bigquery.dataset.DatasetReference:
            if dataset_id == dataset.dataset_id:
                return dataset
            if dataset_id == dataset_2.dataset_id:
                return dataset_2
            raise ValueError(f"No dataset for id: {dataset_id}")

        self.mock_client.dataset_ref_for_id.side_effect = get_dataset_ref

        view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
            view_builders_to_update=mock_view_builders,
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            historically_managed_datasets_to_clean=None,
        )

        self.mock_client.dataset_ref_for_id.assert_has_calls(
            [
                # One set of calls to create datasets and one to update views
                mock.call(_DATASET_NAME),
                mock.call(_DATASET_NAME_2),
                mock.call(_DATASET_NAME),
                mock.call(_DATASET_NAME_2),
            ],
            any_order=True,
        )
        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [mock.call(dataset, None), mock.call(dataset_2, None)]
        )
        self.mock_client.create_or_update_view.assert_has_calls(
            [
                mock.call(mock_view_builders[0].build()),
                mock.call(mock_view_builders[1].build()),
            ],
            any_order=True,
        )

        # Only delete calls should be from recreating views to have changes updating in
        # schema from the dag walker
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(_DATASET_NAME, "my_fake_view"),
                mock.call(_DATASET_NAME_2, "my_fake_view_2"),
            ],
            any_order=True,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.assertEqual(self.mock_client.delete_table.call_count, 2)

    @patch(
        "recidiviz.big_query.view_update_manager_utils.cleanup_datasets_and_delete_unmanaged_views"
    )
    def test_create_managed_dataset_and_deploy_views_for_view_builders_dataset_override(
        self, mock_cleanup_datasets_and_delete_unmanaged_views: mock.MagicMock
    ) -> None:
        """Test that create_managed_dataset_and_deploy_views_for_view_builders creates
        new datasets with a set table expiration for all datasets specified in
        dataset_overrides."""
        materialized_dataset = "other_dataset"

        override_dataset_ref = bigquery.dataset.DatasetReference(
            _PROJECT_ID, "test_prefix_" + _DATASET_NAME
        )
        override_materialized_dataset_ref = bigquery.dataset.DatasetReference(
            _PROJECT_ID, "test_prefix_" + materialized_dataset
        )

        sample_views = [
            {
                "view_id": "my_fake_view",
                "view_query_template": "SELECT NULL LIMIT 0",
            },
            {
                "view_id": "my_other_fake_view",
                "view_query_template": "SELECT NULL LIMIT 0",
            },
        ]
        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                description=f"{view['view_id']} description",
                should_materialize=False,
                projects_to_deploy=None,
                materialized_address_override=None,
                should_build_predicate=None,
                **view,
            )
            for view in sample_views
        ]
        materialized_view_builder = SimpleBigQueryViewBuilder(
            dataset_id=_DATASET_NAME,
            view_id="materialized_view",
            view_query_template="a",
            description="materialized_view description",
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id=materialized_dataset,
                table_id="some_table",
            ),
            should_build_predicate=None,
        )

        mock_view_builders += [materialized_view_builder]
        dataset_overrides = dataset_overrides_for_view_builders(
            view_dataset_override_prefix="test_prefix", view_builders=mock_view_builders
        )

        def get_dataset_ref(dataset_id: str) -> bigquery.dataset.DatasetReference:
            if dataset_id == override_dataset_ref.dataset_id:
                return override_dataset_ref
            if dataset_id == override_materialized_dataset_ref.dataset_id:
                return override_materialized_dataset_ref
            raise ValueError(f"No dataset for id: {dataset_id}")

        self.mock_client.dataset_ref_for_id.side_effect = get_dataset_ref

        view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            view_builders_to_update=mock_view_builders,
            dataset_overrides=dataset_overrides,
            historically_managed_datasets_to_clean=None,
        )

        dataset_refs = [override_dataset_ref, override_materialized_dataset_ref]
        self.mock_client.dataset_ref_for_id.assert_has_calls(
            [mock.call(dataset_ref.dataset_id) for dataset_ref in dataset_refs]
        )
        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [
                mock.call(
                    dataset_ref,
                    view_update_manager.TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
                )
                for dataset_ref in dataset_refs
            ]
        )
        self.mock_client.create_or_update_view.assert_has_calls(
            [
                mock.call(view_builder.build(dataset_overrides=dataset_overrides))
                for view_builder in mock_view_builders
            ],
            any_order=True,
        )
        self.mock_client.materialize_view_to_table.assert_has_calls(
            [
                mock.call(
                    materialized_view_builder.build(dataset_overrides=dataset_overrides)
                )
            ],
            any_order=True,
        )

        # The cleanup function should not be called since we didn't provide a
        #  historically_managed_datasets_to_clean list
        mock_cleanup_datasets_and_delete_unmanaged_views.assert_not_called()

        self.mock_client.delete_dataset.assert_not_called()
        # Only delete calls should be from recreating views to have changes updating
        # in schema from the dag walker
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call("test_prefix_" + _DATASET_NAME, "my_fake_view"),
                mock.call(
                    "test_prefix_" + _DATASET_NAME,
                    "my_other_fake_view",
                ),
                mock.call("test_prefix_" + _DATASET_NAME, "materialized_view"),
            ],
            any_order=True,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.assertEqual(self.mock_client.delete_table.call_count, 3)

    def test_create_dataset_and_update_views(self) -> None:
        """Test that create_dataset_and_update_views creates a dataset if necessary, and updates all views."""
        dataset = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME)

        sample_views = [
            {"view_id": "my_fake_view", "view_query_template": "SELECT NULL LIMIT 0"},
            {
                "view_id": "my_other_fake_view",
                "view_query_template": "SELECT NULL LIMIT 0",
            },
        ]
        mock_views = [
            BigQueryView(
                dataset_id=_DATASET_NAME,
                description=f"{view['view_id']} description",
                should_materialize=False,
                materialized_address_override=None,
                dataset_overrides=None,
                **view,
            )
            for view in sample_views
        ]

        self.mock_client.dataset_ref_for_id.return_value = dataset

        # pylint: disable=protected-access
        view_update_manager._create_managed_dataset_and_deploy_views(
            mock_views, bq_region_override="us-east1", force_materialize=False
        )

        self.mock_client_constructor.assert_called_with(region_override="us-east1")
        self.mock_client.dataset_ref_for_id.assert_called_with(_DATASET_NAME)
        self.mock_client.create_dataset_if_necessary.assert_called_with(dataset, None)
        self.mock_client.create_or_update_view.assert_has_calls(
            [mock.call(view) for view in mock_views], any_order=True
        )
        # Only delete calls should be from recreating views to have changes updating
        # in schema from the dag walker
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(_DATASET_NAME, "my_fake_view"),
                mock.call(_DATASET_NAME, "my_other_fake_view"),
            ],
            any_order=True,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.assertEqual(self.mock_client.delete_table.call_count, 2)

    def test_no_duplicate_views_in_update_list(self) -> None:
        with patch.object(
            BigQueryTableChecker, "_table_has_column"
        ) as mock_table_has_column, patch.object(
            BigQueryTableChecker, "_table_exists"
        ) as mock_table_exists:
            mock_table_has_column.return_value = True
            mock_table_exists.return_value = True
            all_views = [
                view_builder.build() for view_builder in all_deployed_view_builders()
            ]

        expected_keys: Set[Tuple[str, str]] = set()
        for view in all_views:
            dag_key = (view.dataset_id, view.table_id)
            self.assertNotIn(dag_key, expected_keys)
            expected_keys.add(dag_key)

    def test_no_views_in_source_data_datasets(self) -> None:
        for view_builder in all_deployed_view_builders():
            self.assertNotIn(
                view_builder.dataset_id,
                VIEW_SOURCE_TABLE_DATASETS,
                f"Found view [{view_builder.view_id}] in source-table-only "
                f"dataset [{view_builder.dataset_id}]",
            )

    def test_all_cross_project_views_in_cross_project_view_builders(self) -> None:
        """Tests that all views that query from both production and staging
        environments are only deployed to production."""

        with patch.object(
            BigQueryTableChecker, "_table_has_column"
        ) as mock_table_has_column, patch.object(
            BigQueryTableChecker, "_table_exists"
        ) as mock_table_exists:
            mock_table_has_column.return_value = True
            mock_table_exists.return_value = True

            for view_builder in all_deployed_view_builders():
                view_query = view_builder.build().view_query

                if (
                    GCP_PROJECT_STAGING in view_query
                    and GCP_PROJECT_PRODUCTION in view_query
                ):
                    self.assertFalse(
                        view_builder.should_deploy_in_project(GCP_PROJECT_STAGING),
                        f"Found view {view_builder.dataset_id}.{view_builder.view_id} "
                        "that queries from both production and staging projects and is "
                        "deployed in staging. This view should only be deployed in "
                        "production, as staging cannot have access to production "
                        "BigQuery.",
                    )

    def test_create_managed_dataset_and_deploy_views_for_view_builders_unmanaged_views_in_multiple_ds(
        self,
    ) -> None:
        dataset = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME)
        dataset_2 = bigquery.dataset.DatasetReference(_PROJECT_ID, _DATASET_NAME_2)

        historically_managed_datasets = {_DATASET_NAME, _DATASET_NAME_2}

        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id="my_fake_view",
                description="my_fake_view description",
                view_query_template="a",
                should_materialize=False,
                materialized_address_override=None,
                should_build_predicate=None,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME_2,
                view_id="my_other_fake_view",
                description="my_other_fake_view description",
                view_query_template="a",
                should_materialize=False,
                materialized_address_override=None,
                should_build_predicate=None,
            ),
        ]

        mock_table_resource_ds_1_table = {
            "tableReference": {
                "projectId": _PROJECT_ID,
                "datasetId": _DATASET_NAME,
                "tableId": "my_fake_view",
            },
        }

        mock_table_resource_ds_1_table_bogus = {
            "tableReference": {
                "projectId": _PROJECT_ID,
                "datasetId": _DATASET_NAME,
                "tableId": "bogus_view_1",
            },
        }

        mock_table_resource_ds_2_table = {
            "tableReference": {
                "projectId": _PROJECT_ID,
                "datasetId": _DATASET_NAME_2,
                "tableId": "my_other_fake_view",
            },
        }

        mock_table_resource_ds_2_table_bogus = {
            "tableReference": {
                "projectId": _PROJECT_ID,
                "datasetId": _DATASET_NAME_2,
                "tableId": "bogus_view_2",
            },
        }

        def mock_list_tables(dataset_id: str) -> bigquery.table.TableListItem:
            if dataset_id == dataset.dataset_id:
                return [
                    bigquery.table.TableListItem(mock_table_resource_ds_1_table),
                    bigquery.table.TableListItem(mock_table_resource_ds_1_table_bogus),
                ]
            if dataset_id == dataset_2.dataset_id:
                return [
                    bigquery.table.TableListItem(mock_table_resource_ds_2_table),
                    bigquery.table.TableListItem(mock_table_resource_ds_2_table_bogus),
                ]
            raise ValueError(f"No tables for id: {dataset_id}")

        self.mock_client.list_tables.side_effect = mock_list_tables

        def get_dataset_ref(dataset_id: str) -> bigquery.dataset.DatasetReference:
            if dataset_id == dataset.dataset_id:
                return dataset
            if dataset_id == dataset_2.dataset_id:
                return dataset_2
            raise ValueError(f"No dataset for id: {dataset_id}")

        self.mock_client.dataset_ref_for_id.side_effect = get_dataset_ref
        self.mock_client.dataset_exists.return_value = True

        view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            view_builders_to_update=mock_view_builders,
            historically_managed_datasets_to_clean=historically_managed_datasets,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.mock_client.list_tables.assert_called()
        self.mock_client.delete_table.assert_has_calls(
            [
                call(_DATASET_NAME, "my_fake_view"),
                call(_DATASET_NAME_2, "my_other_fake_view"),
                # above two calls are from deleting and recreating every view from
                # _create_or_update_view_and_materialize_if_necessary()
                call(_DATASET_NAME, "bogus_view_1"),
                call(_DATASET_NAME_2, "bogus_view_2"),
                # these two calls are from the actual cleaning up of unmanaged views
            ],
            any_order=True,
        )
        self.assertEqual(self.mock_client.delete_table.call_count, 4)

        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [call(dataset, None), call(dataset_2, None)], any_order=True
        )
        self.assertEqual(self.mock_client.create_dataset_if_necessary.call_count, 2)
        self.mock_client.create_or_update_view.assert_has_calls(
            [mock.call(view_builder.build()) for view_builder in mock_view_builders],
            any_order=True,
        )

    def test_all_deployed_datasets_registered_as_managed(self) -> None:
        with patch.object(
            BigQueryTableChecker, "_table_has_column"
        ) as mock_table_has_column, patch.object(
            BigQueryTableChecker, "_table_exists"
        ) as mock_table_exists:
            mock_table_has_column.return_value = True
            mock_table_exists.return_value = True
            all_views = [
                view_builder.build() for view_builder in all_deployed_view_builders()
            ]

        for view in all_views:
            self.assertIn(
                view.address.dataset_id, DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED
            )
            if view.materialized_address:
                self.assertIn(
                    view.materialized_address.dataset_id,
                    DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
                )
