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
from typing import Optional, Set, Tuple
from unittest import mock
from unittest.mock import call, create_autospec, patch

from google.cloud import bigquery

from recidiviz.big_query import view_update_manager
from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryViewMaterializationResult
from recidiviz.big_query.big_query_view import BigQueryView, SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalkerProcessingFailureMode,
)
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.big_query.big_query_view_update_sandbox_context import (
    BigQueryViewUpdateSandboxContext,
)
from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.cloud_resources.resource_label import ResourceLabel
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_source_table_datasets,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.view_registry.address_overrides_factory import (
    address_overrides_for_view_builders,
)
from recidiviz.view_registry.deployed_views import (
    DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
    all_deployed_view_builders,
)

_PROJECT_ID = "fake-recidiviz-project"
_DATASET_NAME = "my_views_dataset"
_DATASET_NAME_2 = "my_views_dataset_2"
_DATASET_NAME_3 = "my_views_dataset_3"


class ViewManagerTest(unittest.TestCase):
    """Tests for view_update_manager.py."""

    def setUp(self) -> None:
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = _PROJECT_ID

        self.client_patcher = patch(
            "recidiviz.big_query.view_update_manager.BigQueryClientImpl"
        )

        self.mock_client_constructor = self.client_patcher.start()
        self.mock_client = self.mock_client_constructor.return_value

        def fake_materialize_view_to_table(
            view: BigQueryView,
            # pylint: disable=unused-argument
            use_query_cache: bool,
            view_configuration_changed: bool,
            job_labels: Optional[list[ResourceLabel]] = None,
        ) -> BigQueryViewMaterializationResult:
            return BigQueryViewMaterializationResult(
                view_address=view.address,
                materialized_table=create_autospec(bigquery.Table),
                completed_materialization_job=create_autospec(bigquery.QueryJob),
            )

        self.mock_client.materialize_view_to_table.side_effect = (
            fake_materialize_view_to_table
        )

        self.view_source_table_datasets = get_source_table_datasets(
            metadata.project_id()
        )

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.metadata_patcher.stop()

    def test_create_managed_dataset_and_deploy_views_for_view_builders_simple(
        self,
    ) -> None:
        """Test that create_managed_dataset_and_deploy_views_for_view_builders creates
        a dataset if necessary, and updates all views built by the view builders. No
        |historically_managed_datasets_to_clean| provided, so nothing should be
        cleaned up. The only deleted table calls should be the ones from recreating
        views so changes can be reflected in schema."""
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
                clustering_fields=None,
                time_partitioning=None,
                materialized_table_schema=None,
                **view,
            )
            for view in sample_views
        ]

        view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
            view_builders_to_update=mock_view_builders,
            historically_managed_datasets_to_clean=None,
            rematerialize_changed_views_only=True,
            failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
        )

        self.mock_client.create_dataset_if_necessary.assert_called_with(
            _DATASET_NAME, default_table_expiration_ms=None
        )
        self.mock_client.create_or_update_view.assert_has_calls(
            [
                mock.call(view_builder.build(), might_exist=True)
                for view_builder in mock_view_builders
            ],
            any_order=True,
        )
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    BigQueryAddress(dataset_id=_DATASET_NAME, table_id="my_fake_view"),
                    not_found_ok=True,
                ),
                mock.call(
                    BigQueryAddress(
                        dataset_id=_DATASET_NAME, table_id="my_other_fake_view"
                    ),
                    not_found_ok=True,
                ),
            ],
            any_order=True,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.assertEqual(self.mock_client.delete_table.call_count, 2)

    def test_create_managed_dataset_and_deploy_views_for_view_builders_no_materialize_no_update(
        self,
    ) -> None:
        """Tests that we don't materialize any views if they have not been updated"""
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

        def mock_get_table(address: BigQueryAddress) -> bigquery.Table:
            schema = [bigquery.SchemaField("some_field", "STRING", "REQUIRED")]

            if address in {
                mock_view_builders[0].materialized_address,
                mock_view_builders[1].materialized_address,
                mock_view_builders[2].materialized_address,
            }:
                return mock.MagicMock(
                    dataset_id=address.dataset_id,
                    table_id=address.table_id,
                    schema=schema,
                )

            if mock_view_builders[0].view_id == address.table_id:
                view_builder = mock_view_builders[0]
            elif mock_view_builders[1].view_id == address.table_id:
                view_builder = mock_view_builders[1]
            elif mock_view_builders[2].view_id == address.table_id:
                view_builder = mock_view_builders[2]
            else:
                raise ValueError(f"Unexpected view id [{address.table_id}]")
            view = view_builder.build()
            return mock.MagicMock(
                view_query=view.view_query,
                schema=schema,
                clustering_fields=None,
                time_partitioning=None,
            )

        # Create/Update returns the table that was already there
        def mock_create_or_update(
            view: BigQueryView, might_exist: bool  # pylint: disable=W0613
        ) -> bigquery.Table:
            return mock_get_table(view.address)

        self.mock_client.get_table = mock_get_table
        self.mock_client.create_or_update_view.side_effect = mock_create_or_update

        view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
            view_builders_to_update=mock_view_builders,
            historically_managed_datasets_to_clean=None,
            rematerialize_changed_views_only=True,
            failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
        )

        self.mock_client.create_dataset_if_necessary.assert_called_with(
            _DATASET_NAME, default_table_expiration_ms=None
        )
        self.mock_client.create_or_update_view.assert_has_calls(
            [
                mock.call(mock_view_builders[0].build(), might_exist=True),
                mock.call(mock_view_builders[1].build(), might_exist=True),
                mock.call(mock_view_builders[2].build(), might_exist=True),
            ],
            any_order=True,
        )

        # Materialize is not called!
        self.mock_client.materialize_view_to_table.assert_not_called()

        # Only delete calls should be from recreating views to have changes updating
        # in schema from the dag walker
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    BigQueryAddress(dataset_id=_DATASET_NAME, table_id="my_fake_view"),
                    not_found_ok=True,
                ),
                mock.call(
                    BigQueryAddress(
                        dataset_id=_DATASET_NAME, table_id="my_fake_view_2"
                    ),
                    not_found_ok=True,
                ),
                mock.call(
                    BigQueryAddress(
                        dataset_id=_DATASET_NAME, table_id="my_fake_view_3"
                    ),
                    not_found_ok=True,
                ),
            ],
            any_order=True,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.assertEqual(self.mock_client.delete_table.call_count, 3)

    def test_create_managed_dataset_and_deploy_views_for_view_builders_materialize_children(
        self,
    ) -> None:
        """Tests that we don't materialize any views if they have not been updated"""
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

        def mock_get_table(address: BigQueryAddress) -> bigquery.Table:
            schema = [bigquery.SchemaField("some_field", "STRING", "REQUIRED")]

            if address in {
                mock_view_builders[0].materialized_address,
                mock_view_builders[1].materialized_address,
                mock_view_builders[2].materialized_address,
            }:
                return mock.MagicMock(
                    dataset_id=address.dataset_id,
                    table_id=address.table_id,
                    schema=schema,
                )

            if mock_view_builders[0].view_id == address.table_id:
                view_builder = mock_view_builders[0]
            elif mock_view_builders[1].view_id == address.table_id:
                view_builder = mock_view_builders[1]
            elif mock_view_builders[2].view_id == address.table_id:
                view_builder = mock_view_builders[2]
            else:
                raise ValueError(f"Unexpected view id [{address.table_id}]")
            view = view_builder.build()
            if view.view_id != "my_fake_view_2":
                view_query = view.view_query
            else:
                # Old view query is different for this view!
                view_query = "SELECT 1 LIMIT 0"
            return mock.MagicMock(
                view_query=view_query,
                schema=schema,
            )

        # Create/Update returns the table that was already there
        def mock_create_or_update(
            view: BigQueryView, might_exist: bool  # pylint: disable=W0613
        ) -> bigquery.Table:
            table = mock_get_table(view.address)
            if view.view_id == "my_fake_view_2":
                table.view_query = mock_view_builders[1].build().view_query
            return table

        self.mock_client.get_table = mock_get_table
        self.mock_client.create_or_update_view.side_effect = mock_create_or_update

        view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
            view_builders_to_update=mock_view_builders,
            historically_managed_datasets_to_clean=None,
            rematerialize_changed_views_only=True,
            failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
        )

        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [
                mock.call(_DATASET_NAME, default_table_expiration_ms=None),
                mock.call(_DATASET_NAME_2, default_table_expiration_ms=None),
            ],
            any_order=True,
        )
        self.mock_client.create_or_update_view.assert_has_calls(
            [
                mock.call(mock_view_builders[0].build(), might_exist=True),
                mock.call(mock_view_builders[1].build(), might_exist=True),
                mock.call(mock_view_builders[2].build(), might_exist=True),
            ],
            any_order=True,
        )

        # Materialize is called where appropriate!
        self.mock_client.materialize_view_to_table.assert_has_calls(
            [
                # This view was updated
                mock.call(
                    view=mock_view_builders[1].build(),
                    use_query_cache=True,
                    view_configuration_changed=True,
                ),
                # Child view of updated view is also materialized
                mock.call(
                    view=mock_view_builders[2].build(),
                    use_query_cache=True,
                    view_configuration_changed=True,
                ),
            ],
            any_order=True,
        )
        # Only delete calls should be from recreating views to have changes updating in
        # schema from the dag walker
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    BigQueryAddress(dataset_id=_DATASET_NAME, table_id="my_fake_view"),
                    not_found_ok=True,
                ),
                mock.call(
                    BigQueryAddress(
                        dataset_id=_DATASET_NAME, table_id="my_fake_view_2"
                    ),
                    not_found_ok=True,
                ),
                mock.call(
                    BigQueryAddress(
                        dataset_id=_DATASET_NAME, table_id="my_fake_view_3"
                    ),
                    not_found_ok=True,
                ),
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

        view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
            view_builders_to_update=mock_view_builders,
            historically_managed_datasets_to_clean=None,
            rematerialize_changed_views_only=True,
            failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
        )

        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [
                mock.call(_DATASET_NAME, default_table_expiration_ms=None),
                mock.call(_DATASET_NAME_2, default_table_expiration_ms=None),
            ],
            any_order=True,
        )
        self.mock_client.create_or_update_view.assert_has_calls(
            [
                mock.call(mock_view_builders[0].build(), might_exist=True),
                mock.call(mock_view_builders[1].build(), might_exist=True),
            ],
            any_order=True,
        )

        # Only delete calls should be from recreating views to have changes updating in
        # schema from the dag walker
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    BigQueryAddress(dataset_id=_DATASET_NAME, table_id="my_fake_view"),
                    not_found_ok=True,
                ),
                mock.call(
                    BigQueryAddress(
                        dataset_id=_DATASET_NAME_2, table_id="my_fake_view_2"
                    ),
                    not_found_ok=True,
                ),
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
        address_overrides."""
        materialized_dataset = "other_dataset"

        override_dataset_id = "test_prefix_" + _DATASET_NAME
        override_materialized_dataset_id = "test_prefix_" + materialized_dataset

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
                clustering_fields=None,
                time_partitioning=None,
                materialized_table_schema=None,
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
        )

        mock_view_builders += [materialized_view_builder]

        expected_sandbox_context = BigQueryViewSandboxContext(
            parent_address_overrides=address_overrides_for_view_builders(
                view_dataset_override_prefix="test_prefix",
                view_builders=mock_view_builders,
            ),
            parent_address_formatter_provider=None,
            output_sandbox_dataset_prefix="test_prefix",
            state_code_filter=None,
        )

        view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
            view_builders_to_update=mock_view_builders,
            view_update_sandbox_context=BigQueryViewUpdateSandboxContext(
                output_sandbox_dataset_prefix="test_prefix",
                input_source_table_overrides=BigQueryAddressOverrides.empty(),
                parent_address_formatter_provider=None,
                state_code_filter=None,
            ),
            historically_managed_datasets_to_clean=None,
            rematerialize_changed_views_only=True,
            failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
        )

        dataset_ids = [override_dataset_id, override_materialized_dataset_id]
        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [
                mock.call(
                    dataset_id,
                    default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
                )
                for dataset_id in dataset_ids
            ],
            any_order=True,
        )
        self.mock_client.create_or_update_view.assert_has_calls(
            [
                mock.call(
                    view_builder.build(sandbox_context=expected_sandbox_context),
                    might_exist=True,
                )
                for view_builder in mock_view_builders
            ],
            any_order=True,
        )
        self.mock_client.materialize_view_to_table.assert_has_calls(
            [
                mock.call(
                    view=materialized_view_builder.build(
                        sandbox_context=expected_sandbox_context
                    ),
                    use_query_cache=True,
                    view_configuration_changed=True,
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
                mock.call(
                    BigQueryAddress(
                        dataset_id="test_prefix_" + _DATASET_NAME,
                        table_id="my_fake_view",
                    ),
                    not_found_ok=True,
                ),
                mock.call(
                    BigQueryAddress(
                        dataset_id="test_prefix_" + _DATASET_NAME,
                        table_id="my_other_fake_view",
                    ),
                    not_found_ok=True,
                ),
                mock.call(
                    BigQueryAddress(
                        dataset_id="test_prefix_" + _DATASET_NAME,
                        table_id="materialized_view",
                    ),
                    not_found_ok=True,
                ),
            ],
            any_order=True,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.assertEqual(self.mock_client.delete_table.call_count, 3)

    def test_create_dataset_and_update_views(self) -> None:
        """Test that create_dataset_and_update_views creates a dataset if necessary, and updates all views."""
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
                view_id=view["view_id"],
                description=f"{view['view_id']} description",
                view_query_template=view["view_query_template"],
                bq_description=f"{view['view_id']} description",
                materialized_address=None,
                clustering_fields=None,
                sandbox_context=None,
            )
            for view in sample_views
        ]

        # pylint: disable=protected-access
        view_update_manager._create_managed_dataset_and_deploy_views(
            views_to_update=mock_views,
            bq_region_override="us-east1",
            rematerialize_changed_views_only=True,
            historically_managed_datasets_to_clean=None,
            default_table_expiration_for_new_datasets=None,
            views_might_exist=True,
            allow_slow_views=False,
            failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
        )

        self.mock_client_constructor.assert_called_with(region_override="us-east1")
        self.mock_client.create_dataset_if_necessary.assert_called_with(
            _DATASET_NAME, default_table_expiration_ms=None
        )
        self.mock_client.create_or_update_view.assert_has_calls(
            [mock.call(view, might_exist=True) for view in mock_views], any_order=True
        )
        # Only delete calls should be from recreating views to have changes updating
        # in schema from the dag walker
        self.mock_client.delete_table.assert_has_calls(
            [
                mock.call(
                    BigQueryAddress(dataset_id=_DATASET_NAME, table_id="my_fake_view"),
                    not_found_ok=True,
                ),
                mock.call(
                    BigQueryAddress(
                        dataset_id=_DATASET_NAME, table_id="my_other_fake_view"
                    ),
                    not_found_ok=True,
                ),
            ],
            any_order=True,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.assertEqual(self.mock_client.delete_table.call_count, 2)

    def test_no_duplicate_views_in_update_list(self) -> None:
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
                self.view_source_table_datasets,
                f"Found view [{view_builder.view_id}] in source-table-only "
                f"dataset [{view_builder.dataset_id}]",
            )

    def test_all_cross_project_views_in_cross_project_view_builders(self) -> None:
        """Tests that all views that query from both production and staging
        environments are only deployed to production."""
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
        historically_managed_datasets = {_DATASET_NAME, _DATASET_NAME_2}

        mock_view_builders = [
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME,
                view_id="my_fake_view",
                description="my_fake_view description",
                view_query_template="a",
                should_materialize=False,
                materialized_address_override=None,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id=_DATASET_NAME_2,
                view_id="my_other_fake_view",
                description="my_other_fake_view description",
                view_query_template="a",
                should_materialize=False,
                materialized_address_override=None,
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

        def mock_list_tables(dataset_id: str) -> list[bigquery.table.TableListItem]:
            if dataset_id == _DATASET_NAME:
                return [
                    bigquery.table.TableListItem(mock_table_resource_ds_1_table),
                    bigquery.table.TableListItem(mock_table_resource_ds_1_table_bogus),
                ]
            if dataset_id == _DATASET_NAME_2:
                return [
                    bigquery.table.TableListItem(mock_table_resource_ds_2_table),
                    bigquery.table.TableListItem(mock_table_resource_ds_2_table_bogus),
                ]
            raise ValueError(f"No tables for id: {dataset_id}")

        self.mock_client.list_tables.side_effect = mock_list_tables
        self.mock_client.dataset_exists.return_value = True

        view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
            view_builders_to_update=mock_view_builders,
            historically_managed_datasets_to_clean=historically_managed_datasets,
            rematerialize_changed_views_only=True,
            failure_mode=BigQueryViewDagWalkerProcessingFailureMode.FAIL_EXHAUSTIVELY,
        )
        self.mock_client.delete_dataset.assert_not_called()
        self.mock_client.list_tables.assert_called()
        self.mock_client.delete_table.assert_has_calls(
            [
                call(
                    BigQueryAddress(dataset_id=_DATASET_NAME, table_id="my_fake_view"),
                    not_found_ok=True,
                ),
                call(
                    BigQueryAddress(
                        dataset_id=_DATASET_NAME_2, table_id="my_other_fake_view"
                    ),
                    not_found_ok=True,
                ),
                # above two calls are from deleting and recreating every view from
                # _create_or_update_view_and_materialize_if_necessary()
                call(
                    BigQueryAddress(dataset_id=_DATASET_NAME, table_id="bogus_view_1")
                ),
                call(
                    BigQueryAddress(dataset_id=_DATASET_NAME_2, table_id="bogus_view_2")
                ),
                # these two calls are from the actual cleaning up of unmanaged views
            ],
            any_order=True,
        )
        self.assertEqual(self.mock_client.delete_table.call_count, 4)
        self.assertEqual(self.mock_client.create_dataset_if_necessary.call_count, 2)
        self.mock_client.create_or_update_view.assert_has_calls(
            [
                mock.call(view_builder.build(), might_exist=True)
                for view_builder in mock_view_builders
            ],
            any_order=True,
        )

    def test_all_deployed_datasets_registered_as_managed(self) -> None:
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

    def test_no_source_table_datasets_registered_as_managed(self) -> None:
        for source_table_dataset_id in self.view_source_table_datasets:
            self.assertNotIn(
                source_table_dataset_id,
                DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
                f"Source table {source_table_dataset_id} should not be listed in "
                f"DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED, which should only "
                f"contain datasets that currently hold or once held managed views.",
            )
