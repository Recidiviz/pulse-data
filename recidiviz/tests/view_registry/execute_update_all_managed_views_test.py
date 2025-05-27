# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for execute_update_all_managed_views.py"""
import datetime
import unittest
from unittest import mock
from unittest.mock import MagicMock, create_autospec, patch

import pytz
from google.cloud import bigquery
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryViewMaterializationResult
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalker,
    ProcessDagResult,
    ViewProcessingMetadata,
)
from recidiviz.big_query.view_update_manager import (
    CreateOrUpdateViewResult,
    CreateOrUpdateViewStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.test_setup_utils import BQ_EMULATOR_PROJECT_ID
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCPEnvironment
from recidiviz.view_registry.execute_update_all_managed_views import (
    AllViewsUpdateSuccessPersister,
    PerViewUpdateStatsPersister,
    execute_update_all_managed_views,
)
from recidiviz.view_registry.per_view_update_stats import PerViewUpdateStats


class TestAllViewsUpdateSuccessPersister(BigQueryEmulatorTestCase):
    """Tests for AllViewsUpdateSuccessPersister"""

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        return build_source_table_repository_for_yaml_managed_tables(
            BQ_EMULATOR_PROJECT_ID
        ).source_table_collections

    def test_persist(self) -> None:
        persister = AllViewsUpdateSuccessPersister(bq_client=self.bq_client)

        # Just shouldn't crash
        persister.record_success_in_bq(
            success_datetime=datetime.datetime.now(tz=pytz.UTC),
            num_deployed_views=0,
            dataset_override_prefix=None,
            runtime_sec=100,
            num_edges=10,
            num_distinct_paths=10,
        )


class TestPerViewUpdateStatsPersister(BigQueryEmulatorTestCase):
    """Tests for PerViewUpdateStatsPersister"""

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        return build_source_table_repository_for_yaml_managed_tables(
            BQ_EMULATOR_PROJECT_ID
        ).source_table_collections

    def test_persist(self) -> None:
        view = BigQueryView(
            dataset_id="view_dataset",
            view_id="my_view",
            description="my_view description",
            bq_description="my_view description",
            view_query_template="SELECT * FROM `{project_id}.some_dataset.table`",
        )
        view_2 = BigQueryView(
            dataset_id="view_dataset",
            view_id="my_view_2",
            description="my_view_2 description",
            bq_description="my_view_2 description",
            view_query_template="SELECT * FROM `{project_id}.some_dataset.table`",
        )

        mock_table = create_autospec(bigquery.Table)
        mock_table.num_rows = 100
        mock_table.num_bytes = 3000

        mock_query_job = create_autospec(bigquery.QueryJob)
        mock_query_job.slot_millis = 123
        mock_query_job.total_bytes_processed = 4500
        mock_query_job.total_bytes_billed = 4000
        mock_query_job.job_id = "job_123"

        persister = PerViewUpdateStatsPersister(bq_client=self.bq_client)

        update_stats = [
            PerViewUpdateStats(
                success_datetime=datetime.datetime(2024, 1, 1, tzinfo=pytz.UTC),
                create_or_update_result=CreateOrUpdateViewResult(
                    view=view,
                    status=CreateOrUpdateViewStatus.SUCCESS_WITH_CHANGES,
                    materialization_result=None,
                ),
                view_processing_metadata=ViewProcessingMetadata(
                    view_processing_runtime_sec=15.5,
                    total_node_processing_time_sec=16.3,
                    graph_depth=0,
                    longest_path=[view],
                    longest_path_runtime_seconds=16.3,
                    distinct_paths_to_view=10,
                ),
                ancestor_view_addresses=set(),
                state_code_literal_references=set(),
                parent_addresses=[BigQueryAddress.from_str("some_dataset.table")],
                complexity_score_2025=1,
                composite_complexity_score_2025=1,
                post_infra_library_composite_complexity_score_2025=1,
                referenced_raw_data_tables=[],
                is_leaf_node=True,
            ),
            PerViewUpdateStats(
                success_datetime=datetime.datetime(2024, 1, 1, tzinfo=pytz.UTC),
                create_or_update_result=CreateOrUpdateViewResult(
                    view=view_2,
                    status=CreateOrUpdateViewStatus.SUCCESS_WITH_CHANGES,
                    materialization_result=BigQueryViewMaterializationResult(
                        view_address=view_2.address,
                        materialized_table=mock_table,
                        completed_materialization_job=mock_query_job,
                    ),
                ),
                view_processing_metadata=ViewProcessingMetadata(
                    view_processing_runtime_sec=15.5,
                    total_node_processing_time_sec=16.3,
                    graph_depth=2,
                    longest_path=[view_2],
                    longest_path_runtime_seconds=16.3,
                    distinct_paths_to_view=10,
                ),
                ancestor_view_addresses={
                    BigQueryAddress.from_str("some_dataset.raw_latest_view"),
                    BigQueryAddress.from_str("some_dataset.another_view"),
                },
                state_code_literal_references={StateCode.US_XX, StateCode.US_YY},
                parent_addresses=[
                    BigQueryAddress.from_str("some_dataset.raw_latest_view")
                ],
                complexity_score_2025=1,
                composite_complexity_score_2025=10,
                post_infra_library_composite_complexity_score_2025=5,
                referenced_raw_data_tables=[
                    BigQueryAddress.from_str("some_dataset.raw_latest_view")
                ],
                is_leaf_node=False,
            ),
        ]

        persister.record_success_in_bq(view_update_results=update_stats)

        # Confirm we persisted 2 rows
        self.assertEqual(
            2,
            one(
                self.bq_client.run_query_async(
                    query_str=f"SELECT COUNT(*) AS cnt "
                    f"FROM `{BQ_EMULATOR_PROJECT_ID}.view_update_metadata.per_view_update_stats`;",
                    use_query_cache=False,
                ).result()
            )["cnt"],
        )


class TestExecuteUpdateAllManagedViews(unittest.TestCase):
    """Tests the execute_update_all_managed_views function."""

    def setUp(self) -> None:
        self.all_views_update_success_persister_patcher = patch(
            "recidiviz.view_registry.execute_update_all_managed_views.AllViewsUpdateSuccessPersister"
        )
        self.all_views_update_success_persister_constructor = (
            self.all_views_update_success_persister_patcher.start()
        )
        self.mock_all_views_update_success_persister = (
            self.all_views_update_success_persister_constructor.return_value
        )

        self.per_view_update_success_persister_patcher = patch(
            "recidiviz.view_registry.execute_update_all_managed_views.PerViewUpdateStatsPersister"
        )
        self.per_view_update_success_persister_patcher = (
            self.per_view_update_success_persister_patcher.start()
        )
        self.mock_per_view_update_success_persister = (
            self.per_view_update_success_persister_patcher.return_value
        )

        self.environment_patcher = mock.patch(
            "recidiviz.utils.environment.get_gcp_environment",
            return_value=GCPEnvironment.PRODUCTION.value,
        )
        self.environment_patcher.start()

        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = GCP_PROJECT_PRODUCTION

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.all_views_update_success_persister_patcher.stop()
        self.per_view_update_success_persister_patcher.stop()
        self.project_id_patcher.stop()

    @mock.patch(
        "recidiviz.view_registry.execute_update_all_managed_views.deployed_view_builders",
    )
    @mock.patch(
        "recidiviz.view_registry.execute_update_all_managed_views.BigQueryClientImpl"
    )
    @mock.patch(
        "recidiviz.view_registry.execute_update_all_managed_views.create_managed_dataset_and_deploy_views_for_view_builders"
    )
    def test_execute_update_all_managed_views(
        self,
        mock_create: MagicMock,
        _mock_bq_client: MagicMock,
        _mock_view_builders: MagicMock,
    ) -> None:
        mock_create.return_value = (
            ProcessDagResult(
                view_results={},
                view_processing_stats={},
                total_runtime=0,
                leaf_nodes=set(),
            ),
            BigQueryViewDagWalker(views=[]),
        )
        execute_update_all_managed_views(sandbox_prefix=None)
        mock_create.assert_called()
        self.mock_all_views_update_success_persister.record_success_in_bq.assert_called_with(
            success_datetime=mock.ANY,
            num_deployed_views=mock.ANY,
            dataset_override_prefix=None,
            runtime_sec=mock.ANY,
            num_edges=mock.ANY,
            num_distinct_paths=mock.ANY,
        )
        self.mock_per_view_update_success_persister.record_success_in_bq.assert_called()

    @mock.patch(
        "recidiviz.view_registry.execute_update_all_managed_views.deployed_view_builders",
    )
    @mock.patch(
        "recidiviz.view_registry.execute_update_all_managed_views.BigQueryClientImpl"
    )
    @mock.patch(
        "recidiviz.view_registry.execute_update_all_managed_views.create_managed_dataset_and_deploy_views_for_view_builders"
    )
    def test_execute_update_all_managed_views_with_sandbox_prefix(
        self,
        mock_create: MagicMock,
        _mock_bq_client: MagicMock,
        _mock_view_builders: MagicMock,
    ) -> None:
        mock_create.return_value = (
            ProcessDagResult(
                view_results={},
                view_processing_stats={},
                total_runtime=0,
                leaf_nodes=set(),
            ),
            BigQueryViewDagWalker(views=[]),
        )
        execute_update_all_managed_views(sandbox_prefix="test_prefix")
        mock_create.assert_called()
        self.mock_all_views_update_success_persister.record_success_in_bq.assert_called_with(
            success_datetime=mock.ANY,
            num_deployed_views=mock.ANY,
            dataset_override_prefix="test_prefix",
            runtime_sec=mock.ANY,
            num_edges=mock.ANY,
            num_distinct_paths=mock.ANY,
        )
        self.mock_per_view_update_success_persister.record_success_in_bq.assert_called()
