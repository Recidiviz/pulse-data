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
"""Tests per_view_update_stats.py"""
import datetime
import unittest
from typing import Any
from unittest.mock import create_autospec, patch

import pytz
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryViewMaterializationResult
from recidiviz.big_query.big_query_view import (
    BigQueryView,
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalker,
    ViewProcessingMetadata,
)
from recidiviz.big_query.view_update_manager import (
    CreateOrUpdateViewResult,
    CreateOrUpdateViewStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.source_tables.source_table_config import (
    RawDataSourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableConfig,
    SourceTableLabel,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)
from recidiviz.source_tables.yaml_managed.datasets import VIEW_UPDATE_METADATA_DATASET
from recidiviz.utils import metadata
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.address_to_complexity_score_mapping import (
    ParentAddressComplexityScoreMapper,
)
from recidiviz.view_registry.per_view_update_stats import (
    PerViewUpdateStats,
    per_view_update_stats_for_view_update_result,
)

# View builders for views forming a DAG with a diamond in it:
#  1     2
#   \   /
#     3    <-- this is a "library" view
#   /   \
#  4     5
#   \   /
#     6
_DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST: list[BigQueryViewBuilder[Any]] = [
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_1",
        view_id="table_1_us_yy",
        should_materialize=True,
        description="table_1 description",
        # Query complexity: 1
        view_query_template="SELECT *, a + b AS c FROM `{project_id}.source_dataset.source_table`",
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_2",
        view_id="table_2_us_xx",
        should_materialize=False,
        description="table_2 description",
        # Query complexity: 11
        view_query_template="SELECT * FROM `{project_id}.us_xx_raw_data.raw_table` WHERE state_code = 'US_XX'",
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_3",
        view_id="table_3",
        should_materialize=True,
        description="table_3 description",
        # Query complexity: 20
        view_query_template="""
            SELECT * 
            FROM (
                SELECT * FROM `{project_id}.dataset_1.table_1_us_yy_materialized`
                WHERE state_code = 'US_YY'
            )
            JOIN (
                SELECT * FROM `{project_id}.dataset_2.table_2_us_xx`
                WHERE state_code = 'US_XX'
            )
            USING (col)""",
        clustering_fields=["foo", "bar"],
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_4",
        view_id="table_4",
        should_materialize=True,
        description="table_4 description",
        # Query complexity: 1
        view_query_template="""
            SELECT *, c + d AS e FROM `{project_id}.dataset_3.table_3_materialized`""",
        time_partitioning=bigquery.TimePartitioning(
            field="partition_field", type_=bigquery.TimePartitioningType.DAY
        ),
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_5",
        view_id="table_5",
        should_materialize=True,
        description="table_5 description",
        # Query complexity: 4
        view_query_template="""
            SELECT d + f AS g, h + i AS j
            FROM `{project_id}.dataset_3.table_3_materialized`
            WHERE d < h
            """,
    ),
    SimpleBigQueryViewBuilder(
        dataset_id="dataset_6",
        view_id="table_6",
        should_materialize=True,
        description="table_6 description",
        # Query complexity: 2
        view_query_template="""
        SELECT * FROM `{project_id}.dataset_4.table_4_materialized`
        JOIN `{project_id}.dataset_5.table_5_materialized`
        USING (col)""",
    ),
]


class TestPerViewUpdateStats(unittest.TestCase):
    """Tests for the PerViewUpdateStats class"""

    def setUp(self) -> None:
        self.platform_version_patcher = patch(
            "recidiviz.utils.environment.get_data_platform_version"
        )
        self.mock_platform_version_fn = self.platform_version_patcher.start()
        self.mock_platform_version_fn.return_value = "v1.123.0-alpha.0"

        with local_project_id_override("recidiviz-456"):
            source_table_repository = (
                build_source_table_repository_for_yaml_managed_tables(
                    metadata.project_id()
                )
            )
            self.source_table_config = source_table_repository.get_config(
                BigQueryAddress(
                    dataset_id=VIEW_UPDATE_METADATA_DATASET,
                    table_id="per_view_update_stats",
                )
            )
            self.view = BigQueryView(
                dataset_id="view_dataset",
                view_id="my_view",
                description="my_view description",
                bq_description="my_view description",
                view_query_template="SELECT * FROM `{project_id}.some_dataset.table`",
            )

    def tearDown(self) -> None:
        self.platform_version_patcher.stop()

    def test_as_table_row_mostly_empty(self) -> None:
        stats = PerViewUpdateStats(
            success_datetime=datetime.datetime(2024, 1, 1, tzinfo=pytz.UTC),
            create_or_update_result=CreateOrUpdateViewResult(
                view=self.view,
                updated_view=None,
                status=CreateOrUpdateViewStatus.SUCCESS_WITH_CHANGES,
                materialization_result=None,
            ),
            view_processing_metadata=ViewProcessingMetadata(
                view_processing_runtime_sec=15.5,
                total_node_processing_time_sec=16.3,
                graph_depth=0,
                longest_path=[self.view],
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
            is_leaf_node=False,
        )

        expected_column_names = set(self.source_table_config.column_names)
        table_row = stats.as_table_row()

        # Dictionary keys should match expected table column names
        self.assertEqual(expected_column_names, set(table_row.keys()))
        expected_table_row = {
            "success_timestamp": "2024-01-01T00:00:00+00:00",
            "data_platform_version": "v1.123.0-alpha.0",
            "dataset_id": "view_dataset",
            "table_id": "my_view",
            "was_materialized": False,
            "update_runtime_sec": 15.5,
            "view_query_signature": "cdc996837491e7716d35d3c73be265c236f5f6c9ded04f88874ee3bc0718d707",
            "clustering_fields_string": None,
            "time_partitioning_string": None,
            "materialized_table_num_rows": None,
            "materialized_table_size_bytes": None,
            "slot_millis": None,
            "total_bytes_processed": None,
            "total_bytes_billed": None,
            "job_id": None,
            "graph_depth": 0,
            "num_ancestor_views": 0,
            "parent_addresses": ["some_dataset.table"],
            "complexity_score_2025": 1,
            "composite_complexity_score_2025": 1,
            "post_infra_library_composite_complexity_score_2025": 1,
            "has_state_specific_logic": False,
            "states_referenced": [],
            "state_code_specific_to_view": None,
            "referenced_raw_data_tables": [],
            "is_leaf_node": False,
        }
        self.assertEqual(expected_table_row, table_row)

    def test_as_table_row_more_complex(self) -> None:
        mock_table = create_autospec(bigquery.Table)
        mock_table.num_rows = 100
        mock_table.num_bytes = 3000

        mock_query_job = create_autospec(bigquery.QueryJob)
        mock_query_job.slot_millis = 123
        mock_query_job.total_bytes_processed = 4500
        mock_query_job.total_bytes_billed = 4000
        mock_query_job.job_id = "job_123"

        stats = PerViewUpdateStats(
            success_datetime=datetime.datetime(2024, 1, 1, tzinfo=pytz.UTC),
            create_or_update_result=CreateOrUpdateViewResult(
                view=self.view,
                updated_view=None,
                status=CreateOrUpdateViewStatus.SUCCESS_WITH_CHANGES,
                materialization_result=BigQueryViewMaterializationResult(
                    view_address=self.view.address,
                    materialized_table=mock_table,
                    completed_materialization_job=mock_query_job,
                ),
            ),
            view_processing_metadata=ViewProcessingMetadata(
                view_processing_runtime_sec=15.5,
                total_node_processing_time_sec=16.3,
                graph_depth=2,
                longest_path=[self.view],
                longest_path_runtime_seconds=16.3,
                distinct_paths_to_view=10,
            ),
            ancestor_view_addresses={
                BigQueryAddress.from_str("some_dataset.raw_latest_view"),
                BigQueryAddress.from_str("some_dataset.another_view"),
            },
            state_code_literal_references={StateCode.US_XX, StateCode.US_YY},
            parent_addresses=[BigQueryAddress.from_str("some_dataset.raw_latest_view")],
            complexity_score_2025=1,
            composite_complexity_score_2025=10,
            post_infra_library_composite_complexity_score_2025=5,
            referenced_raw_data_tables=[
                BigQueryAddress.from_str("some_dataset.raw_latest_view")
            ],
            is_leaf_node=False,
        )

        expected_column_names = set(self.source_table_config.column_names)
        table_row = stats.as_table_row()

        # Dictionary keys should match expected table column names
        self.assertEqual(expected_column_names, set(table_row.keys()))
        expected_table_row = {
            "success_timestamp": "2024-01-01T00:00:00+00:00",
            "data_platform_version": "v1.123.0-alpha.0",
            "dataset_id": "view_dataset",
            "table_id": "my_view",
            "was_materialized": True,
            "update_runtime_sec": 15.5,
            "view_query_signature": "cdc996837491e7716d35d3c73be265c236f5f6c9ded04f88874ee3bc0718d707",
            "clustering_fields_string": None,
            "time_partitioning_string": None,
            "materialized_table_num_rows": 100,
            "materialized_table_size_bytes": 3000,
            "slot_millis": 123,
            "total_bytes_processed": 4500,
            "total_bytes_billed": 4000,
            "job_id": "job_123",
            "graph_depth": 2,
            "num_ancestor_views": 2,
            "parent_addresses": ["some_dataset.raw_latest_view"],
            "complexity_score_2025": 1,
            "composite_complexity_score_2025": 10,
            "post_infra_library_composite_complexity_score_2025": 5,
            "has_state_specific_logic": True,
            "states_referenced": ["US_XX", "US_YY"],
            "state_code_specific_to_view": None,
            "referenced_raw_data_tables": ["some_dataset.raw_latest_view"],
            "is_leaf_node": False,
        }
        self.assertEqual(expected_table_row, table_row)


class TestBuildPerViewUpdateStats(unittest.TestCase):
    """Tests for the per_view_update_stats_for_view_update_result() helper"""

    @staticmethod
    def _source_table_collection_for_address(
        table_address: BigQueryAddress, labels: list[SourceTableLabel[Any]]
    ) -> SourceTableCollection:
        return SourceTableCollection(
            dataset_id=table_address.dataset_id,
            labels=labels,
            source_tables_by_address={
                table_address: SourceTableConfig(
                    address=table_address,
                    schema_fields=[SchemaField("col", "STRING", "NULLABLE")],
                    description=f"Description for {table_address.to_str()}",
                )
            },
            update_config=SourceTableCollectionUpdateConfig.protected(),
            description=f"Description for dataset {table_address.dataset_id}",
        )

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"

        self.diamond_shaped_dag_views_list = [
            b.build() for b in _DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST
        ]

        self.parent_complexity_mapper_patcher = patch(
            "recidiviz.view_registry.per_view_update_stats.ParentAddressComplexityScoreMapper"
        )

        self.parent_complexity_mapper_patcher.start().return_value = (
            ParentAddressComplexityScoreMapper(
                source_table_repository=SourceTableRepository(
                    source_table_collections=[
                        self._source_table_collection_for_address(
                            BigQueryAddress.from_str("source_dataset.source_table"),
                            labels=[],
                        ),
                        self._source_table_collection_for_address(
                            BigQueryAddress.from_str("us_xx_raw_data.raw_table"),
                            labels=[
                                RawDataSourceTableLabel(
                                    state_code=StateCode.US_XX,
                                    ingest_instance=DirectIngestInstance.PRIMARY,
                                )
                            ],
                        ),
                    ]
                ),
                all_view_builders=_DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST,
            )
        )

        self.is_library_view_patcher = patch(
            "recidiviz.view_registry.per_view_update_stats.is_view_part_of_infra_library_2025"
        )

        self.is_library_view_patcher.start().side_effect = (
            lambda view_address: view_address.dataset_id == "dataset_3"
        )

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.parent_complexity_mapper_patcher.stop()
        self.is_library_view_patcher.stop()

    def test_per_view_update_stats_for_view_update_result(self) -> None:
        all_views_dag_walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)

        def _fake_process_view(
            v: BigQueryView,
            _parent_results: dict[BigQueryView, CreateOrUpdateViewResult],
        ) -> CreateOrUpdateViewResult:
            view_index = self.diamond_shaped_dag_views_list.index(v)
            builder = _DIAMOND_SHAPED_DAG_VIEW_BUILDERS_LIST[view_index]

            mock_table = create_autospec(bigquery.Table)
            mock_table.num_rows = view_index * 100
            mock_table.num_bytes = view_index * 1000

            mock_query_job = create_autospec(bigquery.QueryJob)
            mock_query_job.slot_millis = view_index * 20
            mock_query_job.total_bytes_processed = view_index * 2000
            mock_query_job.total_bytes_billed = view_index * 1000
            mock_query_job.job_id = f"job_{view_index}"

            materialization_result = None
            if builder.materialized_address:
                materialization_result = BigQueryViewMaterializationResult(
                    view_address=v.address,
                    materialized_table=mock_table,
                    completed_materialization_job=mock_query_job,
                )

            return CreateOrUpdateViewResult(
                view=v,
                updated_view=None,
                status=CreateOrUpdateViewStatus.SUCCESS_WITH_CHANGES,
                materialization_result=materialization_result,
            )

        update_result = all_views_dag_walker.process_dag(
            _fake_process_view, synchronous=False
        )

        per_view_stats_list = per_view_update_stats_for_view_update_result(
            success_datetime=datetime.datetime(2024, 1, 1, tzinfo=pytz.UTC),
            view_update_dag_walker=all_views_dag_walker,
            update_views_result=update_result,
        )

        for s in per_view_stats_list:
            # This might vary across test runs, but should always be a float
            self.assertIsInstance(s.as_table_row()["update_runtime_sec"], float)
            # This might vary across test runs, but should always be a string
            self.assertIsInstance(s.as_table_row()["data_platform_version"], str)

        # We already tested these above
        keys_to_exclude_from_comparison = {
            "data_platform_version",
            "update_runtime_sec",
        }

        expected_output_rows = [
            {
                "success_timestamp": "2024-01-01T00:00:00+00:00",
                "dataset_id": "dataset_1",
                "table_id": "table_1_us_yy",
                "view_query_signature": "bb7e62b212044f5944712fcd5bb052c541d7019c0bd453bc07b13ceb57896667",
                "clustering_fields_string": None,
                "time_partitioning_string": None,
                "complexity_score_2025": 1,
                "composite_complexity_score_2025": 1,
                "post_infra_library_composite_complexity_score_2025": 1,
                "graph_depth": 0,
                "num_ancestor_views": 0,
                "parent_addresses": ["source_dataset.source_table"],
                "referenced_raw_data_tables": [],
                "has_state_specific_logic": False,
                "state_code_specific_to_view": "US_YY",
                "states_referenced": [],
                # Materialization info
                "was_materialized": True,
                "materialized_table_num_rows": 0,
                "materialized_table_size_bytes": 0,
                "slot_millis": 0,
                "total_bytes_billed": 0,
                "total_bytes_processed": 0,
                "job_id": "job_0",
                "is_leaf_node": False,
            },
            {
                "success_timestamp": "2024-01-01T00:00:00+00:00",
                "dataset_id": "dataset_2",
                "table_id": "table_2_us_xx",
                "view_query_signature": "07b207f7fcdea521507c0041cd25579fb5736ceb2d908bc2948662b6268402a0",
                "clustering_fields_string": None,
                "time_partitioning_string": None,
                "complexity_score_2025": 11,
                "composite_complexity_score_2025": 11,
                "post_infra_library_composite_complexity_score_2025": 11,
                "graph_depth": 0,
                "num_ancestor_views": 0,
                "parent_addresses": ["us_xx_raw_data.raw_table"],
                "referenced_raw_data_tables": [],
                "has_state_specific_logic": True,
                "state_code_specific_to_view": "US_XX",
                "states_referenced": ["US_XX"],
                # Materialization info
                "was_materialized": False,
                "materialized_table_num_rows": None,
                "materialized_table_size_bytes": None,
                "slot_millis": None,
                "total_bytes_billed": None,
                "total_bytes_processed": None,
                "job_id": None,
                "is_leaf_node": False,
            },
            {
                "success_timestamp": "2024-01-01T00:00:00+00:00",
                "dataset_id": "dataset_3",
                "table_id": "table_3",
                "view_query_signature": "dd3dc013a592a0c038d3cb8290e432ba162b25a9233cdc5a00f4c57ed3a6ba8c",
                "clustering_fields_string": "foo,bar",
                "time_partitioning_string": None,
                "complexity_score_2025": 20,
                "composite_complexity_score_2025": 32,
                "post_infra_library_composite_complexity_score_2025": 32,
                "graph_depth": 1,
                "num_ancestor_views": 2,
                "parent_addresses": [
                    "dataset_1.table_1_us_yy",
                    "dataset_2.table_2_us_xx",
                ],
                "referenced_raw_data_tables": [],
                "has_state_specific_logic": True,
                "state_code_specific_to_view": None,
                "states_referenced": ["US_XX", "US_YY"],
                # Materialization info
                "was_materialized": True,
                "materialized_table_num_rows": 200,
                "materialized_table_size_bytes": 2000,
                "slot_millis": 40,
                "total_bytes_billed": 2000,
                "total_bytes_processed": 4000,
                "job_id": "job_2",
                "is_leaf_node": False,
            },
            {
                "success_timestamp": "2024-01-01T00:00:00+00:00",
                "dataset_id": "dataset_4",
                "table_id": "table_4",
                "view_query_signature": "8434a01d21c51adca8af9f90a4efdb919c65f62b781cda17bce96dab0478283c",
                "clustering_fields_string": None,
                "time_partitioning_string": '{"field": "partition_field", "type": "DAY"}',
                "complexity_score_2025": 1,
                "composite_complexity_score_2025": 33,
                # Post-infra complexity score resets here because dataset_3 is a "library" dataset
                "post_infra_library_composite_complexity_score_2025": 1,
                "graph_depth": 2,
                "num_ancestor_views": 3,
                "parent_addresses": ["dataset_3.table_3"],
                "referenced_raw_data_tables": [],
                "has_state_specific_logic": False,
                "state_code_specific_to_view": None,
                "states_referenced": [],
                # Materialization info
                "was_materialized": True,
                "materialized_table_num_rows": 300,
                "materialized_table_size_bytes": 3000,
                "slot_millis": 60,
                "total_bytes_billed": 3000,
                "total_bytes_processed": 6000,
                "job_id": "job_3",
                "is_leaf_node": False,
            },
            {
                "success_timestamp": "2024-01-01T00:00:00+00:00",
                "dataset_id": "dataset_5",
                "table_id": "table_5",
                "view_query_signature": "520b874d0882be60a2b43250645ed04b63802667fbfd4b9d4bca8dcc02283ea7",
                "clustering_fields_string": None,
                "time_partitioning_string": None,
                "complexity_score_2025": 4,
                "composite_complexity_score_2025": 36,
                # Post-infra complexity score resets here because dataset_3 is a "library" dataset
                "post_infra_library_composite_complexity_score_2025": 4,
                "graph_depth": 2,
                "num_ancestor_views": 3,
                "parent_addresses": ["dataset_3.table_3"],
                "referenced_raw_data_tables": [],
                "has_state_specific_logic": False,
                "state_code_specific_to_view": None,
                "states_referenced": [],
                # Materialization info
                "was_materialized": True,
                "materialized_table_num_rows": 400,
                "materialized_table_size_bytes": 4000,
                "slot_millis": 80,
                "total_bytes_billed": 4000,
                "total_bytes_processed": 8000,
                "job_id": "job_4",
                "is_leaf_node": False,
            },
            {
                "success_timestamp": "2024-01-01T00:00:00+00:00",
                "dataset_id": "dataset_6",
                "table_id": "table_6",
                "view_query_signature": "5c73fee77db98c5f0709b78e6f4092e9354c4f3b7260686b5b015d1b89ce544a",
                "clustering_fields_string": None,
                "time_partitioning_string": None,
                "complexity_score_2025": 2,
                "composite_complexity_score_2025": 71,
                "post_infra_library_composite_complexity_score_2025": 7,
                "graph_depth": 3,
                "num_ancestor_views": 5,
                "parent_addresses": ["dataset_4.table_4", "dataset_5.table_5"],
                "referenced_raw_data_tables": [],
                "has_state_specific_logic": False,
                "state_code_specific_to_view": None,
                "states_referenced": [],
                # Materialization info
                "was_materialized": True,
                "materialized_table_num_rows": 500,
                "materialized_table_size_bytes": 5000,
                "slot_millis": 100,
                "total_bytes_billed": 5000,
                "total_bytes_processed": 10000,
                "job_id": "job_5",
                "is_leaf_node": True,
            },
        ]

        output_rows = [
            {
                k: v
                for k, v in s.as_table_row().items()
                if k not in keys_to_exclude_from_comparison
            }
            for s in sorted(per_view_stats_list, key=lambda s: s.view_address.to_str())
        ]
        self.assertEqual(expected_output_rows, output_rows)
