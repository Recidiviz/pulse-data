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
"""Utility for updating all views in the view registry."""
import datetime
import logging

import pytz

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_row_streamer import BigQueryRowStreamer
from recidiviz.big_query.view_update_manager import (
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_source_table_datasets,
)
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)
from recidiviz.source_tables.yaml_managed.datasets import VIEW_UPDATE_METADATA_DATASET
from recidiviz.utils import metadata
from recidiviz.utils.environment import gcp_only
from recidiviz.view_registry.deployed_views import (
    DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
    deployed_view_builders,
)
from recidiviz.view_registry.per_view_update_stats import (
    PerViewUpdateStats,
    per_view_update_stats_for_view_update_result,
)

# Table that holds job-level information about successful update_managed_views_all jobs
VIEW_UPDATE_TRACKER_TABLE_ADDRESS = BigQueryAddress(
    dataset_id=VIEW_UPDATE_METADATA_DATASET, table_id="view_update_tracker"
)

# Table that holds view-level information about successful update_managed_views_all jobs
PER_VIEW_UPDATE_STATS_TABLE_ADDRESS = BigQueryAddress(
    dataset_id=VIEW_UPDATE_METADATA_DATASET, table_id="per_view_update_stats"
)


class AllViewsUpdateSuccessPersister(BigQueryRowStreamer):
    """Class that persists runtime of successful updated view jobs to BQ"""

    def __init__(self, bq_client: BigQueryClient):
        source_table_repository = build_source_table_repository_for_yaml_managed_tables(
            metadata.project_id()
        )
        source_table_config = source_table_repository.get_config(
            VIEW_UPDATE_TRACKER_TABLE_ADDRESS
        )
        super().__init__(
            bq_client, source_table_config.address, source_table_config.schema_fields
        )

    def record_success_in_bq(
        self,
        *,
        success_datetime: datetime.datetime,
        num_deployed_views: int,
        dataset_override_prefix: str | None,
        runtime_sec: int,
        num_edges: int,
        num_distinct_paths: int,
    ) -> None:

        success_row = {
            "success_timestamp": success_datetime.isoformat(),
            "dataset_override_prefix": dataset_override_prefix,
            "num_deployed_views": num_deployed_views,
            "view_update_runtime_sec": runtime_sec,
            "num_edges": num_edges,
            "num_distinct_paths": num_distinct_paths,
        }

        self.stream_rows([success_row])


class PerViewUpdateStatsPersister(BigQueryRowStreamer):
    def __init__(
        self,
        bq_client: BigQueryClient,
    ):
        source_table_repository = build_source_table_repository_for_yaml_managed_tables(
            metadata.project_id()
        )
        source_table_config = source_table_repository.get_config(
            PER_VIEW_UPDATE_STATS_TABLE_ADDRESS
        )
        super().__init__(
            bq_client, source_table_config.address, source_table_config.schema_fields
        )

    def record_success_in_bq(
        self,
        view_update_results: list[PerViewUpdateStats],
    ) -> None:
        rows = [view_result.as_table_row() for view_result in view_update_results]

        self.stream_rows(rows)


@gcp_only
def execute_update_all_managed_views() -> None:
    """
    Updates all views in the view registry. If sandbox_prefix is provided, all views will be deployed to a sandbox
    dataset.
    """
    start_time = datetime.datetime.now(tz=pytz.UTC)
    view_builders = deployed_view_builders()

    (
        update_views_result,
        dag_walker,
    ) = create_managed_dataset_and_deploy_views_for_view_builders(
        view_source_table_datasets=get_source_table_datasets(metadata.project_id()),
        view_builders_to_update=view_builders,
        historically_managed_datasets_to_clean=DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
        view_update_sandbox_context=None,
        materialize_changed_views_only=False,
        allow_slow_views=False,
    )
    success_time = datetime.datetime.now(tz=pytz.UTC)
    runtime_sec = int((success_time - start_time).total_seconds())

    job_level_success_persister = AllViewsUpdateSuccessPersister(
        bq_client=BigQueryClientImpl()
    )
    job_level_success_persister.record_success_in_bq(
        num_deployed_views=len(view_builders),
        success_datetime=success_time,
        dataset_override_prefix=None,
        runtime_sec=runtime_sec,
        num_edges=dag_walker.get_number_of_edges(),
        num_distinct_paths=update_views_result.get_distinct_paths_to_leaf_nodes(),
    )
    view_level_success_persister = PerViewUpdateStatsPersister(
        bq_client=BigQueryClientImpl()
    )
    view_level_success_persister.record_success_in_bq(
        view_update_results=per_view_update_stats_for_view_update_result(
            success_datetime=success_time,
            update_views_result=update_views_result,
            view_update_dag_walker=dag_walker,
        ),
    )
    logging.info("All managed views successfully updated and materialized.")
