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

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_row_streamer import BigQueryRowStreamer
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.view_update_manager import (
    BigQueryViewUpdateSandboxContext,
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

# Table that holds information about update all jobs
VIEW_UPDATE_TRACKER_TABLE_ADDRESS = BigQueryAddress(
    dataset_id=VIEW_UPDATE_METADATA_DATASET, table_id="view_update_tracker"
)


class AllViewsUpdateSuccessPersister(BigQueryRowStreamer):
    """Class that persists runtime of successful updated view jobs to BQ"""

    def __init__(self, bq_client: BigQueryClient):
        source_table_repository = build_source_table_repository_for_yaml_managed_tables(
            metadata.project_id()
        )
        source_table_config = source_table_repository.build_config(
            VIEW_UPDATE_TRACKER_TABLE_ADDRESS
        )
        super().__init__(
            bq_client, source_table_config.address, source_table_config.schema_fields
        )

    def record_success_in_bq(
        self,
        deployed_builders: list[BigQueryViewBuilder],
        dataset_override_prefix: str | None,
        runtime_sec: int,
    ) -> None:
        num_deployed_views = len(deployed_builders)

        success_row = {
            "success_timestamp": datetime.datetime.now(tz=pytz.UTC).isoformat(),
            "dataset_override_prefix": dataset_override_prefix,
            "num_deployed_views": num_deployed_views,
            "view_update_runtime_sec": runtime_sec,
        }

        self.stream_rows([success_row])


@gcp_only
def execute_update_all_managed_views(sandbox_prefix: str | None) -> None:
    """
    Updates all views in the view registry. If sandbox_prefix is provided, all views will be deployed to a sandbox
    dataset.
    """
    start = datetime.datetime.now()
    view_builders = deployed_view_builders()

    view_update_sandbox_context = None
    if sandbox_prefix:
        view_update_sandbox_context = BigQueryViewUpdateSandboxContext(
            output_sandbox_dataset_prefix=sandbox_prefix,
            input_source_table_overrides=BigQueryAddressOverrides.empty(),
            parent_address_formatter_provider=None,
        )

    # TODO(#34767): Return summary and per-view results from here so we can write to BQ.
    create_managed_dataset_and_deploy_views_for_view_builders(
        view_source_table_datasets=get_source_table_datasets(metadata.project_id()),
        view_builders_to_update=view_builders,
        historically_managed_datasets_to_clean=DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
        view_update_sandbox_context=view_update_sandbox_context,
        materialize_changed_views_only=False,
        allow_slow_views=False,
    )
    end = datetime.datetime.now()
    runtime_sec = int((end - start).total_seconds())

    success_persister = AllViewsUpdateSuccessPersister(bq_client=BigQueryClientImpl())
    success_persister.record_success_in_bq(
        deployed_builders=view_builders,
        dataset_override_prefix=sandbox_prefix,
        runtime_sec=runtime_sec,
    )
    # TODO(#34767): Write per-view stats to view_update_tracker.per_view_update_stats
    #  here.
    logging.info("All managed views successfully updated and materialized.")
