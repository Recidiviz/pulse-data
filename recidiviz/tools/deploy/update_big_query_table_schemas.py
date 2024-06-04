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
"""
Update BigQuery table schemas during deployment
"""
import argparse
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE, BigQueryClient
from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.persistence.database.bq_refresh.big_query_table_manager import (
    update_bq_dataset_to_match_sqlalchemy_schema_for_one_table,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import get_all_table_classes_in_schema
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.source_table_config import RawDataSourceTableLabel
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.source_tables.source_table_update_manager import (
    SourceTableCollectionUpdateConfig,
    SourceTableUpdateManager,
)
from recidiviz.tools.deploy.logging import get_deploy_logs_dir, redirect_logging_to_file
from recidiviz.tools.deploy.update_dataflow_output_table_manager_schemas import (
    update_dataflow_output_schemas,
)
from recidiviz.tools.utils.script_helpers import (
    interactive_loop_until_tasks_succeed,
    interactive_prompt_retry_on_exception,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.future_executor import map_fn_with_progress_bar_results
from recidiviz.utils.metadata import local_project_id_override


def update_all_cloud_sql_bq_refresh_output_schemas(
    file_kwargs: List[Dict[str, Any]], log_path: str
) -> None:
    """Update schemas for bq_refresh datasets based on given kwargs."""

    def update_bq_refresh_tables_with_redirect(
        file_kwargs: List[Dict[str, Any]], log_path: str
    ) -> Tuple[
        List[Tuple[Any, Dict[str, Any]]], List[Tuple[Exception, Dict[str, Any]]]
    ]:
        with redirect_logging_to_file(log_path):
            return map_fn_with_progress_bar_results(
                fn=update_bq_dataset_to_match_sqlalchemy_schema_for_one_table,
                kwargs_list=file_kwargs,
                max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
                timeout=60 * 10,
                progress_bar_message="Updating bq_refresh table schemas...",
            )

    interactive_loop_until_tasks_succeed(
        tasks_fn=lambda tasks_kwargs: update_bq_refresh_tables_with_redirect(
            file_kwargs=tasks_kwargs, log_path=log_path
        ),
        tasks_kwargs=file_kwargs,
    )


def update_cloud_sql_bq_refresh_output_schemas(
    dataset_override_prefix: Optional[str] = None,
) -> None:
    """Update all schemas for bq_refresh datasets in a parallelized way."""
    export_configs: List[CloudSqlToBQConfig] = [
        CloudSqlToBQConfig.for_schema_type(s)
        for s in SchemaType
        if CloudSqlToBQConfig.is_valid_schema_type(s)
    ]

    file_kwargs = [
        {
            "schema_type": export_config.schema_type,
            "dataset_id": export_config.multi_region_dataset(
                dataset_override_prefix=dataset_override_prefix
            ),
            "table": table,
            "default_table_expiration_ms": TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
            if dataset_override_prefix
            else None,
            "bq_region_override": None,
        }
        for export_config in export_configs
        for table in export_config.get_tables_to_export()
    ] + [
        {
            "schema_type": SchemaType.STATE,
            "dataset_id": STATE_BASE_DATASET,
            "table": table,
            "default_table_expiration_ms": TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
            if dataset_override_prefix
            else None,
            "bq_region_override": None,
        }
        for table in list(get_all_table_classes_in_schema(SchemaType.STATE))
    ]

    update_all_cloud_sql_bq_refresh_output_schemas(
        file_kwargs,
        log_path=os.path.join(
            get_deploy_logs_dir(), "update_cloud_sql_bq_refresh_outputs.log"
        ),
    )


def update_all_source_table_schemas(
    source_table_repository: SourceTableRepository,
    client: BigQueryClient | None = None,
) -> None:
    update_manager = SourceTableUpdateManager(client=client)

    # TODO(#30353): Add ingest view output table configs to this list
    # TODO(#30354): Add dataflow output table configs to this list
    # TODO(#30355): Add cloud sql refresh output configs to this list
    # TODO(#30356): Add static yaml tables to this list
    update_configs = [
        SourceTableCollectionUpdateConfig(
            source_table_collection=source_table_collection,
            allow_field_deletions=False,
        )
        for source_table_collection in source_table_repository.collections_labelled_with(
            RawDataSourceTableLabel
        )
    ]

    interactive_prompt_retry_on_exception(
        input_text="Exception encountered when updating source table schemas - retry?",
        accepted_response_override="yes",
        exit_on_cancel=True,
        fn=lambda: update_manager.update_async(
            update_configs=update_configs,
            log_file=os.path.join(
                get_deploy_logs_dir(), "update_all_source_table_schemas.log"
            ),
        ),
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    project_id = parser.parse_args().project_id
    with local_project_id_override(project_id):
        repository = build_source_table_repository_for_collected_schemata()

        update_all_source_table_schemas(source_table_repository=repository)
        update_cloud_sql_bq_refresh_output_schemas()
        update_dataflow_output_schemas()
