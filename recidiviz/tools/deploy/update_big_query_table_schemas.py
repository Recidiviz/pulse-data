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
from typing import Optional

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.persistence.database.bq_refresh.big_query_table_manager import (
    update_bq_dataset_to_match_sqlalchemy_schema,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.deploy.update_dataflow_output_table_manager_schemas import (
    update_dataflow_output_schemas,
)
from recidiviz.tools.deploy.update_raw_data_table_schemas import (
    update_all_raw_data_table_schemas,
)
from recidiviz.tools.utils.script_helpers import interactive_prompt_retry_on_exception
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def _update_cloud_sql_bq_refresh_output_schema(
    export_config: CloudSqlToBQConfig,
    dataset_override_prefix: Optional[str],
) -> None:

    interactive_prompt_retry_on_exception(
        fn=lambda: update_bq_dataset_to_match_sqlalchemy_schema(
            schema_type=export_config.schema_type,
            dataset_id=export_config.unioned_multi_region_dataset(
                dataset_override_prefix=dataset_override_prefix
            ),
            default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
            if dataset_override_prefix
            else None,
        ),
        input_text="failed while updating big query table schemas - retry?",
        accepted_response_override="yes",
        exit_on_cancel=False,
    )


def _update_bq_regional_datasets(
    export_config: CloudSqlToBQConfig,
    dataset_override_prefix: Optional[str],
) -> None:
    """Updates and creates BigQuery datasets for the regional tables for the given
    config."""
    # TODO(#7285): Migrate Justice Counts connection to be in same region as instance
    if export_config.schema_type == SchemaType.JUSTICE_COUNTS:
        bq_region_override = None
    else:
        bq_region_override = SQLAlchemyEngineManager.get_cloudsql_instance_region(
            export_config.schema_type
        )

    update_bq_dataset_to_match_sqlalchemy_schema(
        schema_type=export_config.schema_type,
        dataset_id=export_config.unioned_regional_dataset(
            dataset_override_prefix=dataset_override_prefix
        ),
        bq_region_override=bq_region_override,
        default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
        if dataset_override_prefix
        else None,
    )
    for state_code in get_direct_ingest_states_existing_in_env():
        materialized_dataset = export_config.materialized_dataset_for_segment(
            state_code=state_code,
        )
        if dataset_override_prefix:
            materialized_dataset = BigQueryAddressOverrides.format_sandbox_dataset(
                dataset_override_prefix, materialized_dataset
            )
        update_bq_dataset_to_match_sqlalchemy_schema(
            schema_type=export_config.schema_type,
            dataset_id=materialized_dataset,
            bq_region_override=bq_region_override,
            default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
            if dataset_override_prefix
            else None,
        )


def _update_cloud_sql_bq_refresh_output_schema_regional_datasets(
    export_config: CloudSqlToBQConfig,
    dataset_override_prefix: Optional[str],
) -> None:
    interactive_prompt_retry_on_exception(
        fn=lambda: _update_bq_regional_datasets(
            export_config=export_config,
            dataset_override_prefix=dataset_override_prefix,
        ),
        input_text="failed while updating big query table schemas - retry?",
        accepted_response_override="yes",
        exit_on_cancel=False,
    )


def update_cloud_sql_bq_refresh_output_schemas(
    dataset_override_prefix: Optional[str] = None,
) -> None:
    for s in SchemaType:
        if not CloudSqlToBQConfig.is_valid_schema_type(s):
            continue

        config = CloudSqlToBQConfig.for_schema_type(s)
        _update_cloud_sql_bq_refresh_output_schema(
            export_config=config,
            dataset_override_prefix=dataset_override_prefix,
        )
        if not config.is_state_segmented_refresh_schema():
            continue

        _update_cloud_sql_bq_refresh_output_schema_regional_datasets(
            export_config=config,
            dataset_override_prefix=dataset_override_prefix,
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
        update_all_raw_data_table_schemas()
        update_cloud_sql_bq_refresh_output_schemas()
        update_dataflow_output_schemas()
