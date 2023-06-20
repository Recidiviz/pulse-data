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

from recidiviz.persistence.database.bq_refresh.big_query_table_manager import (
    update_bq_dataset_to_match_sqlalchemy_schema,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.schema_type import SchemaType
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
    schema_type: SchemaType, export_config: CloudSqlToBQConfig
) -> None:
    interactive_prompt_retry_on_exception(
        fn=lambda: update_bq_dataset_to_match_sqlalchemy_schema(
            schema_type=schema_type,
            dataset_id=export_config.unioned_multi_region_dataset(
                dataset_override_prefix=None
            ),
        ),
        input_text="failed while updating big query table schemas - retry?",
        accepted_response_override="yes",
        exit_on_cancel=False,
    )


def update_cloud_sql_bq_refresh_output_schemas() -> None:
    for s in SchemaType:
        if CloudSqlToBQConfig.is_valid_schema_type(s):
            _update_cloud_sql_bq_refresh_output_schema(
                s, CloudSqlToBQConfig.for_schema_type(s)
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
