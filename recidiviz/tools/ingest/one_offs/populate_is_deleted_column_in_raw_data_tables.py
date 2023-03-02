#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Temporary local script to set `is_deleted` to False for all raw data tables in a given project.

    Usage:
         python -m recidiviz.tools.ingest.one_offs.populate_is_deleted_column_in_raw_data_tables --dry-run True --project-id=recidiviz-staging
"""
import argparse
import logging
from typing import List, Optional

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def _set_raw_data_table_is_deleted_default_value_query_job(
    bq_client: BigQueryClientImpl,
    project_id: str,
    dataset_id: str,
    table_name: str,
    dry_run: bool,
) -> Optional[bigquery.QueryJob]:
    """Create and run query job for setting `is_deleted` to false in a specific raw data table."""
    logging.info(
        "\t\t\tCreating and running query job to set `is_deleted` to False for `%s.%s.%s`",
        project_id,
        dataset_id,
        table_name,
    )
    update_query = f"UPDATE `{project_id}.{dataset_id}.{table_name}` SET is_deleted = False WHERE true"
    if not dry_run:
        query_job = bq_client.run_query_async(
            query_str=update_query, use_query_cache=False
        )
        return query_job
    logging.info("\t\t\tWould create query job and run via `run_query_async`.")
    return None


def _get_raw_file_tags_state(
    state_code: StateCode,
) -> List[str]:
    """Retrieve the raw files tags in a state."""
    region_config = DirectIngestRegionRawFileConfig(region_code=state_code.value)
    return list(region_config.raw_file_tags)


def _iterate_through_raw_data_tables_in_state(
    bq_client: BigQueryClientImpl, dry_run: bool, project_id: str, state_code: StateCode
) -> None:
    """Iterate through raw data tables in each instance and generate and run query jobs to set `is_deleted` to False."""
    raw_file_tags = _get_raw_file_tags_state(state_code)

    logging.info(
        "Starting iteration through raw file tags and instances in %s", state_code.value
    )
    query_jobs: List[bigquery.QueryJob] = []
    for file_tag in raw_file_tags:
        for instance in DirectIngestInstance:
            dataset = raw_tables_dataset_for_region(state_code, instance)
            query_job: Optional[
                bigquery.QueryJob
            ] = _set_raw_data_table_is_deleted_default_value_query_job(
                bq_client=bq_client,
                dry_run=dry_run,
                project_id=project_id,
                dataset_id=dataset,
                table_name=file_tag,
            )
            if not dry_run and query_job:
                query_jobs.append(query_job)
    if dry_run:
        logging.info(
            "Would call wait_for_big_query_jobs to wait for query jobs in %s to finish.",
            state_code.value,
        )
    else:
        logging.info(
            "Created %s query jobs to add `is_deleted` for state=%s",
            len(query_jobs),
            state_code.value,
        )
        logging.info("Waiting for query jobs to complete...")
        bq_client.wait_for_big_query_jobs(query_jobs)
        logging.info("Query jobs completed.")


def _set_is_deleted_for_raw_tables_in_project(
    bq_client: BigQueryClientImpl, dry_run: bool, project_id: str
) -> None:
    for state in get_direct_ingest_states_existing_in_env():
        _iterate_through_raw_data_tables_in_state(bq_client, dry_run, project_id, state)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode, only prints the operations it would perform.",
    )

    return parser


# TODO(#18954): Delete this script once the `is_deleted` column is fully populated.
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()

    with local_project_id_override(args.project_id):
        client = BigQueryClientImpl()
        _set_is_deleted_for_raw_tables_in_project(client, args.dry_run, args.project_id)
