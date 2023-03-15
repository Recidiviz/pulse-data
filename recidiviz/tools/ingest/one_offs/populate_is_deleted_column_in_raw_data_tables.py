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
import uuid
from typing import List, Optional, Tuple

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.calculator.cluster_table import THREE_DAYS_MS
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
        "Creating and running query job to set `is_deleted` to False for `%s.%s.%s`",
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
    logging.info("Would create query job and run via `run_query_async`.")
    return None


def _copy_tables_to_temporary(
    dry_run: bool,
    state_code: StateCode,
    bq_client: BigQueryClientImpl,
) -> List[Tuple[str, str]]:
    """Copy all raw data tables to a temporary dataset."""
    copy_jobs = []
    tmp_to_orig_dataset_ids: List[Tuple[str, str]] = []
    # Copy raw tables to temporary tables, and append `is_deleted=False` to each temporary table
    for instance in DirectIngestInstance:
        dataset_id = raw_tables_dataset_for_region(state_code, instance)

        tmp_dataset_id = f"tmp_{str(uuid.uuid4())[:6]}_{dataset_id}"
        tmp_dataset_ref = bq_client.dataset_ref_for_id(tmp_dataset_id)
        tmp_to_orig_dataset_ids.append((tmp_dataset_id, dataset_id))
        logging.info("Creating temporary dataset with id=%s", tmp_dataset_id)
        if not dry_run:
            bq_client.create_dataset_if_necessary(
                tmp_dataset_ref, default_table_expiration_ms=THREE_DAYS_MS
            )

        for table in bq_client.list_tables(dataset_id):
            table_id = table.table_id
            # Copy existing raw data table to temporary table
            logging.info(
                "Creating copy job for `%s.%s` to `%s.%s`...",
                dataset_id,
                table_id,
                tmp_dataset_id,
                table_id,
            )
            if not dry_run:
                copy_job = bq_client.copy_table(dataset_id, table_id, tmp_dataset_id)
                if copy_job is None:
                    raise ValueError("Expected copy job")
                copy_jobs.append(copy_job)
            else:
                logging.info("Would create copy job for %s", table_id)

        if copy_jobs and not dry_run:
            logging.info("Waiting for copy jobs to complete...")
            bq_client.wait_for_big_query_jobs(copy_jobs)
        else:
            logging.info("Would wait for copy jobs to complete...")
        logging.info("Copy finished.")
    return tmp_to_orig_dataset_ids


def _hydrate_is_deleted_in_temp_tables(
    dry_run: bool,
    tmp_dataset_ids: List[str],
    bq_client: BigQueryClientImpl,
    project_id: str,
) -> None:
    """Hydrated the is_deleted column in temp tables with `False`."""
    query_jobs: List[bigquery.QueryJob] = []

    for tmp_dataset_id in tmp_dataset_ids:
        logging.info("Iterating through tables in %s", tmp_dataset_id)
        if not dry_run:
            for table in bq_client.list_tables(tmp_dataset_id):
                # Set all values of `is_deleted` to False in the **temporary table**
                query_job: Optional[
                    bigquery.QueryJob
                ] = _set_raw_data_table_is_deleted_default_value_query_job(
                    bq_client=bq_client,
                    dry_run=dry_run,
                    project_id=project_id,
                    # Add `is_deleted` to tables in ** temporary ** dataset
                    dataset_id=tmp_dataset_id,
                    table_name=table.table_id,
                )
                if query_job:
                    query_jobs.append(query_job)
        else:
            logging.info(
                "Would iterate through tables in %s and call "
                "`_set_raw_data_table_is_deleted_default_value_query_job`",
                tmp_dataset_id,
            )

    if query_jobs and not dry_run:
        logging.info(
            "Waiting for query jobs to hydrate is_deleted=False in all temp tables..."
        )
        bq_client.wait_for_big_query_jobs(query_jobs)
    else:
        logging.info(
            "Would call `wait_for_big_query_jobs` for query jobs to hydrate is_deleted=False"
        )
    logging.info("Hydrating is_deleted=False completed.")


def _delete_and_recreate_empty_tables_with_required_is_deleted(
    dry_run: bool,
    dataset_ids: List[str],
    bq_client: BigQueryClientImpl,
    project_id: str,
) -> None:
    """For every table in every dataset in the provided |dataset_ids|, deletes the
    table, then recreates an empty version of that table with `is_deleted` = REQUIRED.
    """
    for dataset_id in dataset_ids:
        dataset_ref = bq_client.dataset_ref_for_id(dataset_id)
        for table in bq_client.list_tables(dataset_id):
            table_id = table.table_id
            # Delete and recreate the original table, with `is_deleted` = REQUIRED instead of NULLABLE
            logging.info(
                "Updating schema for `%s.%s.%s` to set `is_deleted` as REQUIRED...",
                project_id,
                dataset_id,
                table_id,
            )
            table = bq_client.get_table(dataset_ref, table_id)
            schema = [column for column in table.schema if column.name != "is_deleted"]
            schema.append(bigquery.SchemaField("is_deleted", "BOOLEAN", "REQUIRED"))
            logging.info("Updated local schema with `is_deleted` set to `REQUIRED`")
            if not dry_run:
                logging.info("Starting delete and recreate for `%s`", table_id)
                bq_client.delete_table(dataset_id, table_id)
                bq_client.create_table_with_schema(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    schema_fields=schema,
                    # Don't touch existing table's clustering fields
                    clustering_fields=table.clustering_fields,
                )
                logging.info("Delete and recreate finished for `%s`", table_id)
            else:
                logging.info(
                    "Would call `delete_table` and `create_table_with_schema` to recreate %s "
                    "with new schema.",
                    table_id,
                )


def _insert_populated_temporary_back_to_recreated(
    dry_run: bool,
    state_code: StateCode,
    bq_client: BigQueryClientImpl,
    project_id: str,
    tmp_to_orig_dataset_ids: List[Tuple[str, str]],
) -> None:
    """Copy over raw data contents from temporary tables (augmented with `is_deleted = False`) to the recreated original
    tables."""
    query_jobs: List[bigquery.QueryJob] = []
    for (tmp_dataset_id, orig_dataset_id) in tmp_to_orig_dataset_ids:
        if not dry_run:
            for table in bq_client.list_tables(tmp_dataset_id):
                file_tag = table.table_id
                logging.info(
                    "[%s] Copying table from temporary dataset=%s to original dataset %s...",
                    project_id,
                    tmp_dataset_id,
                    orig_dataset_id,
                )
                query_job = bq_client.insert_into_table_from_table_async(
                    source_dataset_id=tmp_dataset_id,
                    source_table_id=file_tag,
                    destination_dataset_id=orig_dataset_id,
                    destination_table_id=file_tag,
                    use_query_cache=False,
                )
                query_jobs.append(query_job)
        else:
            logging.info(
                "Would iterate through tables in %s and run `bq_client.insert_into_table_from_async`",
                tmp_dataset_id,
            )
    if dry_run:
        logging.info(
            "Would call wait_for_big_query_jobs to wait for query jobs in %s to finish.",
            state_code.value,
        )
    else:
        logging.info(
            "Created %s query jobs to populate raw data tables from temporary tables in %s",
            len(query_jobs),
            state_code.value,
        )
        logging.info("Waiting for query jobs to copy tables back to complete...")
        bq_client.wait_for_big_query_jobs(query_jobs)
        logging.info("Copy query jobs completed.")


def _iterate_through_raw_data_tables_in_state(
    bq_client: BigQueryClientImpl, dry_run: bool, project_id: str, state_code: StateCode
) -> None:
    """Iterate through raw data tables in each instance and generate and run query jobs to set `is_deleted` to False."""
    logging.info(
        "Starting iteration through raw file tags and instances in %s", state_code.value
    )
    tmp_to_orig_dataset_ids: List[Tuple[str, str]] = _copy_tables_to_temporary(
        dry_run, state_code, bq_client
    )
    tmp_dataset_ids = [tmp_dataset_id for tmp_dataset_id, _ in tmp_to_orig_dataset_ids]
    original_dataset_ids = [dataset_id for _, dataset_id in tmp_to_orig_dataset_ids]
    _hydrate_is_deleted_in_temp_tables(
        dry_run,
        tmp_dataset_ids,
        bq_client,
        project_id,
    )

    _delete_and_recreate_empty_tables_with_required_is_deleted(
        dry_run,
        original_dataset_ids,
        bq_client,
        project_id,
    )
    _insert_populated_temporary_back_to_recreated(
        dry_run,
        state_code,
        bq_client,
        project_id,
        tmp_to_orig_dataset_ids,
    )


def _set_is_deleted_for_raw_tables_in_project(
    bq_client: BigQueryClientImpl, dry_run: bool, project_id: str
) -> None:
    allowed_states = get_direct_ingest_states_existing_in_env()
    # US_AR is not launched fully yet
    filtered_states = [state for state in allowed_states if state != StateCode.US_AR]
    for state in filtered_states:
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
