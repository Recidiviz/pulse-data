# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Manages the storage of data produced by calculations."""
import datetime
import logging
import uuid
from collections import defaultdict
from http import HTTPStatus
from typing import Dict, List, Tuple

import flask
from google.api_core.future.polling import PollingFuture
from google.cloud.bigquery import WriteDisposition
from google.cloud.bigquery.table import TableListItem
from more_itertools import one, peekable

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.calculator import dataflow_config
from recidiviz.calculator.dataflow_orchestration_utils import (
    get_metric_pipeline_enabled_states,
)
from recidiviz.calculator.normalized_state_update_lock_manager import (
    NormalizedStateUpdateLockManager,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    NORMALIZED_ENTITY_CLASSES,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entity_conversion_utils import (
    bq_schema_for_normalized_state_entity,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    normalized_state_dataset_for_state_code,
)
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_metrics import (
    make_most_recent_metric_view_builders,
)
from recidiviz.persistence.database import schema_utils
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.yaml_dict import YAMLDict

# Datasets must be at least 12 hours old to be deleted
DATASET_DELETION_MIN_SECONDS = 12 * 60 * 60

DATASET_MANAGED_BY_TERRAFORM_KEY = "managed_by_terraform"

calculation_data_storage_manager_blueprint = flask.Blueprint(
    "calculation_data_storage_manager", __name__
)


@calculation_data_storage_manager_blueprint.route("/prune_old_dataflow_data")
@requires_gae_auth
def prune_old_dataflow_data() -> Tuple[str, HTTPStatus]:
    """Calls the move_old_dataflow_metrics_to_cold_storage function."""
    move_old_dataflow_metrics_to_cold_storage()

    return "", HTTPStatus.OK


@calculation_data_storage_manager_blueprint.route("/delete_empty_datasets")
@requires_gae_auth
def delete_empty_datasets() -> Tuple[str, HTTPStatus]:
    """Calls the _delete_empty_datasets function."""
    _delete_empty_datasets()

    return "", HTTPStatus.OK


@calculation_data_storage_manager_blueprint.route("/update_normalized_state_dataset")
@requires_gae_auth
def update_normalized_state_dataset() -> Tuple[str, HTTPStatus]:
    """Calls the _update_normalized_state_dataset function."""
    _update_normalized_state_dataset()

    return "", HTTPStatus.OK


def _delete_empty_datasets() -> None:
    """Deletes all empty datasets in BigQuery."""
    bq_client = BigQueryClientImpl()
    datasets = bq_client.list_datasets()

    for dataset_resource in datasets:
        dataset_ref = bq_client.dataset_ref_for_id(dataset_resource.dataset_id)
        dataset = bq_client.get_dataset(dataset_ref)
        dataset_labels = dataset.labels

        # Skip datasets that are managed by terraform
        managed_by_terraform = False
        for label, value in dataset_labels.items():
            if label == DATASET_MANAGED_BY_TERRAFORM_KEY and value == "true":
                managed_by_terraform = True

        if managed_by_terraform:
            logging.info(
                "Skipping empty dataset that is in Terraform state [%s]",
                dataset_ref.dataset_id,
            )
            continue

        tables = peekable(bq_client.list_tables(dataset.dataset_id))
        created_time = dataset.created
        dataset_age_seconds = (
            datetime.datetime.now(datetime.timezone.utc) - created_time
        ).total_seconds()

        if not tables and dataset_age_seconds > DATASET_DELETION_MIN_SECONDS:
            logging.info(
                "Dataset %s is empty and was not created very recently. Deleting...",
                dataset_ref.dataset_id,
            )
            bq_client.delete_dataset(dataset_ref)


def _get_month_range_for_metric_and_state() -> Dict[str, Dict[str, int]]:
    """Determines the maximum number of months that each metric is calculated regularly
    for each state.

    Returns a dictionary in the format: {
        metric_table: {
                        state_code: int,
                        state_code: int
                      }
        }
    where the int values are the number of months for which the metric is regularly
    calculated for that state.
    """
    # Map metric type enum values to the corresponding tables in BigQuery
    metric_type_to_table: Dict[str, str] = {
        metric_type.value: table
        for table, metric_type in dataflow_config.DATAFLOW_TABLES_TO_METRIC_TYPES.items()
    }

    all_pipelines = YAMLDict.from_path(dataflow_config.PIPELINE_CONFIG_YAML_PATH)
    incremental_metric_pipelines = all_pipelines.pop_dicts(
        "incremental_metric_pipelines"
    )
    historical_metric_pipelines = all_pipelines.pop_dicts("historical_metric_pipelines")

    # Dict with the format: {metric_table: {state_code: int}}
    month_range_for_metric_and_state: Dict[str, Dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )

    for pipeline_config_group in [
        incremental_metric_pipelines,
        historical_metric_pipelines,
    ]:
        for pipeline_config in pipeline_config_group:
            metric_values = pipeline_config.pop("metric_types", str).split(" ")

            is_unbounded_date_pipeline = any(
                metric_value
                in [
                    metric_type.value
                    for metric_type in dataflow_config.ALWAYS_UNBOUNDED_DATE_METRICS
                ]
                for metric_value in metric_values
            )

            if is_unbounded_date_pipeline:
                # This pipeline is always run in full, and is handled separately
                continue

            calculation_month_count = pipeline_config.pop(
                "calculation_month_count", int
            )
            state_code = pipeline_config.pop("state_code", str)

            for metric in metric_values:
                metric_table = metric_type_to_table[metric]
                current_max = month_range_for_metric_and_state[metric_table][state_code]
                month_range_for_metric_and_state[metric_table][state_code] = max(
                    current_max, calculation_month_count
                )

    return month_range_for_metric_and_state


SOURCE_DATA_JOIN_CLAUSE_STANDARD_TEMPLATE = """LEFT JOIN
            (-- Job_ids that are the most recent for the given metric/state_code
                WITH most_recent_metrics AS (
                    {most_recent_metrics_view_query}
                )    
                SELECT DISTINCT job_id as keep_job_id FROM most_recent_metrics
            )
        ON job_id = keep_job_id
        LEFT JOIN 
          (SELECT DISTINCT created_on AS keep_created_date FROM
          `{project_id}.{dataflow_metrics_dataset}.{dataflow_metric_table_id}`
          ORDER BY created_on DESC
          LIMIT {day_count_limit})
        ON created_on = keep_created_date"""


SOURCE_DATA_JOIN_CLAUSE_WITH_MONTH_LIMIT_TEMPLATE = """LEFT JOIN
            (WITH ordered_months AS (
                -- All months in the output for the state, ordered by recency
                SELECT *, RANK() OVER (PARTITION BY state_code ORDER BY DATE(year, month, 1) DESC) as month_order 
                FROM
                (SELECT DISTINCT state_code, year, month
                FROM `{project_id}.{dataflow_metrics_dataset}.{dataflow_metric_table_id}`)
            ), month_limit_by_state AS (
                {month_limit_by_state}
            ), months_in_range AS (
                -- Only the months that are in range remain
                SELECT
                    ordered_months.state_code,
                    ordered_months.year,
                    ordered_months.month
                FROM
                    month_limit_by_state
                LEFT JOIN 
                    ordered_months
                ON month_limit_by_state.state_code = ordered_months.state_code
                 AND ordered_months.month_order <= month_limit_by_state.month_limit
            ), most_recent_metrics AS (
                {most_recent_metrics_view_query}
            )
        
            -- Job_ids that are the most recent for the given metric/state_code/year/month and are in the month range
            SELECT DISTINCT job_id as keep_job_id FROM
                months_in_range
            LEFT JOIN most_recent_metrics
            USING (state_code, year, month)
            )
        ON job_id = keep_job_id
        LEFT JOIN 
          (SELECT DISTINCT created_on AS keep_created_date FROM
          `{project_id}.{dataflow_metrics_dataset}.{dataflow_metric_table_id}`
          ORDER BY created_on DESC
          LIMIT {day_count_limit})
        ON created_on = keep_created_date"""


def move_old_dataflow_metrics_to_cold_storage(dry_run: bool = False) -> None:
    """Moves old output in Dataflow metrics tables to tables in a cold storage dataset.
    We only keep the MAX_DAYS_IN_DATAFLOW_METRICS_TABLE days worth of data in a Dataflow
    metric table at once. All other output is moved to cold storage, unless it is the
    most recent job_id for a metric in a state where that metric is regularly calculated,
    and where the year and month of the output falls into the window of what is regularly
    calculated for that metric and state. See the calculation_pipeline_templates.yaml
    file for a list of regularly scheduled calculations.

    If a metric has been entirely decommissioned, handles the deletion of the corresponding table.

    If dry_run is True, will log queries that would otherwise be run.
    """
    bq_client = BigQueryClientImpl()
    dataflow_metrics_dataset = DATAFLOW_METRICS_DATASET
    cold_storage_dataset = dataflow_config.DATAFLOW_METRICS_COLD_STORAGE_DATASET
    dataflow_metrics_tables = bq_client.list_tables(dataflow_metrics_dataset)

    month_range_for_metric_and_state = _get_month_range_for_metric_and_state()

    for table_ref in dataflow_metrics_tables:
        table_id = table_ref.table_id

        if table_id not in dataflow_config.DATAFLOW_TABLES_TO_METRIC_TYPES:
            # This metric has been deprecated. Handle the deletion of the table
            _decommission_dataflow_metric_table(bq_client, table_ref, dry_run)
            continue

        metric_type = dataflow_config.DATAFLOW_TABLES_TO_METRIC_TYPES[table_id]

        is_unbounded_date_pipeline = (
            metric_type in dataflow_config.ALWAYS_UNBOUNDED_DATE_METRICS
        )

        # This means there are no currently scheduled pipelines writing metrics to
        # this table with specific month ranges
        no_active_month_range_pipelines = not month_range_for_metric_and_state[
            table_id
        ].items()

        # we expect make_most_recent_metric_view_builders to only return one view builder
        # when it is not being split on the included_in_population bool
        most_recent_metrics_view_query = (
            one(
                make_most_recent_metric_view_builders(
                    metric_name=table_id, split_on_included_in_population=False
                )
            )
            .build()
            .view_query
        )

        if is_unbounded_date_pipeline or no_active_month_range_pipelines:
            source_data_join_clause = StrictStringFormatter().format(
                SOURCE_DATA_JOIN_CLAUSE_STANDARD_TEMPLATE,
                project_id=table_ref.project,
                dataflow_metrics_dataset=table_ref.dataset_id,
                dataflow_metric_table_id=table_id,
                most_recent_metrics_view_query=most_recent_metrics_view_query,
                day_count_limit=dataflow_config.MAX_DAYS_IN_DATAFLOW_METRICS_TABLE,
            )
        else:
            month_limit_by_state = "\nUNION ALL\n".join(
                [
                    f"SELECT '{state_code}' as state_code, {month_limit} as month_limit"
                    for state_code, month_limit in month_range_for_metric_and_state[
                        table_id
                    ].items()
                ]
            )
            source_data_join_clause = StrictStringFormatter().format(
                SOURCE_DATA_JOIN_CLAUSE_WITH_MONTH_LIMIT_TEMPLATE,
                project_id=table_ref.project,
                dataflow_metrics_dataset=table_ref.dataset_id,
                dataflow_metric_table_id=table_id,
                most_recent_metrics_view_query=most_recent_metrics_view_query,
                day_count_limit=dataflow_config.MAX_DAYS_IN_DATAFLOW_METRICS_TABLE,
                month_limit_by_state=month_limit_by_state,
            )

        # Exclude these columns leftover from the exclusion join from being added to the metric tables in cold storage
        columns_to_exclude_from_transfer = ["keep_job_id", "keep_created_date"]

        # This filter will return the rows that should be moved to cold storage
        insert_filter_clause = "WHERE keep_job_id IS NULL AND keep_created_date IS NULL"

        # Query for rows to be moved to the cold storage table
        columns_to_exclude = ", ".join(columns_to_exclude_from_transfer)
        insert_query = f"""
            SELECT * EXCEPT({columns_to_exclude}) FROM
            `{table_ref.project}.{table_ref.dataset_id}.{table_id}`
            {source_data_join_clause}
            {insert_filter_clause}
            """

        if dry_run:
            logging.info("###INSERT QUERY INTO COLD STORAGE TABLE###")
            logging.info("%s;", insert_query)
        else:
            # Move data from the Dataflow metrics dataset into the cold storage table, creating the table if necessary
            insert_job = bq_client.insert_into_table_from_query_async(
                destination_dataset_id=cold_storage_dataset,
                destination_table_id=table_id,
                query=insert_query,
                allow_field_additions=True,
                write_disposition=WriteDisposition.WRITE_APPEND,
                use_query_cache=True,
            )

            # Wait for the insert job to complete before running the replace job
            insert_job.result()

        # This will return the rows that were not moved to cold storage and should remain in the table
        columns_to_exclude = ", ".join(columns_to_exclude_from_transfer)
        replace_query = f"""
            SELECT * EXCEPT({columns_to_exclude}) FROM
            `{table_ref.project}.{table_ref.dataset_id}.{table_id}`
            {source_data_join_clause}
            WHERE keep_job_id IS NOT NULL OR keep_created_date IS NOT NULL
        """

        if dry_run:
            logging.info("###REPLACE QUERY INTO METRIC TABLE###")
            logging.info("%s;", replace_query)
        else:
            # Replace the Dataflow table with only the rows that should remain
            replace_job = bq_client.create_table_from_query_async(
                dataset_id=dataflow_metrics_dataset,
                table_id=table_ref.table_id,
                query=replace_query,
                overwrite=True,
                use_query_cache=True,
            )

            # Wait for the replace job to complete before moving on
            replace_job.result()


def _load_normalized_state_dataset_into_empty_temp_dataset(
    bq_client: BigQueryClient, dataset_id: str
) -> None:
    """Builds a full normalized_state dataset in the specified empty dataset location.
    If the temp dataset does not exist, creates the dataset first with a table
    expiration."""
    dataset_ref = bq_client.dataset_ref_for_id(dataset_id)

    # Create temp dataset that unified tables will be staged in.
    bq_client.create_dataset_if_necessary(
        dataset_ref,
        default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
    )

    state_specific_normalized_dataset_ids: List[str] = [
        normalized_state_dataset_for_state_code(
            state_code,
        )
        for state_code in get_metric_pipeline_enabled_states()
    ]

    non_normalized_dataset_id = dataset_config.STATE_BASE_DATASET

    jobs: List[PollingFuture] = []

    # Build a map of normalized entity table_id to the schema for that table.
    normalized_table_id_to_schema = {
        # We store normalized entities in tables with the same names as the tables of
        # their underlying base entity classes.
        schema_utils.get_state_database_entity_with_name(
            entity_cls.base_class_name()
        ).__tablename__: bq_schema_for_normalized_state_entity(entity_cls)
        for entity_cls in NORMALIZED_ENTITY_CLASSES
    }

    for table in bq_client.list_tables(non_normalized_dataset_id):
        table_id = table.table_id

        if table_id not in normalized_table_id_to_schema:
            # This is not a normalized entity. Copy the entire table from state into
            # the temporary dataset.
            job = bq_client.copy_table(
                source_dataset_id=non_normalized_dataset_id,
                source_table_id=table_id,
                destination_dataset_id=dataset_id,
            )

            if job:
                jobs.append(job)
        else:
            schema_for_entity_class = normalized_table_id_to_schema[table_id]

            bq_client.create_table_with_schema(
                dataset_id,
                table_id,
                schema_for_entity_class,
            )

            # This is a normalized entity. Insert the contents of this table from
            # each of the state's us_xx_normalized_state datasets into the
            # corresponding table in the temporary dataset
            for (
                state_specific_normalized_dataset_id
            ) in state_specific_normalized_dataset_ids:
                job = bq_client.insert_into_table_from_table_async(
                    source_dataset_id=state_specific_normalized_dataset_id,
                    source_table_id=table_id,
                    destination_dataset_id=dataset_id,
                    destination_table_id=table_id,
                    use_query_cache=True,
                )

                jobs.append(job)

    for job in jobs:
        job.result()  # Wait for the job to complete.


def _update_normalized_state_dataset() -> None:
    """Updates the normalized_state dataset with fresh data.

    First, builds a temporary dataset called `temp_normalized_state_TIMESTAMP` with
    data from each state's `us_xx_normalized_state` dataset (for each entity that is
    normalized) and the `state` dataset (for each entity that is not normalized). Then,
    replaces the `normalized_state` dataset with the contents of the temporary dataset.
    """
    lock_id = str(uuid.uuid4())
    logging.info("Request lock id: %s", lock_id)

    logging.info(
        "Acquiring lock on CloudSQL to BQ state refresh to prevent the "
        "`state` dataset from being updated during this process..."
    )
    lock_manager = NormalizedStateUpdateLockManager()
    lock_manager.acquire_lock(lock_id=lock_id)

    if not lock_manager.can_proceed():
        raise AssertionError(
            "Unable to acquire lock on CloudSQL to BQ state refresh. "
            "Due to the sequencing of our orchestration, this should not happen. "
            "Endpoint will need to be manually re-triggered once there are no "
            "CloudSQL to BQ refreshes in-progress for the state database."
        )

    bq_client = BigQueryClientImpl()

    temp_normalized_state_dataset_id = bq_client.add_timestamp_suffix_to_dataset_id(
        f"temp_{dataset_config.NORMALIZED_STATE_DATASET}"
    )

    _load_normalized_state_dataset_into_empty_temp_dataset(
        bq_client, temp_normalized_state_dataset_id
    )

    normalized_state_dataset_id = dataset_config.NORMALIZED_STATE_DATASET
    normalized_state_dataset_ref = bq_client.dataset_ref_for_id(
        normalized_state_dataset_id
    )
    bq_client.create_dataset_if_necessary(normalized_state_dataset_ref)

    # Copy the temporary dataset into normalized_state, overwriting the contents.
    # The `copy_dataset_tables` call will delete all extra tables that don't exist in
    # the source dataset because |overwrite_destination_tables| is set to True.
    bq_client.copy_dataset_tables(
        source_dataset_id=temp_normalized_state_dataset_id,
        destination_dataset_id=normalized_state_dataset_id,
        overwrite_destination_tables=True,
    )

    logging.info(
        "Process of building [%s] dataset complete.",
        dataset_config.NORMALIZED_STATE_DATASET,
    )

    logging.info("Deleting temporary [%s] dataset.", temp_normalized_state_dataset_id)
    bq_client.delete_dataset(
        bq_client.dataset_ref_for_id(temp_normalized_state_dataset_id),
        delete_contents=True,
    )

    logging.info("Releasing lock on CloudSQL to BQ state refresh.")
    lock_manager.release_lock()


def _decommission_dataflow_metric_table(
    bq_client: BigQueryClientImpl, table_ref: TableListItem, dry_run: bool = False
) -> None:
    """Decommissions a deprecated Dataflow metric table. Moves all remaining rows
    to cold storage and deletes the table in the DATAFLOW_METRICS_DATASET."""
    logging.info("Decommissioning Dataflow metric table: [%s]", table_ref.table_id)

    dataflow_metrics_dataset = DATAFLOW_METRICS_DATASET
    cold_storage_dataset = dataflow_config.DATAFLOW_METRICS_COLD_STORAGE_DATASET
    table_id = table_ref.table_id

    # Move all rows in the table to cold storage
    insert_query = (
        f"""SELECT * FROM `{table_ref.project}.{dataflow_metrics_dataset}.{table_id}`"""
    )

    if dry_run:
        logging.info("###DECOMMISION INSERT QUERY INTO COLD STORAGE TABLE###")
        logging.info("%s;", insert_query)
    else:
        insert_job = bq_client.insert_into_table_from_query_async(
            destination_dataset_id=cold_storage_dataset,
            destination_table_id=table_id,
            query=insert_query,
            allow_field_additions=True,
            write_disposition=WriteDisposition.WRITE_APPEND,
            use_query_cache=True,
        )

        # Wait for the insert job to complete before deleting the table
        insert_job.result()

        bq_client.delete_table(dataset_id=dataflow_metrics_dataset, table_id=table_id)
