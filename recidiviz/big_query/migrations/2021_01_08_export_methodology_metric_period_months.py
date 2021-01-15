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
"""Moves all metrics with a PERSON methodology or a metric_period_months value greater than 1 to cold storage. This is
required before we deprecate and delete these columns entirely.

*** THIS SCRIPT SHOULD ONLY BE RUN BY THE ON-CALL ENGINEER AT THE TIME OF THE FIRST DEPLOY AFTER THIS LANDS ***

Run with the following command:
    python -m recidiviz.big_query.migrations.2021_01_08_export_methodology_metric_period_months \
        --project_id [PROJECT_ID]
        --dry_run [DRY_RUN]

"""
import argparse
import logging
import sys
from typing import List, Tuple

from google.cloud.bigquery import WriteDisposition

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.dataflow_output_storage_config import DATAFLOW_METRICS_COLD_STORAGE_DATASET, \
    DATAFLOW_METRICS_TO_TABLES
from recidiviz.calculator.pipeline.recidivism.metrics import ReincarcerationRecidivismRateMetric
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET

from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def main(dry_run: bool) -> None:
    """Moves all metrics with a PERSON methodology or a metric_period_months value greater than 1 to cold storage."""
    bq_client = BigQueryClientImpl()
    dataflow_metrics_dataset = DATAFLOW_METRICS_DATASET
    cold_storage_dataset = DATAFLOW_METRICS_COLD_STORAGE_DATASET
    dataflow_metrics_tables = bq_client.list_tables(dataflow_metrics_dataset)

    for table_ref in dataflow_metrics_tables:
        table_id = table_ref.table_id

        logging.info('Migrating data to cold storage for table [%s]', table_id)

        filter_clause = "WHERE methodology = 'PERSON'"

        # Every metric except this one has a metric_period_months column
        if table_id != DATAFLOW_METRICS_TO_TABLES[ReincarcerationRecidivismRateMetric]:
            filter_clause += " OR metric_period_months != 1"

        # Query for rows to be moved to the cold storage table
        insert_query = """
            SELECT * FROM
            `{project_id}.{dataflow_metrics_dataset}.{table_id}`
            {filter_clause}
        """.format(
            project_id=table_ref.project,
            dataflow_metrics_dataset=table_ref.dataset_id,
            table_id=table_id,
            filter_clause=filter_clause
        )

        if dry_run:
            logging.info("[DRY RUN] Would insert rows into [%s].[%s] from [%s].[%s] that match this query: %s",
                         cold_storage_dataset, table_id, dataflow_metrics_dataset, table_id,
                         insert_query)
        else:
            # Move data from the Dataflow metrics dataset into the cold storage table, creating the table if necessary
            insert_job = bq_client.insert_into_table_from_query(
                destination_dataset_id=cold_storage_dataset,
                destination_table_id=table_id,
                query=insert_query,
                allow_field_additions=True,
                write_disposition=WriteDisposition.WRITE_APPEND)

            # Wait for the insert job to complete before running the delete job
            insert_job.result()

        if dry_run:
            logging.info("[DRY RUN] Would delete rows from [%s].[%s] %s",
                         dataflow_metrics_dataset, table_id, filter_clause)
        else:
            # Delete these rows from the Dataflow metrics table
            delete_job = bq_client.delete_from_table_async(
                dataflow_metrics_dataset, table_id, filter_clause=filter_clause)

            # Wait for the replace job to complete before moving on
            delete_job.result()

        logging.info('Done migrating data for table [%s]', table_id)


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--project_id',
                        dest='project_id',
                        type=str,
                        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
                        required=True)

    parser.add_argument('--dry_run',
                        dest='dry_run',
                        type=str_to_bool,
                        required=True)

    return parser.parse_known_args(argv)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    logging.warning("This script should only be run by the on-call engineer doing a deploy."
                    " Are you the on-call engineer running a deploy for the %s project? (Y/n)", known_args.project_id)
    continue_running = input()
    if continue_running != 'Y':
        sys.exit()

    with local_project_id_override(known_args.project_id):
        main(known_args.dry_run)
