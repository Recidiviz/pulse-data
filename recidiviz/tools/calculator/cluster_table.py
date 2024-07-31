# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
Updates a table to be clustered by the specified fields.

Note this is not an atomic operation, it requires deleting and recreating the underlying
table. Ensure no downstream operations read from the table during this time as they may
error or receive empty results.

Run locally with the following command:

    python -m recidiviz.tools.calculator.cluster_table \
        --project-id [PROJECT_ID] \
        --dataset-id [DATASET_ID] \
        --table-id [TABLE_ID] \
        --fields [FIELD_1] [FIELD_2]...

Example:

    python -m recidiviz.tools.calculator.cluster_table \
        --project-id recidiviz-staging \
        --dataset-id us_tn_raw_data \
        --table-id ContactNoteComment \
        --fields file_id
"""

import argparse
import logging
import uuid
from typing import List

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000


def cluster_table(
    bq_client: BigQueryClient,
    dataset_id: str,
    table_id: str,
    fields_to_cluster: List[str],
) -> None:
    """This clusters the table by the specified fields.

    It does so by copying the table to a temporary dataset, recreating the table with
    clustering fields specified, and inserting the data back into the newly created
    table.
    """
    table = bq_client.get_table(dataset_id, table_id)
    existing_schema = table.schema
    existing_fields = {field.name for field in existing_schema}
    if extra_fields := set(fields_to_cluster).difference(existing_fields):
        raise ValueError(
            f"Fields {extra_fields} do not exist in table `{dataset_id}.{table_id}`."
        )

    if table.clustering_fields:
        if table.clustering_fields == fields_to_cluster:
            logging.info(
                "`%s.%s` is already clustered by the desired fields (%s). Aborting.",
                dataset_id,
                table_id,
                fields_to_cluster,
            )
            return
        prompt_for_confirmation(
            f"`{dataset_id}.{table_id}` is already clustered by {table.clustering_fields}. "
            f"Do you want to cluster by {fields_to_cluster} instead?",
            accepted_response_override=table_id,
        )

    tmp_dataset_id = f"tmp_{str(uuid.uuid4())[:6]}_{dataset_id}"
    bq_client.create_dataset_if_necessary(
        tmp_dataset_id, default_table_expiration_ms=THREE_DAYS_MS
    )

    logging.info(
        "Copying `%s.%s` to `%s.%s`...", dataset_id, table_id, tmp_dataset_id, table_id
    )
    copy_job = bq_client.copy_table(dataset_id, table_id, tmp_dataset_id)
    if copy_job is None:
        raise ValueError("Expected copy job")
    copy_job.result()
    logging.info("Copy finished.")

    logging.info(
        "Deleting and recreating `%s.%s`, clustered by %s...",
        dataset_id,
        table_id,
        fields_to_cluster,
    )
    # We explicitly recreate the table, because if we just overwrite the existing one
    # with query results all the fields are marked nullable.
    bq_client.delete_table(dataset_id, table_id)
    bq_client.create_table_with_schema(
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields=existing_schema,
        clustering_fields=fields_to_cluster,
    )
    logging.info("Delete and recreate finished.")

    logging.info("Inserting back into `%s.%s`...", dataset_id, table_id)
    query_job = bq_client.insert_into_table_from_table_async(
        source_dataset_id=tmp_dataset_id,
        source_table_id=table_id,
        destination_dataset_id=dataset_id,
        destination_table_id=table_id,
        use_query_cache=False,
    )
    query_job.result()
    logging.info("Insert finished.")

    logging.info("Deleting temporary dataset `%s`...", tmp_dataset_id)
    bq_client.delete_dataset(tmp_dataset_id, delete_contents=True)
    logging.info("Delete finished.")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--dataset-id",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--table-id",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--fields",
        type=str,
        nargs="+",
        required=True,
    )

    return parser


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    prompt_for_confirmation(
        "Have you confirmed that all processes that might query this table are paused "
        "(e.g. Cloud Task queues, Airflow DAG, Dataflow pipelines, etc)?"
    )

    with local_project_id_override(args.project_id):
        client = BigQueryClientImpl()
        cluster_table(client, args.dataset_id, args.table_id, args.fields)
