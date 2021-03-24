#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#  #
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  #
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  #
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
#
"""
Script to upload raw SendGrid CSV to Google Cloud Storage and add new rows to BigQuery
table.

To get data, go to https://app.sendgrid.com/, from the side bar, Activity > search for
all activity (in last 30 days) > Export CSV. The site will send you an email with a
link to download the CSV from. Upload that CSV using this script.

Usage:

python -m recidiviz.tools.upload_sendgrid_csv_to_gcs_and_bq
    --project-id [project-id]
    --local-filepath [file-path]

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.upload_sendgrid_csv_to_gcs_and_bq
    --project-id recidiviz-staging
    --local-filepath ~/Downloads/example.csv
"""

import argparse
import logging
import sys

from datetime import date
from typing import List
from google.cloud import bigquery
from google.cloud.bigquery import WriteDisposition

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.bq_refresh.bq_refresh import wait_for_table_load

BUCKET_SUFFIX = "-sendgrid-data"
DATASET_ID = "sendgrid_email_data"
FINAL_DESTINATION_TABLE = "raw_sendgrid_email_data"
TEMP_DESTINATION_TABLE = "temp_for_upload"
DATE_FORMAT = "%Y.%m.%d"
QUERY_TEMPLATE = """
SELECT processed, message_id, event, email 
FROM `{project_id}.sendgrid_email_data.{temp_table}` 
EXCEPT DISTINCT 
SELECT processed, message_id, event, email 
FROM `{project_id}.sendgrid_email_data.{final_table}` 
ORDER BY processed"""


def main(*, project_id: str, local_filepath: str) -> None:
    """Uploads file at filepath to Google Cloud Storage and adds rows that do not
    already exist in BigQuery table"""

    # Upload raw Sendgrid CSV to GCS
    fs = GcsfsFactory.build()
    fs.upload_from_contents_handle_stream(
        path=GcsfsFilePath(
            bucket_name=project_id + BUCKET_SUFFIX,
            blob_name=date.today().strftime(DATE_FORMAT),
        ),
        contents_handle=GcsfsFileContentsHandle(
            local_file_path=local_filepath, cleanup_file=False
        ),
        content_type="text/csv",
    )
    logging.info(
        "Uploaded file [%s] to Google Cloud Storage bucket name=[%s] blob name=[%s]",
        local_filepath,
        project_id + BUCKET_SUFFIX,
        date.today().strftime(DATE_FORMAT),
    )

    bq_client = BigQueryClientImpl(project_id=project_id)

    # Upload to BQ from GCS
    load_job = bq_client.load_table_from_cloud_storage_async(
        source_uri=f"gs://{project_id}{BUCKET_SUFFIX}/"
        f"{date.today().strftime(DATE_FORMAT)}",
        destination_dataset_ref=bigquery.DatasetReference(
            project=project_id,
            dataset_id=DATASET_ID,
        ),
        destination_table_id=TEMP_DESTINATION_TABLE,
        destination_table_schema=[
            bigquery.SchemaField("processed", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("message_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("event", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("api_key_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("recv_message_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("credential_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("subject", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("from", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("asm_group_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("template_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("originating_ip", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("reason", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("outbound_ip", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("outbound_ip_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("mx", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("attempt", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("user_agent", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_unique", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("username", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("categories", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("marketing_campaign_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("marketing_campaign_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "marketing_campaign_split_id", "STRING", mode="NULLABLE"
            ),
            bigquery.SchemaField(
                "marketing_campaign_version", "STRING", mode="NULLABLE"
            ),
            bigquery.SchemaField("unique_args", "STRING", mode="NULLABLE"),
        ],
        skip_leading_rows=1,
    )
    table_load_success = wait_for_table_load(bq_client, load_job)

    if not table_load_success:
        logging.info("Copy from cloud storage to temporary table failed")
        return

    # Upload non-duplicate rows to existing table
    num_rows_before = bq_client.get_table(
        dataset_ref=bigquery.DatasetReference(
            project=project_id,
            dataset_id=DATASET_ID,
        ),
        table_id=FINAL_DESTINATION_TABLE,
    ).num_rows

    insert_job = bq_client.insert_into_table_from_query(
        destination_dataset_id=DATASET_ID,
        destination_table_id=FINAL_DESTINATION_TABLE,
        query=QUERY_TEMPLATE.format(
            project_id=project_id,
            temp_table=TEMP_DESTINATION_TABLE,
            final_table=FINAL_DESTINATION_TABLE,
        ),
        write_disposition=WriteDisposition.WRITE_APPEND,
    )

    insert_job_result = insert_job.result()

    logging.info(
        "Loaded [%d] non-duplicate rows into table [%s]",
        (insert_job_result.total_rows - num_rows_before),
        FINAL_DESTINATION_TABLE,
    )

    bq_client.delete_table(dataset_id=DATASET_ID, table_id=TEMP_DESTINATION_TABLE)


def parse_arguments(argv: List[str]) -> argparse.Namespace:
    """Parses the named arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        required=True,
        help="The project_id for the destination table",
    )

    parser.add_argument(
        "--local-filepath",
        required=True,
        help="The local filepath for the local csv to upload.",
    )

    known_args, _ = parser.parse_known_args(argv)

    return known_args


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv)
    main(local_filepath=args.local_filepath, project_id=args.project_id)
