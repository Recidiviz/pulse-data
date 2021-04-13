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

Also has --backfill flag for when schema is updated or files are updated. Use flag and
all rows in current permanent BQ table will be deleted and filled with updated rows
from CSVs in GCS.

Intended use is only one of --backfill or --local-filepath flags to be used at a time.
However when both flags are given, the new file will be uploaded and then the permanent
table will be backfilled with all files in GCS, including the new file.

To get data, go to https://app.sendgrid.com/, from the side bar, Activity > search for
all activity (in last 30 days) > Export CSV. The site will send you an email with a
link to download the CSV from. Upload that CSV using this script.

Most common usage, this uploads a file from sendgrid to GCS and BQ:

python -m recidiviz.tools.upload_sendgrid_csv_to_gcs_and_bq
    --project-id [project-id]
    --local-filepath [file-path]

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.upload_sendgrid_csv_to_gcs_and_bq
    --project-id recidiviz-staging
    --local-filepath ~/Downloads/example.csv

Back fill usage:

python -m recidiviz.tools.upload_sendgrid_csv_to_gcs_and_bq
    --project-id [project-id]
    --backfill true
"""

import argparse
import logging
import sys

from datetime import date
from typing import List
from google.cloud import bigquery
from google.cloud.bigquery import WriteDisposition

from recidiviz.cloud_storage.gcs_file_system import (
    GcsfsFileContentsHandle,
    GCSFileSystem,
)
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.bq_refresh.bq_refresh import wait_for_table_load
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION

BUCKET_SUFFIX = "-sendgrid-data"
DATASET_ID = "sendgrid_email_data"
FINAL_DESTINATION_TABLE = "raw_sendgrid_email_data"
TEMP_DESTINATION_TABLE = "temp_for_upload"
DATE_FORMAT = "%Y.%m.%d"
INSERT_QUERY_TEMPLATE = """
SELECT processed, subject, api_key_id, message_id, event, email 
FROM `{project_id}.{dataset_id}.{temp_table}` 
EXCEPT DISTINCT 
SELECT processed, subject, api_key_id, message_id, event, email 
FROM `{project_id}.{dataset_id}.{final_table}` 
ORDER BY processed"""


def main(*, project_id: str, local_filepath: str, backfill: bool) -> None:
    """If filepath, uploads file at filepath to Google Cloud Storage and adds rows that
     do not already exist in BigQuery table.
    If backfill, loads all rows in CSVs in GCS to BQ table.
    """

    fs = GcsfsFactory.build()
    bq_client = BigQueryClientImpl(project_id=project_id)

    # If backfill, clear out all data in BQ table and load in new rows
    if backfill and (
        input(
            "Are you sure? This action will delete all data from current raw data "
            "table and replace it with rows from CSVs currently in GCS. "
            "Enter 'backfill' if you are sure. \n"
        )
        == "backfill"
    ):
        # Clear out old rows from table
        bq_client.delete_from_table_async(
            dataset_id=DATASET_ID,
            table_id=FINAL_DESTINATION_TABLE,
            filter_clause="WHERE TRUE",
        )
        # For each file in table, load into BQ
        for blob in fs.ls_with_blob_prefix(f"{project_id}{BUCKET_SUFFIX}", ""):
            if isinstance(blob, GcsfsFilePath):
                logging.info(
                    "Back filling from blob [%s] in bucket [%s]",
                    blob.file_name,
                    f"{project_id}{BUCKET_SUFFIX}",
                )
                load_from_gcs_to_temp_table(bq_client, project_id, blob.file_name)
                load_from_temp_to_permanent_table(bq_client, project_id)

    # If local file path was provided, upload that file to GCS and load data into BQ
    if local_filepath:
        # If local file path provided, upload file at file path into GCS
        upload_raw_file_to_gcs(fs, local_filepath, f"{project_id}{BUCKET_SUFFIX}")
        logging.info("Found local file path, uploading from [%s]", local_filepath)

        # Load data to temporary table and then to permanent
        load_from_gcs_to_temp_table(
            bq_client, project_id, date.today().strftime(DATE_FORMAT)
        )
        load_from_temp_to_permanent_table(bq_client, project_id)


def upload_raw_file_to_gcs(
    fs: GCSFileSystem, local_filepath: str, bucket_name: str
) -> None:
    """Upload raw Sendgrid CSV to GCS"""

    fs.upload_from_contents_handle_stream(
        path=GcsfsFilePath(
            bucket_name=bucket_name,
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
        bucket_name,
        date.today().strftime(DATE_FORMAT),
    )


def load_from_gcs_to_temp_table(
    bq_client: BigQueryClientImpl, project_id: str, blob_name: str
) -> None:
    """Upload raw data from GCS to temporary BQ table """
    bucket_name = f"{project_id}{BUCKET_SUFFIX}"
    load_job = bq_client.load_table_from_cloud_storage_async(
        source_uri=f"gs://{bucket_name}/" f"{blob_name}",
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


def load_from_temp_to_permanent_table(
    bq_client: BigQueryClientImpl, project_id: str
) -> None:
    """Query temporary table and persist view to permanent table"""
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
        query=INSERT_QUERY_TEMPLATE.format(
            project_id=project_id,
            dataset_id=DATASET_ID,
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
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="The project_id for the destination table",
    )

    parser.add_argument(
        "--local-filepath",
        required=False,
        help="The local filepath for the local csv to upload.",
    )

    parser.add_argument(
        "--backfill",
        required=False,
        action="store_true",
        help="Whether or not to backfill bigquery with all the files in GCS. "
        "Set to 'true' to backfill",
    )

    known_args, _ = parser.parse_known_args(argv)

    return known_args


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv)
    main(
        local_filepath=args.local_filepath,
        project_id=args.project_id,
        backfill=args.backfill,
    )
