#!/usr/bin/env bash

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
This file contains a script that checks the last time each JII in a Big Query view has
received a text message.

Usage:
python -m recidiviz.case_triage.jii.jii_texts_check_last_text \
    --bigquery-view recidiviz-staging.michelle_id_lsu_jii.michelle_id_lsu_jii_solo_test \
    --credentials-path \
    --message-type initial_text
"""
import argparse
import datetime
import logging
from collections import defaultdict
from typing import Optional

import pandas as pd
from google.cloud.firestore_v1 import FieldFilter
from google.oauth2.service_account import Credentials

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.case_triage.jii.jii_texts_message_analytics import _get_doc_id_from_doc
from recidiviz.case_triage.util import MessageType
from recidiviz.case_triage.workflows.utils import ExternalSystemRequestStatus
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.justice_counts.control_panel.utils import write_data_to_spreadsheet
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)

# Spreadsheet Name: ID LSU Texting Pilot Most Recent Messages
# https://docs.google.com/spreadsheets/d/1InhzLK7YIbNv6L_M92L7Qy0AnkVzM-OJ9rqgWSZ5rmg/edit?gid=0#gid=0
SPREADSHEET_ID = "1InhzLK7YIbNv6L_M92L7Qy0AnkVzM-OJ9rqgWSZ5rmg"


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode. Only prints the operations it would perform.",
    )
    parser.add_argument(
        "--bigquery-view",
        help="Name of the BigQuery view that the script will query to get the raw data.",
        required=True,
    )
    parser.add_argument(
        "--credentials-path",
        help="Used to point to path of JSON file with Google Cloud credentials.",
        required=False,
    )
    parser.add_argument(
        "--message-type",
        required=False,
        help="Specify which type of text message that each jii last received (initial_text or eligibility_text).",
        choices=[
            MessageType.INITIAL_TEXT.value.lower(),
            MessageType.ELIGIBILITY_TEXT.value.lower(),
        ],
    )
    return parser


def check_jii_last_text(
    bigquery_view: str,
    credentials: Credentials,
    dry_run: bool,
    message_type: Optional[str] = None,
) -> None:
    """
    For each JII that in a given BigQuery view, output the last time that jii has
    received a text message, if ever, (and the text message type).

    If a message_type is not provided, we output when the most recent text message was
    received (regardless of type).
    If a message_type is provided, we output the most recent text message that was
    received of the given message_type.
    """

    # First, grab all external_ids from the BigQuery view

    big_query_external_ids = set()

    query = f"SELECT * FROM {bigquery_view}"
    query_job = BigQueryClientImpl().run_query_async(
        query_str=query, use_query_cache=True
    )

    for individual in query_job:
        external_id = str(individual["external_id"])
        big_query_external_ids.add(external_id)

    print(f"{len(big_query_external_ids)} total external_ids from Big Query view")

    firestore_client = FirestoreClientImpl(project_id="jii-pilots")
    external_id_to_texts = defaultdict(list)

    # Next, grab all documents from Firestore at the message level
    message_ref = firestore_client.get_collection_group(
        collection_path="lsu_eligibility_messages"
    ).where(
        filter=FieldFilter("status", "==", ExternalSystemRequestStatus.SUCCESS.value)
    )

    if message_type == MessageType.INITIAL_TEXT.value.lower():
        message_ref = message_ref.where(
            filter=FieldFilter("message_type", "==", MessageType.INITIAL_TEXT.value)
        )
    elif message_type == MessageType.ELIGIBILITY_TEXT.value.lower():
        message_ref = message_ref.where(
            filter=FieldFilter("message_type", "==", MessageType.ELIGIBILITY_TEXT.value)
        )

    for message_doc in message_ref.stream():
        doc_id = _get_doc_id_from_doc(message_doc)
        external_id = doc_id.split("us_id_")[1]

        if external_id not in big_query_external_ids:
            continue

        jii_message = message_doc.to_dict()
        if jii_message is None:
            continue

        external_id_to_texts[external_id].append(jii_message)

    # Next, store the most recent message for each jii in the BigQuery view
    message_df = pd.DataFrame()
    for external_id, message_list in external_id_to_texts.items():

        max_timestamp = datetime.datetime(2000, 1, 1).replace(
            tzinfo=datetime.timezone.utc
        )
        max_message = None
        for message in message_list:
            message_timestamp = message["timestamp"]
            if message_timestamp > max_timestamp:
                max_timestamp = message_timestamp
                max_message = message

        jii_df = pd.DataFrame(max_message, index=[0])
        jii_df["external_id"] = external_id
        message_df = pd.concat([message_df, jii_df])

    if dry_run is True:
        num_rows, num_cols = message_df.shape
        print(
            f"Would write dataframe with {num_rows} rows and {num_cols} columns to Google Sheets."
        )
    else:
        now = datetime.datetime.now()
        new_sheet_title = f"{now.month}-{now.day}-{now.year}"
        write_data_to_spreadsheet(
            google_credentials=credentials,
            df=message_df,
            columns=list(message_df.columns),
            new_sheet_title=new_sheet_title,
            spreadsheet_id=SPREADSHEET_ID,
            logger=logger,
            index=0,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    # When running locally, point to JSON file with service account credentials.
    # The service account has access to the spreadsheet with editor permissions.
    glocal_credentials = Credentials.from_service_account_file(args.credentials_path)
    with local_project_id_override(GCP_PROJECT_STAGING):
        check_jii_last_text(
            bigquery_view=args.bigquery_view,
            credentials=glocal_credentials,
            dry_run=args.dry_run,
            message_type=args.message_type,
        )
