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
This file contains a script to output information regarding replies and undelivered text messages
relating to the JII Text Message Pilot.

Usage:
python -m recidiviz.case_triage.jii.jii_texts_undelivered_messages \
    --dry-run True \
    --initial-batch-id 10_21_2024_11_30_00 \
    --eligibility-batch-id 10_22_2024_11_01_36 \
    --initial-batch-id-redelivery 10_28_2024_11_01_25 \
    --eligibility-batch-id-redelivery 10_29_2024_10_58_26 \
    --launch-name D1/5/7 \
    --credentials-path
"""
import argparse
import logging

import pandas as pd
from google.cloud.firestore_v1 import FieldFilter
from google.oauth2.service_account import Credentials
from twilio.rest import Client as TwilioClient

from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.justice_counts.control_panel.utils import write_data_to_spreadsheet
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool
from recidiviz.utils.secrets import get_secret


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--initial-batch-id",
        help="A string representing a datetime of the initial texts run of the send_jii_texts.py script.",
        required=True,
    )
    parser.add_argument(
        "--eligibility-batch-id",
        help="A string representing a datetime of the eligibility texts of the send_jii_texts.py script.",
        required=True,
    )
    parser.add_argument(
        "--initial-batch-id-redelivery",
        help="A string representing a datetime of the initial texts redelivery run of the send_jii_texts.py script.",
        required=True,
    )
    parser.add_argument(
        "--eligibility-batch-id-redelivery",
        help="A string representing a datetime of the eligibility texts redelivery run of the send_jii_texts.py script.",
        required=True,
    )
    parser.add_argument(
        "--launch-name",
        help="A string representing the name of the given launch (that encompasses all 4 batches). This will be used in the Sheet Name.",
        required=True,
    )
    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode. Only prints the operations it would perform.",
    )
    parser.add_argument(
        "--credentials-path",
        help="Used to point to path of JSON file with Google Cloud credentials.",
        required=False,
    )
    return parser


# Spreadsheet Name: LSU Texting Pilot Replies and Undelivered Texts
# https://docs.google.com/spreadsheets/d/12ZhNDb0eP46si66z7aGc1iDp_xn0myIuFD26Z9M6GHg/edit?gid=944970904#gid=944970904
SPREADSHEET_ID = "12ZhNDb0eP46si66z7aGc1iDp_xn0myIuFD26Z9M6GHg"
firestore_client = FirestoreClientImpl(project_id="jii-pilots")
logger = logging.getLogger(__name__)


def get_undelivered_messages(
    initial_batch_id: str,
    eligibility_batch_id: str,
    initial_batch_id_redelivery: str,
    eligibility_batch_id_redelivery: str,
    launch_name: str,
    credentials: Credentials,
    dry_run: bool,
) -> None:
    """
    Given 4 batch_ids related to a launch of the LSU Texting Pilot, grabs documents for all undelivered text messages in those batches.
    Writes a sheet containing one row per undelivered text document to a google sheet.

    Note: a batch_id is a string representing a datetime of a previous run of the send_jii_texts.py script.
    """
    undelivered_message_df = pd.DataFrame()

    # Query the Firestore database for all undelivered messages in the given batch
    jii_messages_ref = firestore_client.get_collection_group(
        collection_path="lsu_eligibility_messages"
    )
    firestore_query = jii_messages_ref.where(
        filter=FieldFilter(
            "batch_id",
            "in",
            [
                initial_batch_id,
                eligibility_batch_id,
                initial_batch_id_redelivery,
                eligibility_batch_id_redelivery,
            ],
        )
    ).where(filter=FieldFilter("raw_status", "==", "undelivered"))
    undelivered_text_docs = firestore_query.stream()

    for undelivered_text_doc in undelivered_text_docs:
        undelivered_text_dict = undelivered_text_doc.to_dict()
        if undelivered_text_dict is None:
            continue

        doc_path = undelivered_text_doc.reference.path
        text_df = pd.DataFrame(undelivered_text_dict, index=[0])
        text_df["document_path"] = doc_path
        # Store undelivered message level document as row in df
        undelivered_message_df = pd.concat([undelivered_message_df, text_df])

    # Export df of text documents to google sheet
    num_rows, num_cols = undelivered_message_df.shape
    if dry_run is True:
        print(
            f"Would write dataframe with {num_rows} rows and {num_cols} columns to Google Sheets."
        )
    else:
        new_sheet_title = f"{launch_name} Undelivered Texts"
        write_data_to_spreadsheet(
            google_credentials=credentials,
            df=undelivered_message_df,
            columns=list(undelivered_message_df.columns),
            new_sheet_title=new_sheet_title,
            spreadsheet_id=SPREADSHEET_ID,
            logger=logger,
            index=1,
        )
        print(
            f"Wrote dataframe with {num_rows} rows and {num_cols} columns to {new_sheet_title} Sheet."
        )


def get_replies(
    initial_batch_id: str,
    eligibility_batch_id: str,
    initial_batch_id_redelivery: str,
    eligibility_batch_id_redelivery: str,
    launch_name: str,
    credentials: Credentials,
    dry_run: bool,
) -> None:
    """
    Given an initial_batch_id associated with a launch of the LSU Texting Pilot, grabs all reply text
    messages received after the date of the initial batch.

    Joins the reply text to information related to the jii with that reply phone number.

    Writes a sheet containing one row per reply text to a google sheet.
    """
    account_sid = get_secret("twilio_sid")
    auth_token = get_secret("twilio_auth_token")
    twilio_client = TwilioClient(account_sid, auth_token)
    initial_batch_id_split = initial_batch_id.split("_")
    # We need to subtract 1 day from the date_sent since the date_sent_after arg below is > rather than >=
    date_sent = f"{initial_batch_id_split[2]}-{initial_batch_id_split[0]}-{int(initial_batch_id_split[1]) - 1}"

    # This will be a list of dictionaries that we will export to the Google Sheet
    replies_list = []

    # Query Twilio for all replies
    all_replies = twilio_client.messages.list(
        to=get_secret("jii_twilio_phone_number"), date_sent_after=date_sent
    )

    for reply in all_replies:
        from_phone_num = reply.from_[2:]
        reply_dict = {
            "From": from_phone_num,
            "To": reply.to[2:],
            "Body": reply.body,
            "SendDate": reply.date_sent,
        }

        # Query Firestore for the jii (individual) level docs that have replies
        # We need to do this in order to grab external_id, state_code, districts, po_names
        jii_ref = firestore_client.get_collection_group(
            collection_path="twilio_messages"
        )
        firestore_query = jii_ref.where(
            filter=FieldFilter("phone_numbers", "array_contains", from_phone_num)
        )
        jii_level_docs = firestore_query.stream()
        doc_count = 0

        for jii_level_doc in jii_level_docs:
            jii_level_dict = jii_level_doc.to_dict()
            if jii_level_dict is None:
                continue

            batch_ids = jii_level_dict.get("batch_ids", [])
            if not isinstance(batch_ids, list):
                continue

            # Check that this particular jii was included in at least 1 of the batches
            if (
                len(
                    set(batch_ids)
                    & set(
                        [
                            initial_batch_id,
                            eligibility_batch_id,
                            initial_batch_id_redelivery,
                            eligibility_batch_id_redelivery,
                        ]
                    )
                )
                == 0
            ):
                continue
            doc_count += 1

            reply_dict["external_id"] = jii_level_dict["external_id"]
            reply_dict["state_code"] = jii_level_dict["state_code"]
            reply_dict["districts"] = jii_level_dict["districts"]
            reply_dict["po_names"] = jii_level_dict["po_names"]
            # We only expect 1 individual per phone_number
            if doc_count > 1:
                raise SystemError(
                    f"{doc_count} documents with phone number {from_phone_num} found. We only expect 1 jii level document per phone number."
                )

        replies_list.append(reply_dict)

    replies_df = pd.DataFrame(replies_list)
    num_rows, num_cols = replies_df.shape

    # Export df of replies to google sheet
    if dry_run is True:
        print(
            f"Would write dataframe with {num_rows} rows and {num_cols} columns to Google Sheets."
        )
    else:
        new_sheet_title = f"{launch_name} All Replies"
        write_data_to_spreadsheet(
            google_credentials=credentials,
            df=replies_df,
            columns=list(replies_df.columns),
            new_sheet_title=new_sheet_title,
            spreadsheet_id=SPREADSHEET_ID,
            logger=logger,
            index=1,
        )
        print(
            f"Wrote dataframe with {num_rows} rows and {num_cols} columns to {new_sheet_title} Sheet."
        )


if __name__ == "__main__":
    args = create_parser().parse_args()
    # When running locally, point to JSON file with service account credentials.
    # The service account has access to the spreadsheet with editor permissions.
    glocal_credentials = Credentials.from_service_account_file(args.credentials_path)
    get_undelivered_messages(
        dry_run=args.dry_run,
        initial_batch_id=args.initial_batch_id,
        eligibility_batch_id=args.eligibility_batch_id,
        initial_batch_id_redelivery=args.initial_batch_id_redelivery,
        eligibility_batch_id_redelivery=args.eligibility_batch_id_redelivery,
        launch_name=args.launch_name,
        credentials=glocal_credentials,
    )
    with local_project_id_override(GCP_PROJECT_STAGING):
        get_replies(
            dry_run=args.dry_run,
            initial_batch_id=args.initial_batch_id,
            eligibility_batch_id=args.eligibility_batch_id,
            initial_batch_id_redelivery=args.initial_batch_id_redelivery,
            eligibility_batch_id_redelivery=args.eligibility_batch_id_redelivery,
            launch_name=args.launch_name,
            credentials=glocal_credentials,
        )
