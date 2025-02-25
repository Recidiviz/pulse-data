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
This file contains a script to generate and output a dataframe with information relating to the Idaho
LSU JII Text Message Pilot.

Usage:
python -m recidiviz.case_triage.jii.id_lsu_generate_message_dataframe \
    --dry-run true \
    --credentials-path
"""
import argparse
import datetime
import logging

import pandas as pd
from google.cloud.firestore_v1 import FieldFilter
from google.oauth2.service_account import Credentials

from recidiviz.case_triage.jii.id_lsu_message_analytics import (
    _get_batch_id_from_doc,
    _get_doc_id_from_doc,
)
from recidiviz.case_triage.jii.send_id_lsu_texts import OPT_OUT_KEY_WORDS
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.justice_counts.control_panel.utils import write_data_to_spreadsheet
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

# Spreadsheet Name: ID LSU Texting Pilot Messages
# https://docs.google.com/spreadsheets/d/1zwktTUSzXbH__IxgamvXgxi6M6odnYi7GzQ09ly6z4E/edit#gid=0
SPREADSHEET_ID = "1zwktTUSzXbH__IxgamvXgxi6M6odnYi7GzQ09ly6z4E"

logger = logging.getLogger(__name__)


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
        "--credentials-path",
        help="Used to point to path of JSON file with Google Cloud credentials.",
        required=False,
    )
    return parser


def generate_id_lsu_text_df(dry_run: bool, credentials: Credentials) -> None:
    """Generate a dataframe that includes all attempted text messages along with their
    delivery statuses and opt-out information. Writes the dataframe to a google sheet.
    """
    firestore_client = FirestoreClientImpl(project_id="jii-pilots")

    message_df = pd.DataFrame()

    # First, grab all documents at the message level
    message_ref = firestore_client.get_collection_group(
        collection_path="lsu_eligibility_messages"
    )
    for message_doc in message_ref.stream():
        doc_id = _get_doc_id_from_doc(message_doc)
        batch_id = _get_batch_id_from_doc(message_doc)
        external_id = doc_id.split("us_id_")[1]
        jii_message = message_doc.to_dict()
        if jii_message is None:
            continue
        jii_df = pd.DataFrame(jii_message, index=[0])
        jii_df["document_id"] = doc_id
        jii_df["external_id"] = external_id
        jii_df["batch_id"] = batch_id

        # Store message level document as row in df
        message_df = pd.concat([message_df, jii_df])

    # Next, grab all documents at the individual (jii) level who have opted-out
    twilio_ref = firestore_client.get_collection(collection_path="twilio_messages")
    doc_query = twilio_ref.where(
        filter=FieldFilter("opt_out_type", "in", OPT_OUT_KEY_WORDS)
    )
    for jii_doc in doc_query.stream():
        individual_doc = jii_doc.to_dict()
        if individual_doc is None:
            continue

        # For a given text message, if the jii/individual has opted-out at any point, concatenate
        # the opt-out data to the text message row
        message_df.loc[
            message_df.loc[:, "document_id"] == jii_doc.id, "last_opt_out_update"
        ] = individual_doc["last_opt_out_update"]
        message_df.loc[
            message_df.loc[:, "document_id"] == jii_doc.id, "opt_out_type"
        ] = individual_doc["opt_out_type"]

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
        generate_id_lsu_text_df(dry_run=args.dry_run, credentials=glocal_credentials)
