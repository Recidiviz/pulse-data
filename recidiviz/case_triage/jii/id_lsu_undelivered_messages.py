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
This file contains a script to output information regarding undelivered text messages
relating to the Idaho LSU JII Text Message Pilot.

Usage:
python -m recidiviz.case_triage.jii.id_lsu_undelivered_messages \
    --batch-id mm_dd_YYYY_HH_MM_SS \
    --bigquery-view recidiviz-staging.hsalas_scratch.ix_lsu_jii_pilot_2_POs
"""
import argparse

from google.cloud.firestore_v1 import FieldFilter
from twilio.rest import Client as TwilioClient

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.case_triage.workflows.utils import TwilioStatus
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.secrets import get_secret


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--batch-id",
        help="A string representing a datetime of a previous run of the send_id_lsu_texts.py script.",
        required=True,
    )
    parser.add_argument(
        "--bigquery-view",
        help="Name of the BigQuery view that we will use to map external_ids to JII names.",
        required=True,
    )
    return parser


def get_undelivered_message_info(batch_id: str, bigquery_view: str) -> None:
    """
    Given a batch_id, prints out JII names, external_ids, phone_numbers, and messages for all undelivered text messages
    in that batch. A batch_id is a string representing a datetime of a previous run of the send_id_lsu_texts.py script.
    """
    firestore_client = FirestoreClientImpl(project_id="jii-pilots")

    account_sid = get_secret("twilio_sid")
    auth_token = get_secret("twilio_auth_token")
    twilio_client = TwilioClient(account_sid, auth_token)
    batch_id_split = batch_id.split("_")
    date_sent = f"{batch_id_split[2]}-{batch_id_split[0]}-{batch_id_split[1]}"

    # Query Twilio for message attempts
    all_attempted_messages = twilio_client.messages.list(
        from_=get_secret("jii_twilio_phone_number"), date_sent=date_sent
    )

    print("-------------------------------------------------------------")
    print(f"The following messages from batch {batch_id} were undelivered")
    print("-------------------------------------------------------------")
    undelivered_message_list = []
    undelivered_external_ids = []
    undelivered_message_sids = []
    for attempted_message in all_attempted_messages:
        message_status = attempted_message.status
        message_sid = attempted_message.sid

        if message_status == TwilioStatus.UNDELIVERED.value:
            undelivered_message_sids.append(message_sid)

    # Query the Firestore database for the messages
    # Need to do this to get the external_ids
    jii_messages_ref = firestore_client.get_collection_group(
        collection_path="lsu_eligibility_messages"
    )
    firestore_query = jii_messages_ref.where(
        filter=FieldFilter("message_sid", "in", undelivered_message_sids)
    )
    jii_text_docs = firestore_query.stream()

    for jii_text_doc in jii_text_docs:
        jii_reply_doc_dict = jii_text_doc.to_dict()
        if jii_reply_doc_dict is None:
            continue

        individual_undelivered_message_dict = {}
        # We store external_ids pre-pended with us_id_ in Firestore
        # We need to slice this string to get the actual external_id
        external_id = jii_text_doc.reference.path.split("/")[1][6:]
        undelivered_external_ids.append(external_id)
        individual_undelivered_message_dict["external_id"] = external_id
        individual_undelivered_message_dict["phone_number"] = jii_reply_doc_dict[
            "phone_number"
        ]
        individual_undelivered_message_dict["text_body"] = jii_reply_doc_dict["body"]
        undelivered_message_list.append(individual_undelivered_message_dict)

    # Query BigQuery for JII Names
    external_id_to_name = {}
    bq_query = f"SELECT external_id, person_name FROM {bigquery_view} WHERE external_id IN {tuple(undelivered_external_ids)}"
    query_job = BigQueryClientImpl().run_query_async(
        query_str=bq_query,
        use_query_cache=True,
    )
    for individual in query_job:
        external_id = str(individual["external_id"])
        person_name = str(individual["person_name"])
        external_id_to_name[external_id] = person_name

    for undelivered_message_dict in undelivered_message_list:
        external_id = undelivered_message_dict["external_id"]
        person_name = external_id_to_name.get(external_id, "UNKNOWN")
        print(f"Name: {person_name}")
        for key, value in undelivered_message_dict.items():
            print(f"{key}: {value}")
        print("-------------------------------------------------------------")
    print(
        f"Total undelivered messages from batch_id={batch_id}: {len(undelivered_message_list)}"
    )
    print("-------------------------------------------------------------")


if __name__ == "__main__":
    args = create_parser().parse_args()
    with local_project_id_override(GCP_PROJECT_STAGING):
        get_undelivered_message_info(
            batch_id=args.batch_id, bigquery_view=args.bigquery_view
        )
