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
Usage:
python -m recidiviz.case_triage.jii.send_id_lsu_texts \
    --bigquery-view recidiviz-staging.michelle_id_lsu_jii.michelle_id_lsu_jii_solo_test \
    --dry-run True \
    --message-type initial_text
"""

import argparse
import datetime
import logging
from typing import Optional

from google.cloud.firestore_v1 import ArrayUnion, FieldFilter
from twilio.rest import Client as TwilioClient

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.case_triage.jii.helpers import (
    generate_eligibility_text_messages_dict,
    generate_initial_text_messages_dict,
)
from recidiviz.case_triage.util import MessageType
from recidiviz.case_triage.workflows.utils import ExternalSystemRequestStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import GCP_PROJECT_STAGING, get_gcp_environment
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool
from recidiviz.utils.secrets import get_secret

STAGING_URL = "https://app-staging.recidiviz.org"
PRODUCTION_URL = "https://app.recidiviz.org"


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bigquery-view",
        help="Name of the BigQuery view that the script will query to get the raw data.",
        required=True,
    )
    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode. Only prints the operations it would perform.",
    )
    parser.add_argument(
        "--callback-url",
        required=False,
        help="Specify a Twilio callback url for testing purposes.",
    )
    parser.add_argument(
        "--message-type",
        required=True,
        help="Specify which type of text message you would like to send (initial_text or eligibility_text).",
        choices=[
            MessageType.INITIAL_TEXT.value.lower(),
            MessageType.ELIGIBILITY_TEXT.value.lower(),
        ],
    )
    return parser


# Opt-In and Opt-Out Keywords can be configered via the Twilio Console (Opt-Out Management page)
OPT_OUT_KEY_WORDS = ["CANCEL", "END", "QUIT", "STOP", "STOPALL", "UNSUBSCRIBE"]


def send_id_lsu_texts(
    bigquery_view: str,
    dry_run: bool,
    message_type: str,
    callback_url: Optional[str] = None,
) -> None:
    """Given a bigquery view, fetches to bigquery data. We then iterate through each
    row in the bigquery data and construct a dictionary of phone numbers mapped to a
    text message string. Using that dictionary, we send a text message using Twilio. We
    then store a record of the sent text messages in Firestore.

    This script will send 1 of 2 types of text messages: initial text messages and
    eligibility text messages. Initial text messages are simply an introduction and allow
    the opportunity for jii to opt-out of future messages. Eligibility text messages are
    messages that actually notify whether or not the jii is eligible for lsu.
    """
    account_sid = get_secret("twilio_sid")
    auth_token = get_secret("twilio_auth_token")
    messaging_service_sid = get_secret("twilio_us_id_messaging_service_sid")

    client = TwilioClient(account_sid, auth_token)
    batch_id = datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S")

    query = f"SELECT * FROM {bigquery_view}"
    query_job = BigQueryClientImpl().run_query_async(
        query_str=query, use_query_cache=True
    )

    firestore_client = FirestoreClientImpl(project_id="jii-pilots")

    # Get all document_ids for individuals who already received an initial text
    message_ref = firestore_client.get_collection_group(
        collection_path="lsu_eligibility_messages"
    )
    message_query = message_ref.where(
        filter=FieldFilter("message_type", "==", MessageType.INITIAL_TEXT.value)
    ).where(
        filter=FieldFilter("status", "==", ExternalSystemRequestStatus.SUCCESS.value)
    )
    initial_text_document_ids = {
        message_doc.reference.path.split("/")[1]
        for message_doc in message_query.stream()
    }
    logging.info(
        "%s individuals have already received initial/welcome texts.",
        len(initial_text_document_ids),
    )
    logging.info(
        "Document ids of individuals who have received initial/welcome texts: "
    )
    logging.info(initial_text_document_ids)

    # Get all document_ids for individuals who have opted-out
    twilio_ref = firestore_client.get_collection(collection_path="twilio_messages")
    doc_query = twilio_ref.where(
        filter=FieldFilter("opt_out_type", "in", OPT_OUT_KEY_WORDS)
    )
    opt_out_document_ids = {jii_doc.id for jii_doc in doc_query.stream()}
    logging.info("%s individuals have opted-out of texts.", len(opt_out_document_ids))
    logging.info("Document ids of individuals who have opted-out: ")
    logging.info(opt_out_document_ids)

    # Generate the dictionary that maps external ids to phone numbers to text strings
    logging.info("Generate dictionary of external ids to phone numbers to text strings")
    if message_type == MessageType.INITIAL_TEXT.value.lower():
        external_id_to_phone_num_to_text_dict = generate_initial_text_messages_dict(
            bq_output=query_job
        )
        message_type = MessageType.INITIAL_TEXT.value
        logging.info("message_type: %s", message_type)
    elif message_type == MessageType.ELIGIBILITY_TEXT.value.lower():
        external_id_to_phone_num_to_text_dict = generate_eligibility_text_messages_dict(
            bq_output=query_job
        )
        message_type = MessageType.ELIGIBILITY_TEXT.value
        logging.info("message_type: %s", message_type)

    state_code = StateCode.US_ID.value.lower()

    # FOR TEST PURPOSES
    if bigquery_view in [
        "recidiviz-staging.michelle_id_lsu_jii.michelle_id_lsu_jii_playtesting",
        "recidiviz-staging.michelle_id_lsu_jii.michelle_id_lsu_jii_solo_test",
    ]:
        logging.info("Iterate through dictionary construction from bigquery")
        # Iterate through the dictionary that maps external ids to phone numbers to text strings
        for (
            external_id,
            phone_num_to_text_dict,
        ) in external_id_to_phone_num_to_text_dict.items():
            for phone_number, text_body in phone_num_to_text_dict.items():
                document_id = f"{state_code}_{external_id}"
                logging.info("document_id: %s", document_id)

                # Check that current document_id has not opted-out
                if document_id in opt_out_document_ids:
                    logging.info(
                        "JII with document id %s has opted-out of receiving texts. Will not attempt to send message.",
                        document_id,
                    )
                    continue

                logging.info(
                    "Twilio send SMS gcp environment: [%s]",
                    get_gcp_environment(),
                )
                base_url = STAGING_URL
                if get_gcp_environment() == "production":
                    base_url = PRODUCTION_URL
                if callback_url is not None:
                    base_url = callback_url

                # If this is an initial/welcome text, and the individual has already
                # received an initial/welcome text in the past, do not attempt to send
                # them an initial/welcome text
                if message_type == MessageType.INITIAL_TEXT.value:
                    if document_id in initial_text_document_ids:
                        logging.info(
                            "Individual with document id [%s] has already received initial text. Do not attempt to send initial text.",
                            document_id,
                        )
                        continue

                # If this is an eligibility text, and the individual has not
                # received an initial/welcome text in the past, do not attempt to send
                # them an eligibility text
                if message_type == MessageType.ELIGIBILITY_TEXT.value:
                    if document_id not in initial_text_document_ids:
                        logging.info(
                            "Individual with document id [%s] has not received initial text. Do not attempt to send eligibility text.",
                            document_id,
                        )
                        continue

                if dry_run is True:
                    logging.info(
                        "DRY RUN: Would send the following text to individual with document id: %s",
                        document_id,
                    )
                    with open(f"{batch_id}_texts.txt", "a", encoding="utf-8") as file:
                        file.write(text_body)
                        file.write("\n")
                        file.write("\n")
                        file.close()
                    continue
                # Send text message to individual
                logging.info(
                    "Sending text to individual with document id: %s", document_id
                )
                firestore_individual_path = f"twilio_messages/{document_id}"
                try:
                    response = client.messages.create(
                        body=text_body,
                        messaging_service_sid=messaging_service_sid,
                        to=phone_number,
                        status_callback=f"{base_url}/jii/webhook/twilio_status",
                    )

                    # Store the individual's phone number in their individual level doc
                    logging.info(
                        "Updating individual's phone_numbers with document id: %s",
                        document_id,
                    )
                    firestore_client.set_document(
                        document_path=firestore_individual_path,
                        data={
                            "last_phone_num_update": datetime.datetime.now(
                                datetime.timezone.utc
                            ),
                            "phone_numbers": ArrayUnion([phone_number]),
                        },
                        merge=True,
                    )

                    # Next, update their message level doc
                    firestore_message_path = f"twilio_messages/{document_id}/lsu_eligibility_messages/eligibility_{batch_id}"
                    logging.info(
                        "Updating individual's message level doc with document id: %s",
                        document_id,
                    )
                    firestore_client.set_document(
                        document_path=firestore_message_path,
                        data={
                            "timestamp": datetime.datetime.now(datetime.timezone.utc),
                            "message_sid": response.sid,
                            "body": text_body,
                            "phone_number": phone_number,
                            "message_type": message_type,
                        },
                        merge=True,
                    )
                except Exception as e:
                    logging.error("Error sending sms request to Twilio. Error: %s", e)
                    firestore_client.set_document(
                        firestore_individual_path,
                        {
                            "status": ExternalSystemRequestStatus.FAILURE.value,
                            "updated": {
                                "date": datetime.datetime.now(datetime.timezone.utc),
                                "by": "RECIDIVIZ",
                            },
                            "errors": [str(e)],
                        },
                        merge=True,
                    )


if __name__ == "__main__":
    args = create_parser().parse_args()

    with local_project_id_override(GCP_PROJECT_STAGING):
        send_id_lsu_texts(
            bigquery_view=args.bigquery_view,
            dry_run=args.dry_run,
            callback_url=args.callback_url,
            message_type=args.message_type,
        )
