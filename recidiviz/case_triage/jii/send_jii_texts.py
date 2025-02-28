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
For a given JII ID LSU launch, we plan to run this script 4 times as follows:

1. Attempt to send initial texts to all individuals.
python -m recidiviz.case_triage.jii.send_jii_texts \
    --bigquery-view recidiviz-staging.michelle_id_lsu_jii.michelle_id_lsu_jii_solo_test \
    --dry-run false \
    --message-type initial_text

2. After at least 4 hours, attempt to send initial texts to individuals in which the first initial text (from step 1)
was not delivered.
python -m recidiviz.case_triage.jii.send_jii_texts \
    --bigquery-view recidiviz-staging.michelle_id_lsu_jii.michelle_id_lsu_jii_solo_test \
    --dry-run false \
    --message-type initial_text \
    --previous-batch-id-to-update-status-for mm_dd_YYYY_HH_MM_SS \
    --redeliver-failed-messages true

3. Attempt to send eligibility texts to all individuals.
python -m recidiviz.case_triage.jii.send_jii_texts \
    --bigquery-view recidiviz-staging.michelle_id_lsu_jii.michelle_id_lsu_jii_solo_test \
    --dry-run false \
    --message-type eligibility_text \
    
4. After at least 4 hours, attempt to send eligibility texts to individuals in which the first eligibility text
(from step 3) was not delivered.
python -m recidiviz.case_triage.jii.send_jii_texts \
    --bigquery-view recidiviz-staging.michelle_id_lsu_jii.michelle_id_lsu_jii_solo_test \
    --dry-run false \
    --message-type eligibility_text \
    --previous-batch-id-to-update-status-for mm_dd_YYYY_HH_MM_SS \
    --redeliver-failed-messages true


In general, we have 8 combinations of the following 3 arguments (dry-run, previous-batch-id-to-update-status-for, and redeliver-failed-messages):

1. dry-run is True, previous-batch-id-to-update-status-for is None and redeliver-failed-messages is False
a. First-attempt messages are not sent
b. No message statuses are updated in Firestore
c. No previously failed messages are re-delivered

2. dry-run is False, previous-batch-id-to-update-status-for is None and redeliver-failed-messages is False
a. First-attempt messages are sent
b. No message statuses are updated in Firestore
c. No previously failed messages are re-delivered

3. dry-run is True, previous-batch-id-to-update-status-for is None and redeliver-failed-messages is True (same result as 1 above)
a. First-attempt messages are not sent
b. No message statuses are updated in Firestore
c. No previously failed messages are re-delivered

4. dry-run is False, previous-batch-id-to-update-status-for is None and redeliver-failed-messages is True (same result as 2 above)
a. First-attempt messages are sent
b. No message statuses are updated in Firestore
c. No previously failed messages are re-delivered

5. dry-run is True, previous-batch-id-to-update-status-for is not None and redeliver-failed-messages is False
a. First-attempt messages are not sent
b. Message statuses are updated in Firestore
c. No previously failed messages are re-delivered

6. dry-run is False, previous-batch-id-to-update-status-for is not None and redeliver-failed-messages is False
a. First-attempt messages are sent
b. Message statuses are updated in Firestore
c. No previously failed messages are re-delivered

7. dry-run is True, previous-batch-id-to-update-status-for is not None and redeliver-failed-messages is True (same result as 5 above)
a. First-attempt messages are not sent
b. Message statuses are updated in Firestore
c. No previously failed messages are re-delivered

8. dry-run is False, previous-batch-id-to-update-status-for is not None and redeliver-failed-messages is True
a. First-attempt messages are not sent
b. Message statuses are updated in Firestore
c. Previously failed messages are re-delivered


The only results we currently do not cover are:

A.
a. First-attempt messages are sent
b. Message statuses are updated in Firestore
c. Previously failed messages are re-delivered

B.
a. First-attempt messages are not sent
b. Message statuses are not updated in Firestore
c. Previously failed messages are re-delivered

C.
a. First-attempt messages are sent
b. Message statuses are not updated in Firestore
c. Previously failed messages are re-delivered

This is because we make the following assumptions:
- either a or c can happen (we are either sending out first-attempt texts or retries, never both)
- c can only happen if b happens (we are only sending re-tries if all statuses are updated)
"""

import argparse
import datetime
import logging
from collections import defaultdict
from typing import Dict, Optional, Set, Tuple

from google.cloud.firestore_v1 import ArrayUnion, FieldFilter
from twilio.rest import Client as TwilioClient

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.case_triage.jii.helpers import (
    generate_eligibility_text_messages_dict,
    generate_initial_text_messages_dict,
    update_status_helper,
)
from recidiviz.case_triage.util import MessageType
from recidiviz.case_triage.workflows.utils import ExternalSystemRequestStatus
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import (
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
    get_gcp_environment,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool
from recidiviz.utils.secrets import get_secret

STAGING_URL = "https://app-staging.recidiviz.org"
PRODUCTION_URL = "https://app.recidiviz.org"

logger = logging.getLogger(__name__)


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
    parser.add_argument(
        "--previous-batch-id-to-update-status-for",
        required=False,
        help="A string representing a datetime of a previous run of this script. Used to update the statuses of messages in the Firestore db.",
    )
    parser.add_argument(
        "--redeliver-failed-messages",
        required=False,
        type=str_to_bool,
        default=False,
        help="Whether or not you would like to attempt to resend texts that were previously undelivered. If provided, you must also specify the --previous-batch-id-to-update-status-for-to-update-status-for flag.",
    )
    parser.add_argument(
        "--resend-eligibility-texts",
        required=False,
        type=str_to_bool,
        default=False,
        help="Whether or not you would like to attempt to resend eligibility texts to individuals that have already received eligibility texts in the past 90 days.",
    )
    parser.add_argument(
        "--project-id",
        choices=[
            GCP_PROJECT_STAGING,
            GCP_PROJECT_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=False,
    )
    return parser


# Opt-In and Opt-Out Keywords can be configered via the Twilio Console (Opt-Out Management page)
OPT_OUT_KEY_WORDS = ["CANCEL", "END", "QUIT", "STOP", "STOPALL", "UNSUBSCRIBE"]


def send_jii_texts(
    bigquery_view: str,
    dry_run: bool,
    message_type: str,
    redeliver_failed_messages: bool,
    resend_eligibility_texts: bool,
    previous_batch_id: Optional[str] = None,
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


    previous_batch_id is a string representing a datetime of a previous run of this script.
    If provided, this is used to update the statuses of messages in the Firestore db.

    If redeliver_failed_messages is True, we will look up which messages were undelivered during
    that previous attempt to send text messages. For each previously undelivered message,
    we attempt to re-send a text message to that individual. If redeliver_failed_messages is True,
    previous_batch_id must be supplied (cannot be None).
    """
    account_sid = get_secret("twilio_sid")
    auth_token = get_secret("twilio_auth_token")
    messaging_service_sid = get_secret("twilio_jii_texting_messaging_service_sid")

    twilio_client = TwilioClient(account_sid, auth_token)
    current_batch_id = datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S")

    firestore_client = FirestoreClientImpl(project_id="jii-pilots")

    # Get all data from the BigQuery table
    query = f"SELECT * FROM {bigquery_view}"
    query_job = BigQueryClientImpl().run_query_async(
        query_str=query, use_query_cache=True
    )

    # Get the state_code from the BigQuery table
    state_code_query = f"SELECT DISTINCT(state_code) FROM {bigquery_view}"
    state_code_query_job = BigQueryClientImpl().run_query_async(
        query_str=state_code_query, use_query_cache=True
    )
    state_codes = []
    for row in state_code_query_job:
        state_codes.append(row[0])

    num_states = len(state_codes)
    if num_states != 1:
        raise SystemError(
            f"The BigQuery view contains {num_states} distinct state codes: {state_codes}. We expect 1 state code per batch."
        )
    state_code = state_codes[0].lower()
    logging.info("state_code: %s", state_code)

    # Store current_batch_id and Big Query View in Firestore db
    if dry_run is False:
        store_batch_id(
            firestore_client=firestore_client,
            current_batch_id=current_batch_id,
            message_type=message_type,
            redeliver_failed_messages=redeliver_failed_messages,
            bigquery_view=bigquery_view,
            state_code=state_code,
        )

    (
        initial_text_document_ids,
        eligibility_text_document_ids_to_text_timestamp,
    ) = get_initial_and_eligibility_doc_ids(
        firestore_client=firestore_client, state_code=state_code
    )

    logging.info(
        "%s individuals from %s have already received initial/welcome texts.",
        len(initial_text_document_ids),
        state_code,
    )
    logging.info(
        "Document ids of individuals from %s who have received initial/welcome texts: ",
        state_code,
    )
    logging.info(initial_text_document_ids)
    logging.info(
        "%s individuals from %s have already received eligibility texts.",
        len(eligibility_text_document_ids_to_text_timestamp),
        state_code,
    )
    logging.info(
        "Document ids of individuals from %s who have received eligibility texts: ",
        state_code,
    )
    logging.info(eligibility_text_document_ids_to_text_timestamp.keys())

    opt_out_document_ids = get_opt_out_document_ids(
        firestore_client=firestore_client, state_code=state_code
    )

    # Generate the list of dictionaries that contains external ids, phone numbers, and text strings
    logging.info(
        "Generate list of dictionaries with external ids, phone numbers, and text strings"
    )
    if message_type == MessageType.INITIAL_TEXT.value.lower():
        text_messages_dicts = generate_initial_text_messages_dict(bq_output=query_job)
        message_type = MessageType.INITIAL_TEXT.value
        logging.info("message_type: %s", message_type)
    elif message_type == MessageType.ELIGIBILITY_TEXT.value.lower():
        text_messages_dicts = generate_eligibility_text_messages_dict(
            bq_output=query_job
        )
        message_type = MessageType.ELIGIBILITY_TEXT.value
        logging.info("message_type: %s", message_type)
    else:
        raise ValueError(f"Unexpected message_type [{message_type}]")

    if previous_batch_id is not None:
        external_ids_to_retry = update_statuses_from_previous_batch(
            twilio_client=twilio_client,
            firestore_client=firestore_client,
            previous_batch_id=previous_batch_id,
        )
    else:
        external_ids_to_retry = set()

    logging.info("Iterate through list of dictionaries constructed from bigquery")
    # Iterate through the list of dictionaries that contains text strings
    for text_messages_dict in text_messages_dicts:
        external_id = text_messages_dict["external_id"]
        phone_number = text_messages_dict["phone_num"]
        text_body = text_messages_dict["text_body"]
        po_name = text_messages_dict["po_name"]
        district = text_messages_dict["district"]

        document_id = f"{state_code}_{external_id}"
        logging.info("document_id: %s", document_id)

        attempt_to_send_text_bool = attempt_to_send_text(
            previous_batch_id=previous_batch_id,
            redeliver_failed_messages=redeliver_failed_messages,
            document_id=document_id,
            external_ids_to_retry=external_ids_to_retry,
            opt_out_document_ids=opt_out_document_ids,
            message_type=message_type,
            initial_text_document_ids=initial_text_document_ids,
            eligibility_text_document_ids_to_text_timestamp=eligibility_text_document_ids_to_text_timestamp,
            resend_eligibility_texts=resend_eligibility_texts,
        )
        if attempt_to_send_text_bool is False:
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

        if dry_run is True:
            logging.info(
                "DRY RUN: Would send the following text to individual with document id: %s",
                document_id,
            )
            with open(f"{current_batch_id}_texts.txt", "a", encoding="utf-8") as file:
                file.write(phone_number)
                file.write("\n")
                file.write(text_body)
                file.write("\n")
                file.write("\n")
                file.close()
            continue
        # Send text message to individual
        logging.info("Sending text to individual with document id: %s", document_id)
        firestore_individual_path = f"twilio_messages/{document_id}"
        try:
            response = twilio_client.messages.create(
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
                    "last_update": datetime.datetime.now(datetime.timezone.utc),
                    "phone_numbers": ArrayUnion([phone_number]),
                    "state_code": state_code,
                    "external_id": external_id,
                    "batch_ids": ArrayUnion([current_batch_id]),
                    "po_names": ArrayUnion([po_name]),
                    "districts": ArrayUnion([district]),
                },
                merge=True,
            )

            # Next, update their message level doc
            firestore_message_path = f"twilio_messages/{document_id}/lsu_eligibility_messages/eligibility_{current_batch_id}"
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
                    "state_code": state_code,
                    "external_id": external_id,
                    "batch_id": current_batch_id,
                    "po_name": po_name,
                    "district": district,
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


def get_opt_out_document_ids(
    firestore_client: FirestoreClientImpl, state_code: str
) -> set[str]:
    """Get all document ids of jii level documents associated with individuals that
    have opted out of text messages.
    """
    twilio_ref = firestore_client.get_collection(collection_path="twilio_messages")
    doc_query = twilio_ref.where(
        filter=FieldFilter("opt_out_type", "in", OPT_OUT_KEY_WORDS)
    ).where(filter=FieldFilter("state_code", "==", state_code))
    opt_out_document_ids = {jii_doc.id for jii_doc in doc_query.stream()}
    logging.info(
        "%s individuals from %s have opted-out of texts.",
        len(opt_out_document_ids),
        state_code,
    )
    logging.info("Document ids of individuals who have opted-out from %s: ", state_code)
    logging.info(opt_out_document_ids)
    return opt_out_document_ids


def store_batch_id(
    firestore_client: FirestoreClientImpl,
    current_batch_id: str,
    message_type: str,
    redeliver_failed_messages: bool,
    bigquery_view: str,
    state_code: str,
) -> None:
    """
    Store the batch_id and Big Query View of the current launch in the Firestore database.
    The batch_id will be stored as a document in the batch_ids collection.
    """
    firestore_batch_id_path = f"batch_ids/{current_batch_id}"
    logging.info(
        "Storing batch_id %s and Big Query View %s in Firestore",
        current_batch_id,
        bigquery_view,
    )
    firestore_client.set_document(
        document_path=firestore_batch_id_path,
        data={
            "message_type": message_type.upper(),
            "redelivery": redeliver_failed_messages,
            "bigquery_view": bigquery_view,
            "state_code": state_code,
        },
    )


def attempt_to_send_text(
    redeliver_failed_messages: bool,
    document_id: str,
    external_ids_to_retry: set,
    opt_out_document_ids: set,
    message_type: str,
    initial_text_document_ids: set,
    eligibility_text_document_ids_to_text_timestamp: Dict[str, datetime.datetime],
    resend_eligibility_texts: bool,
    previous_batch_id: Optional[str] = None,
) -> bool:
    """
    Checks whether or not to attempt to send a text message to an individual.

    Returns either True or False
    """
    if previous_batch_id is not None and redeliver_failed_messages is True:
        # Filter to only send texts to individuals if previous text failed
        if document_id not in external_ids_to_retry:
            return False

    # Check that current document_id has not opted-out
    if document_id in opt_out_document_ids:
        logging.info(
            "JII with document id %s has opted-out of receiving texts. Will not attempt to send message.",
            document_id,
        )
        return False

    # If this is an initial/welcome text, and the individual has already
    # received an initial/welcome text in the past, do not attempt to send
    # them an initial/welcome text
    if message_type == MessageType.INITIAL_TEXT.value:
        if document_id in initial_text_document_ids:
            logging.info(
                "Individual with document id [%s] has already received initial text. Do not attempt to send initial text.",
                document_id,
            )
            return False

    # If this is an eligibility text, and the individual has not
    # received an initial/welcome text in the past, do not attempt to send
    # them an eligibility text
    if message_type == MessageType.ELIGIBILITY_TEXT.value:
        if document_id not in initial_text_document_ids:
            logging.info(
                "Individual with document id [%s] has not received initial text. Do not attempt to send eligibility text.",
                document_id,
            )
            return False

    # If this is an eligibility text, and the individual has
    # received an eligibility text in the past 90 days, do not attempt to send
    # them an eligibility text
    if (
        message_type == MessageType.ELIGIBILITY_TEXT.value
        and eligibility_text_document_ids_to_text_timestamp.get(document_id) is not None
    ):
        text_timestamp = eligibility_text_document_ids_to_text_timestamp.get(
            document_id
        )
        ninety_days_ago = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(days=90)
        if text_timestamp > ninety_days_ago and resend_eligibility_texts is False:  # type: ignore[operator]
            logging.info(
                "Individual with document id [%s] has already received an eligibility text in the past 90 days. Do not attempt to send eligibility text.",
                document_id,
            )
            return False

        if text_timestamp > ninety_days_ago and resend_eligibility_texts is True:  # type: ignore[operator]
            logging.info(
                "Individual with document id [%s] has already received an eligibility text in the past 90 days. Re-send eligibility text anyway.",
                document_id,
            )
        elif text_timestamp < ninety_days_ago:  # type: ignore[operator]
            logging.info(
                "Individual with document id [%s] received an eligibility text over 90 days ago. Re-send eligibility text.",
                document_id,
            )

    return True


def get_initial_and_eligibility_doc_ids(
    firestore_client: FirestoreClientImpl,
    state_code: str,
) -> Tuple[set, Dict]:
    """Get all document_ids for individuals who already received a text (initial or eligibility).
    Store and return document_ids for initial text messages as a set.
    Store and return document_ids for eligibility texts as a dictionary that maps the document_id to the timestamp of the
    eligibility text.
    These structures are used later to determine if/who we should attempt to send texts to.
    """

    message_ref = firestore_client.get_collection_group(
        collection_path="lsu_eligibility_messages"
    )
    message_query = message_ref.where(
        filter=FieldFilter("status", "==", ExternalSystemRequestStatus.SUCCESS.value)
    ).where(filter=FieldFilter("state_code", "==", state_code))
    initial_text_document_ids = set()
    eligibility_text_document_ids_to_text_timestamp: Dict[
        str, datetime.datetime
    ] = defaultdict()
    for message_doc in message_query.stream():
        message_dict = message_doc.to_dict()
        if message_dict is None:
            continue

        message_doc_id = message_doc.reference.path.split("/")[1]
        if message_dict["message_type"] == MessageType.INITIAL_TEXT.value:
            initial_text_document_ids.add(message_doc_id)
        elif message_dict["message_type"] == MessageType.ELIGIBILITY_TEXT.value:
            text_timestamp = message_dict["timestamp"]
            eligibility_text_document_ids_to_text_timestamp[
                message_doc_id
            ] = text_timestamp

    return initial_text_document_ids, eligibility_text_document_ids_to_text_timestamp


def update_statuses_from_previous_batch(
    twilio_client: TwilioClient,
    firestore_client: FirestoreClientImpl,
    previous_batch_id: str,
) -> set:
    """
    Given a previous_batch_id (identifies a previous run of the send_jii_texts.py script), poll the Twilio API
    for the status of each message in that batch. If the status does not match the one we have in the Firestore database,
    update the status of that message.
    """
    previous_batch_id_split = previous_batch_id.split("_")
    date_sent = f"{previous_batch_id_split[2]}-{previous_batch_id_split[0]}-{previous_batch_id_split[1]}"
    # Construct a set of external_ids in which messages were undelivered
    external_ids_to_retry: Set[str] = set()
    # Query Twilio for message attempts
    all_attempted_messages = twilio_client.messages.list(
        from_=get_secret("jii_twilio_phone_number"), date_sent=date_sent
    )

    for attempted_message in all_attempted_messages:
        message_status = attempted_message.status
        message_sid = attempted_message.sid
        error_code = attempted_message.error_code

        # Get the previously stored message
        jii_messages_ref = firestore_client.get_collection_group(
            collection_path="lsu_eligibility_messages"
        )
        query = jii_messages_ref.where(
            filter=FieldFilter("message_sid", "==", message_sid)
        )
        jii_updates_docs = query.stream()

        external_ids = update_status_helper(
            message_status=message_status,
            firestore_client=firestore_client,
            jii_updates_docs=jii_updates_docs,
            error_code=error_code,
        )
        external_ids_to_retry.update(external_ids)

    logging.info(
        "Number of phone numbers with undelivered messages from %s: %s",
        date_sent,
        len(external_ids_to_retry),
    )
    logging.info(
        "External_ids with undelivered messages from %s: %s",
        date_sent,
        external_ids_to_retry,
    )
    return external_ids_to_retry


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()

    if args.project_id is not None:
        with local_project_id_override(args.project_id):
            send_jii_texts(
                bigquery_view=args.bigquery_view,
                dry_run=args.dry_run,
                callback_url=args.callback_url,
                message_type=args.message_type,
                previous_batch_id=args.previous_batch_id_to_update_status_for,
                redeliver_failed_messages=args.redeliver_failed_messages,
                resend_eligibility_texts=args.resend_eligibility_texts,
            )
    else:
        send_jii_texts(
            bigquery_view=args.bigquery_view,
            dry_run=args.dry_run,
            callback_url=args.callback_url,
            message_type=args.message_type,
            previous_batch_id=args.previous_batch_id_to_update_status_for,
            redeliver_failed_messages=args.redeliver_failed_messages,
            resend_eligibility_texts=args.resend_eligibility_texts,
        )
