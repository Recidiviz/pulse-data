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
This file contains a script to calculate and output analytics relating to the JII Text Message Pilot.

Usage:
python -m recidiviz.case_triage.jii.jii_texts_message_analytics \
    --initial-batch-id 02_28_2024_13_48_04 \
    --eligibility-batch-id 03_01_2024_14_47_43
"""
import argparse
import datetime
import itertools
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from google.cloud.firestore_v1 import FieldFilter
from google.cloud.firestore_v1.base_document import DocumentSnapshot

from recidiviz.case_triage.jii.send_jii_texts import OPT_OUT_KEY_WORDS
from recidiviz.case_triage.util import MessageType
from recidiviz.case_triage.workflows.utils import ExternalSystemRequestStatus
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--initial-batch-id",
        required=True,
        help="A string representing a datetime of a previous initial_text run of the send_jii_texts.py script.",
    )
    parser.add_argument(
        "--eligibility-batch-id",
        help="A string representing a datetime of a previous eligibility_text run of the send_jii_texts.py script.",
    )
    return parser


def calculate_text_analytics(
    initial_batch_id: str, eligibility_batch_id: Optional[str] = None
) -> None:
    """
    If only a initial_batch_id (a string representing a datetime of a previous initial_text run of the send_jii_texts.py script) is provided,
    calculates various statistics for the ID LSU JII Texting Pilot for after the initial_batch_id.

    If a initial_batch_id and eligibility_batch_id (a string representing a datetime of a previous eligibility_text run of the send_jii_texts.py script) are provided,
    calculates various statistics for the ID LSU JII Texting Pilot for between the initial_batch_id and eligibility_batch_id as well as after the eligibility_batch_id.

    Statistics of interest include:

    1. When only initial_batch_id provided
    Initial Texts
        - Total number of initial text attempts
        - Total number of individuals that have successfully received a initial text
        - Total number of individuals who have opted-out AND have successfully received a initial text
        - Percentage of individuals who have opted-out that have successfully received a initial text
        - Total number and percentage of initial texts broken down by text status

    Replies
        - Total number of unique individuals who have replied
        - Total number of replies
        - The actual reply texts

    2. When both initial_batch_id and eligibility_batch_id provided
    Initial Texts
        - Total number of initial text attempts
        - Total number of individuals that have successfully received a initial text
        - Total number of individuals who have opted-out AND have successfully received a initial text
        - Percentage of individuals who have opted-out that have successfully received a initial text
        - Total number and percentage of initial texts broken down by text status

    Eligibility Texts
        - Total number of eligibility text attempts
        - Total number of individuals that have successfully received a eligibility text
        - Total number of individuals who have opted-out AND have successfully received a eligibility text
        - Percentage of individuals who have opted-out that have successfully received a eligibility text
        - Total number and percentage of eligibility texts broken down by text status

    Replies
        - Total number of unique individuals who have replied between the 2 batches
        - Total number of unique individuals who have replied after the eligibility_batch_id
        - Total number of replies between the 2 batches
        - Total number of replies after the eligibility_batch_id
        - The actual reply texts (between the 2 batches)
        - The actual reply texts (after the eligibility_batch_id)
    """
    firestore_client = FirestoreClientImpl(project_id="jii-pilots")

    # First, convert initial_batch_id and eligibility_batch_id to datetimes
    initial_batch_date = convert_batch_id_to_utc(initial_batch_id)
    if eligibility_batch_id is not None:
        eligibility_batch_date = convert_batch_id_to_utc(eligibility_batch_id)
    else:
        eligibility_batch_date = None

    # Get a list of all opted out individuals
    # We use this to intersect it with the individuals whom we sent texts to in order to
    # figure out which of the individuals we texted have since opted-out
    twilio_ref = firestore_client.get_collection(collection_path="twilio_messages")
    doc_query = twilio_ref.where(
        filter=FieldFilter("opt_out_type", "in", OPT_OUT_KEY_WORDS)
    ).where(filter=FieldFilter("last_opt_out_update", ">=", initial_batch_date))
    initial_opt_out_document_ids = set()
    eligibility_opt_out_document_ids = set()
    if eligibility_batch_date is not None:
        for jii_doc in doc_query.stream():
            if jii_doc.to_dict() is None:
                continue
            jii_last_opt_out_update = jii_doc.to_dict()["last_opt_out_update"]  # type: ignore[index]
            if jii_last_opt_out_update < eligibility_batch_date:
                initial_opt_out_document_ids.add(jii_doc.id)
            elif jii_last_opt_out_update >= eligibility_batch_date:
                eligibility_opt_out_document_ids.add(jii_doc.id)
    else:
        initial_opt_out_document_ids = {jii_doc.id for jii_doc in doc_query.stream()}

    ### Initial Texts ###
    initial_text_document_ids = _print_initial_analytics(
        initial_batch_id=initial_batch_id,
        firestore_client=firestore_client,
        opt_out_document_ids=initial_opt_out_document_ids,
    )

    ### Eligibility Texts ###
    if eligibility_batch_id is not None:
        eligibility_text_document_ids = _print_eligibility_analytics(
            eligibility_batch_id=eligibility_batch_id,
            firestore_client=firestore_client,
            opt_out_document_ids=eligibility_opt_out_document_ids,
        )
    else:
        eligibility_text_document_ids = None

    ### Replies ###
    _print_reply_analytics(
        firestore_client=firestore_client,
        initial_batch_id=initial_batch_id,
        initial_batch_date=initial_batch_date,
        initial_text_document_ids=initial_text_document_ids,
        eligibility_batch_id=eligibility_batch_id,
        eligibility_batch_date=eligibility_batch_date,
        eligibility_text_document_ids=eligibility_text_document_ids,
    )


def _print_initial_analytics(
    initial_batch_id: str,
    firestore_client: FirestoreClientImpl,
    opt_out_document_ids: Set[str],
) -> Set[str]:
    """
    Helper function to print analytics for after a given initial_batch_id.
    """
    print("--- Initial Texts ---")

    # Total number of individuals that have successfully received a initial text
    message_ref = firestore_client.get_collection_group(
        collection_path="lsu_eligibility_messages"
    )
    message_query = message_ref.where(
        filter=FieldFilter("message_type", "==", MessageType.INITIAL_TEXT.value)
    ).where(
        filter=FieldFilter("status", "==", ExternalSystemRequestStatus.SUCCESS.value)
    )

    initial_text_document_ids = set()
    for message_doc in message_query.stream():
        doc_batch_id = _get_batch_id_from_doc(message_doc)
        if doc_batch_id == initial_batch_id:
            doc_id = _get_doc_id_from_doc(message_doc)
            initial_text_document_ids.add(doc_id)
    num_initial_text_document_ids = len(initial_text_document_ids)

    # Total number of individuals who have opted-out AND have successfully received a initial text
    initial_opt_out_intersection = initial_text_document_ids.intersection(
        opt_out_document_ids
    )
    total_initial_opt_out = len(initial_opt_out_intersection)
    # Percentage of individuals who have opted-out that have successfully received a initial text
    if num_initial_text_document_ids > 0:
        percent_initial_opt_out = round(
            ((total_initial_opt_out / num_initial_text_document_ids) * 100), 2
        )
    else:
        percent_initial_opt_out = 0

    # Initial Text Message Status Breakdowns
    initial_message_query = message_ref.where(
        filter=FieldFilter("message_type", "==", MessageType.INITIAL_TEXT.value)
    )
    raw_status_to_count = {
        "undelivered": 0,
        "failed": 0,
        "canceled": 0,
        "delivered": 0,
        "read": 0,
        "accepted": 0,
        "scheduled": 0,
        "queued": 0,
        "sending": 0,
        "sent": 0,
    }

    for message_doc in initial_message_query.stream():
        doc_batch_id = _get_batch_id_from_doc(message_doc)
        if doc_batch_id != initial_batch_id:
            continue
        jii_message = message_doc.to_dict()
        if jii_message is None:
            continue
        raw_status = jii_message.get("raw_status")
        if (
            raw_status is not None
            and raw_status_to_count.get(raw_status.lower()) is not None
        ):
            raw_status_to_count[raw_status.lower()] += 1

    total_message_count = sum(raw_status_to_count.values())
    print(f"Total number of initial text attempts: {total_message_count}")
    print(
        f"Total number of individuals that have successfully received a initial text: {num_initial_text_document_ids}"
    )
    print(
        f"Total number of individuals who have opted-out AND have successfully received a initial text: {total_initial_opt_out}"
    )
    print(
        f"Percentage of individuals who have opted-out that have successfully received a initial text: {percent_initial_opt_out}%"
    )

    for raw_status, raw_status_count in raw_status_to_count.items():
        if total_message_count > 0:
            status_percent = round(((raw_status_count / total_message_count) * 100), 2)
        else:
            status_percent = 0
        print(
            f"{raw_status_count} initial texts ({status_percent}%) have the following status: {raw_status}"
        )
    print("")
    return initial_text_document_ids


def _print_eligibility_analytics(
    eligibility_batch_id: str,
    firestore_client: FirestoreClientImpl,
    opt_out_document_ids: Set[str],
) -> Set[str]:
    """
    Helper function to print analytics for after a given eligibility_batch_id.
    """
    print("--- Eligibility Texts ---")
    # Total number of individuals that have successfully received a eligibility text
    eligibility_message_ref = firestore_client.get_collection_group(
        collection_path="lsu_eligibility_messages"
    )
    eligibility_message_query = eligibility_message_ref.where(
        filter=FieldFilter("message_type", "==", MessageType.ELIGIBILITY_TEXT.value)
    ).where(
        filter=FieldFilter("status", "==", ExternalSystemRequestStatus.SUCCESS.value)
    )

    eligibility_text_document_ids = set()
    for eligibility_message_doc in eligibility_message_query.stream():
        doc_batch_id = _get_batch_id_from_doc(eligibility_message_doc)
        if doc_batch_id == eligibility_batch_id:
            doc_id = _get_doc_id_from_doc(eligibility_message_doc)
            eligibility_text_document_ids.add(doc_id)
    num_eligibility_text_document_ids = len(eligibility_text_document_ids)

    # Total number of individuals who have opted-out AND have successfully received a eligibility text
    eligibility_opt_out_intersection = eligibility_text_document_ids.intersection(
        opt_out_document_ids
    )
    total_eligibility_opt_out = len(eligibility_opt_out_intersection)
    # Percentage of individuals who have opted-out that have successfully received a eligibility text
    if num_eligibility_text_document_ids > 0:
        percent_eligibility_opt_out = round(
            ((total_eligibility_opt_out / num_eligibility_text_document_ids) * 100),
            2,
        )
    else:
        percent_eligibility_opt_out = 0

    # Eligibility Text Message Status Breakdowns
    eligibility_message_query = eligibility_message_ref.where(
        filter=FieldFilter("message_type", "==", MessageType.ELIGIBILITY_TEXT.value)
    )
    eligibility_raw_status_to_count = {
        "undelivered": 0,
        "failed": 0,
        "canceled": 0,
        "delivered": 0,
        "read": 0,
        "accepted": 0,
        "scheduled": 0,
        "queued": 0,
        "sending": 0,
        "sent": 0,
    }

    for eligibility_message_doc in eligibility_message_query.stream():
        doc_batch_id = _get_batch_id_from_doc(eligibility_message_doc)
        if doc_batch_id != eligibility_batch_id:
            continue
        eligibility_jii_message = eligibility_message_doc.to_dict()
        if eligibility_jii_message is None:
            continue
        eligibility_raw_status = eligibility_jii_message.get("raw_status")
        if (
            eligibility_raw_status is not None
            and eligibility_raw_status_to_count.get(eligibility_raw_status.lower())
            is not None
        ):
            eligibility_raw_status_to_count[eligibility_raw_status.lower()] += 1

    eligibility_total_message_count = sum(eligibility_raw_status_to_count.values())
    print(
        f"Total number of eligibility text attempts: {eligibility_total_message_count}"
    )
    print(
        f"Total number of individuals that have successfully received a eligibility text: {num_eligibility_text_document_ids}"
    )
    print(
        f"Total number of individuals who have opted-out AND have successfully received a eligibility text: {total_eligibility_opt_out}"
    )
    print(
        f"Percentage of individuals who have opted-out that have successfully received a eligibility text: {percent_eligibility_opt_out}%"
    )

    for (
        eligibility_raw_status,
        eligibility_raw_status_count,
    ) in eligibility_raw_status_to_count.items():
        if eligibility_total_message_count > 0:
            eligibility_status_percent = round(
                (
                    (eligibility_raw_status_count / eligibility_total_message_count)
                    * 100
                ),
                2,
            )
        else:
            eligibility_status_percent = 0
        print(
            f"{eligibility_raw_status_count} eligibility texts ({eligibility_status_percent}%) have the following status: {eligibility_raw_status}"
        )
    print("")
    return eligibility_text_document_ids


def _print_reply_analytics(
    firestore_client: FirestoreClientImpl,
    initial_batch_id: str,
    initial_batch_date: datetime.datetime,
    initial_text_document_ids: Set[str],
    eligibility_batch_id: Optional[str] = None,
    eligibility_batch_date: Optional[datetime.datetime] = None,
    eligibility_text_document_ids: Optional[Set[str]] = None,
) -> None:
    """Helper function to print reply analytics."""
    print("--- Replies ---")

    replies_ref = firestore_client.get_collection_group(
        collection_path="twilio_messages"
    )
    replies_query = replies_ref.where(filter=FieldFilter("responses", "!=", "null"))
    text_type_to_doc_id_to_replies: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
        "initial_text": {},
        "eligibility_text": {},
    }
    for reply_doc in replies_query.stream():
        jii_reply_doc = reply_doc.to_dict()
        if jii_reply_doc is None:
            continue

        # Replies for both initial and eligibility messages
        if eligibility_batch_id is not None:
            # Initial message replies
            if reply_doc.id in initial_text_document_ids:
                individual_replies = []
                for response_map in jii_reply_doc["responses"]:
                    if (
                        response_map["response_date"] > initial_batch_date
                        and response_map["response_date"] <= eligibility_batch_date
                    ):
                        individual_replies.append(response_map)
                if len(individual_replies) > 0:
                    text_type_to_doc_id_to_replies["initial_text"][
                        reply_doc.id
                    ] = individual_replies
            # Eligibility message replies
            if (
                eligibility_text_document_ids is not None
                and reply_doc.id in eligibility_text_document_ids
            ):
                individual_replies = []
                for response_map in jii_reply_doc["responses"]:
                    if response_map["response_date"] > eligibility_batch_date:
                        individual_replies.append(response_map)
                if len(individual_replies) > 0:
                    text_type_to_doc_id_to_replies["eligibility_text"][
                        reply_doc.id
                    ] = individual_replies
        # Replies for just initial messages
        else:
            # Initial message replies
            if reply_doc.id in initial_text_document_ids:
                individual_replies = []
                for response_map in jii_reply_doc["responses"]:
                    if response_map["response_date"] > initial_batch_date:
                        individual_replies.append(response_map)
                if len(individual_replies) > 0:
                    text_type_to_doc_id_to_replies["initial_text"][
                        reply_doc.id
                    ] = individual_replies

    num_total_initial_replies = len(
        list(
            itertools.chain.from_iterable(
                text_type_to_doc_id_to_replies["initial_text"].values()
            )
        )
    )
    num_total_eligibility_replies = len(
        list(
            itertools.chain.from_iterable(
                text_type_to_doc_id_to_replies["eligibility_text"].values()
            )
        )
    )
    num_total_replies = num_total_initial_replies + num_total_eligibility_replies
    num_unique_initial_repliers = len(
        text_type_to_doc_id_to_replies["initial_text"].keys()
    )
    if eligibility_batch_id is None:
        print(
            f"Total number of unique individuals who have replied after {initial_batch_id}: {num_unique_initial_repliers}"
        )
        print(f"Total number of replies after {initial_batch_id}: {num_total_replies}")
    else:
        num_unique_eligibility_repliers = len(
            text_type_to_doc_id_to_replies["eligibility_text"].keys()
        )
        print(
            f"Total number of unique individuals who have replied between {initial_batch_id}-{eligibility_batch_id}: {num_unique_initial_repliers}"
        )
        print(
            f"Total number of unique individuals who have replied after {eligibility_batch_id}: {num_unique_eligibility_repliers}"
        )
        print(
            f"Total number of replies between {initial_batch_id}-{eligibility_batch_id}: {num_total_initial_replies}"
        )
        print(
            f"Total number of replies after {eligibility_batch_id}: {num_total_eligibility_replies}"
        )

    print("")
    print("Initial Text Replies")
    for doc_id, replies in text_type_to_doc_id_to_replies["initial_text"].items():
        for reply in replies:
            print(
                f"Individual with document_id {doc_id} replied '{reply['response']}' on {reply['response_date'].date()}"
            )

    if eligibility_batch_id is not None:
        print("")
        print("Eligibility Text Replies")
        for doc_id, replies in text_type_to_doc_id_to_replies[
            "eligibility_text"
        ].items():
            for reply in replies:
                print(
                    f"Individual with document_id {doc_id} replied '{reply['response']}' on {reply['response_date'].date()}"
                )


def convert_batch_id_to_utc(batch_id: str) -> datetime.datetime:
    """This helper function converts a string batch_id from the current (machine) timezone
    to a string adjusted_batch_id in UTC. initial_batch_id and eligibility_batch_id strings
    are generated using the local/machine timezone while Firestore dates are in UTC.
    We need this conversion for comparison.
    """
    print("batch_id:", batch_id)
    # Default hour_delta to 5. We assume that this will get overwritten in the logic below
    hour_delta = 5

    # local_time gives us the local (machine) datetime including the local timezone
    local_time = datetime.datetime.now().astimezone()
    print("local_time:", local_time)
    # hour_delta is the number of hours that the local_time is behind UTC
    if local_time.tzinfo is not None:
        utc_offset = local_time.tzinfo.utcoffset(local_time)
        if utc_offset is not None:
            hour_delta = int(24 - (utc_offset.seconds / 60 / 60))

    # adjusted_batch_id represents the batch_id in UTC
    adjusted_batch_id = datetime.datetime.strptime(
        batch_id, "%m_%d_%Y_%H_%M_%S"
    ).replace(tzinfo=datetime.timezone.utc) - datetime.timedelta(hours=hour_delta)
    print("adjusted_batch_id:", adjusted_batch_id)
    return adjusted_batch_id


def _get_doc_id_from_doc(doc: DocumentSnapshot) -> str:
    """Helper function that returns a document's document_id"""
    return doc.reference.path.split("/")[1]


def _get_batch_id_from_doc(doc: DocumentSnapshot) -> str:
    """Helper function that returns a document's batch_id"""
    return doc.reference.path.split("/")[-1].split("eligibility_")[1]


def get_all_batch_ids() -> None:
    firestore_client = FirestoreClientImpl(project_id="jii-pilots")
    twilio_ref = firestore_client.get_collection_group(
        collection_path="lsu_eligibility_messages"
    )
    batch_ids: Dict[str, str] = defaultdict()

    for jii_message in twilio_ref.stream():
        jii_message_doc = jii_message.to_dict()
        if jii_message_doc is None:
            continue

        batch_id = _get_batch_id_from_doc(doc=jii_message)
        batch_ids[batch_id] = jii_message_doc["message_type"]

    print(f"All batch_ids (and their message_type): {batch_ids}")
    print(f"{len(batch_ids)} total batch_ids")


if __name__ == "__main__":
    args = create_parser().parse_args()
    with local_project_id_override(GCP_PROJECT_STAGING):
        calculate_text_analytics(
            initial_batch_id=args.initial_batch_id,
            eligibility_batch_id=args.eligibility_batch_id,
        )
