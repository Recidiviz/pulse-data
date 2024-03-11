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
This file contains a script to calculate and output analytics relating to the Idaho
LSU JII Text Message Pilot.

Usage:
python -m recidiviz.case_triage.jii.id_lsu_message_analytics
"""
import itertools

from google.cloud.firestore_v1 import FieldFilter

from recidiviz.case_triage.jii.send_id_lsu_texts import OPT_OUT_KEY_WORDS
from recidiviz.case_triage.util import MessageType
from recidiviz.case_triage.workflows.utils import ExternalSystemRequestStatus
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def calculate_id_lsu_text_analytics() -> None:
    """
    Calculates various statistics for the ID LSU JII Texting Pilot.
    Statistics of interest include:

    General
        - Total number of individuals that have opted-out

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
        - Total number of unique individuals who have replied
        - Total number of replies
        - The actual reply texts
    """
    firestore_client = FirestoreClientImpl(project_id="jii-pilots")

    ### General ###

    print("--- General ---")
    # Total number of individuals opted-out
    twilio_ref = firestore_client.get_collection(collection_path="twilio_messages")
    doc_query = twilio_ref.where(
        filter=FieldFilter("opt_out_type", "in", OPT_OUT_KEY_WORDS)
    )
    opt_out_document_ids = {jii_doc.id for jii_doc in doc_query.stream()}
    num_opt_out_document_ids = len(opt_out_document_ids)
    print(
        f"Total number of individuals that have opted-out: {num_opt_out_document_ids}"
    )
    print("")

    ### Initial Texts ###

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
    initial_text_document_ids = {
        message_doc.reference.path.split("/")[1]
        for message_doc in message_query.stream()
    }
    num_initial_text_document_ids = len(initial_text_document_ids)

    # Total number of individuals who have opted-out AND have successfully received a initial text
    initial_opt_out_intersection = initial_text_document_ids.intersection(
        opt_out_document_ids
    )
    total_initial_opt_out = len(initial_opt_out_intersection)
    # Percentage of individuals who have opted-out that have successfully received a initial text
    percent_initial_opt_out = round(
        ((total_initial_opt_out / num_initial_text_document_ids) * 100), 2
    )

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
        status_percent = round(((raw_status_count / total_message_count) * 100), 2)
        print(
            f"{raw_status_count} initial texts ({status_percent}%) have the following status: {raw_status}"
        )
    print("")

    ### Eligibility Texts ###

    print("--- Eligibility Texts ---")
    # Total number of individuals that have successfully received a eligibility text
    eligibility_message_ref = firestore_client.get_collection_group(
        collection_path="lsu_eligibility_messages"
    )
    eligbility_message_query = eligibility_message_ref.where(
        filter=FieldFilter("message_type", "==", MessageType.ELIGIBILITY_TEXT.value)
    ).where(
        filter=FieldFilter("status", "==", ExternalSystemRequestStatus.SUCCESS.value)
    )
    eligibility_text_document_ids = {
        eligbility_message_doc.reference.path.split("/")[1]
        for eligbility_message_doc in eligbility_message_query.stream()
    }
    num_eligibility_text_document_ids = len(eligibility_text_document_ids)

    # Total number of individuals who have opted-out AND have successfully received a eligibility text
    eligibility_opt_out_intersection = eligibility_text_document_ids.intersection(
        opt_out_document_ids
    )
    total_eligibility_opt_out = len(eligibility_opt_out_intersection)
    # Percentage of individuals who have opted-out that have successfully received a eligibility text
    percent_eligibility_opt_out = round(
        ((total_eligibility_opt_out / num_eligibility_text_document_ids) * 100), 2
    )

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
        eligibility_status_percent = round(
            ((eligibility_raw_status_count / eligibility_total_message_count) * 100), 2
        )
        print(
            f"{eligibility_raw_status_count} eligibility texts ({eligibility_status_percent}%) have the following status: {eligibility_raw_status}"
        )
    print("")

    ### Replies ###

    print("--- Replies ---")

    replies_ref = firestore_client.get_collection_group(
        collection_path="twilio_messages"
    )
    replies_query = replies_ref.where(filter=FieldFilter("responses", "!=", "null"))
    doc_id_to_replies = {}
    for reply_doc in replies_query.stream():
        jii_reply_doc = reply_doc.to_dict()
        if jii_reply_doc is None:
            continue
        doc_id_to_replies[reply_doc.reference.path.split("/")[1]] = jii_reply_doc[
            "responses"
        ]

    num_total_replies = len(
        list(itertools.chain.from_iterable(doc_id_to_replies.values()))
    )
    num_unique_repliers = len(doc_id_to_replies.keys())
    print(f"Total number of unique individuals who have replied: {num_unique_repliers}")
    print(f"Total number of replies: {num_total_replies}")

    for doc_id, replies in doc_id_to_replies.items():
        for reply in replies:
            print(
                f"Individual with document_id {doc_id} replied '{reply['response']}' on {reply['response_date'].date()}"
            )


if __name__ == "__main__":
    # args = create_parser().parse_args()
    with local_project_id_override(GCP_PROJECT_STAGING):
        calculate_id_lsu_text_analytics()
