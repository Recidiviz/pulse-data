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
File with helper functions used by recidiviz/case_triage/jii/send_id_lsu_texts.py
"""
import datetime
import logging
from ast import literal_eval
from collections import defaultdict
from typing import Dict, Generator, Optional

from google.cloud import bigquery

from recidiviz.case_triage.workflows.utils import (
    TwilioStatus,
    get_consolidated_status,
    get_jii_texting_error_message,
)
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.string import StrictStringFormatter

INITIAL_TEXT = "Hi {given_name}, we are reaching out to notify you that we will be sending text messages to this phone number on behalf of the Idaho Department of Correction. These texts will share information about options available on supervision."
FULLY_ELIGIBLE_TEXT = "Hi {given_name}, according to records from the Idaho Department of Correction, you may be eligible for the Limited Supervision Unit (LSU). To learn more about LSU, visit rviz.co/id_lsu. If you’re interested in learning more, you can reach out to {po_name} or your current PO. They can answer any questions and review your case to see if you've met all the requirements. You may or may not be approved for LSU."
MISSING_NEGATIVE_UA_OR_INCOME_OPENER = "Hi {given_name}, according to records from the Idaho Department of Correction, you may be close to being eligible for the Limited Supervision Unit (LSU). To learn more about LSU visit rviz.co/id_lsu. This program is optional, but if you are interested in joining, you can do the following to become eligible:\n"
MISSING_INCOME_BULLET = "\n- Verify that you have a job, are a full-time student, or have enough lawful income from sources other than a job. You can do so by sharing a recent paycheck stub or other documents with your PO, {po_name}.\n"
MISSING_NEGATIVE_UA_BULLET = "\n- Visit the parole and probation office to provide a negative urine analysis test\n"
MISSING_NEGATIVE_UA_OR_INCOME_CLOSER = "\nIf you’re interested in learning more, you can reach out to {po_name} or your current PO. You may or may not be approved for LSU."
ALL_CLOSER = "\n\nReply STOP if you would like to stop receiving these messages at any time. To resubscribe, reply START. We're currently unable to respond to messages sent to this number. Your PO can answer any questions you may have."


def generate_initial_text_messages_dict(
    bq_output: bigquery.QueryJob,
) -> Dict[str, Dict[str, str]]:
    """Iterates through the data (bigquery output). For each bigquery row (individual),
    we construct an initial text message body for that individual, regardless of their
    eligibility status. This allows jii to opt-out of future text messages.

    This function returns a dictionary that maps external ids to phone number strings to text message strings.
    """
    external_id_to_phone_num_to_text_dict: Dict[str, Dict[str, str]] = defaultdict(dict)

    for individual in bq_output:
        external_id = str(individual["external_id"])
        phone_num = str(individual["phone_number"])
        given_name = literal_eval(individual["person_name"])["given_names"].title()
        text_body = """"""
        text_body += StrictStringFormatter().format(INITIAL_TEXT, given_name=given_name)
        text_body += ALL_CLOSER
        external_id_to_phone_num_to_text_dict[external_id] = {phone_num: text_body}
        logging.info("Initial text constructed for external_id: %s", external_id)

    return external_id_to_phone_num_to_text_dict


def generate_eligibility_text_messages_dict(
    bq_output: bigquery.QueryJob,
) -> Dict[str, Dict[str, str]]:
    """Iterates through the data (bigquery output). For each bigquery row (individual),
    we check if the individual is either fully eligible, missing ua, and or missing
    employment eligibility. Depending on these criteria, we then call
    construct_text_body() to construct a text message body for that individual.

    This function returns a dictionary that maps external ids to phone number strings to text message strings.
    """
    external_id_to_phone_num_to_text_dict: Dict[str, Dict[str, str]] = defaultdict(dict)

    for individual in bq_output:
        fully_eligible = False
        missing_negative_ua_within_90_days = False
        missing_income_verified_within_3_months = False
        if individual["is_eligible"] is True:
            fully_eligible = True
        elif set(individual["ineligible_criteria"]) == {
            "NEGATIVE_UA_WITHIN_90_DAYS",
            "US_IX_INCOME_VERIFIED_WITHIN_3_MONTHS",
        }:
            missing_negative_ua_within_90_days = True
            missing_income_verified_within_3_months = True
        elif individual["ineligible_criteria"] == ["NEGATIVE_UA_WITHIN_90_DAYS"]:
            missing_negative_ua_within_90_days = True
        elif individual["ineligible_criteria"] == [
            "US_IX_INCOME_VERIFIED_WITHIN_3_MONTHS"
        ]:
            missing_income_verified_within_3_months = True
        else:
            continue

        external_id = str(individual["external_id"])
        phone_num = str(individual["phone_number"])
        text_body = construct_text_body(
            individual=individual,
            fully_eligible=fully_eligible,
            missing_negative_ua_within_90_days=missing_negative_ua_within_90_days,
            missing_income_verified_within_3_months=missing_income_verified_within_3_months,
        )
        external_id_to_phone_num_to_text_dict[external_id] = {phone_num: text_body}
        logging.info("Eligibility text constructed for external_id: %s", external_id)
        logging.info("fully_eligible: %s", fully_eligible)
        logging.info(
            "missing_negative_ua_within_90_days: %s", missing_negative_ua_within_90_days
        )
        logging.info(
            "missing_income_verified_within_3_months: %s",
            missing_income_verified_within_3_months,
        )

    return external_id_to_phone_num_to_text_dict


def construct_text_body(
    individual: Dict[str, str],
    fully_eligible: bool,
    missing_negative_ua_within_90_days: bool,
    missing_income_verified_within_3_months: bool,
) -> str:
    """Constructs a text message (string) to be sent to a given individual based on their
    eligibility criteria.
    """
    text_body = """"""
    given_name = literal_eval(individual["person_name"])["given_names"].title()
    po_name = individual["po_name"].title()

    if fully_eligible is True:
        text_body += StrictStringFormatter().format(
            FULLY_ELIGIBLE_TEXT, given_name=given_name, po_name=po_name
        )
    elif (
        missing_negative_ua_within_90_days is True
        or missing_income_verified_within_3_months is True
    ):
        text_body += StrictStringFormatter().format(
            MISSING_NEGATIVE_UA_OR_INCOME_OPENER, given_name=given_name
        )

    if missing_income_verified_within_3_months is True:
        text_body += StrictStringFormatter().format(
            MISSING_INCOME_BULLET, po_name=po_name
        )
    if missing_negative_ua_within_90_days is True:
        text_body += MISSING_NEGATIVE_UA_BULLET
    if (
        missing_negative_ua_within_90_days is True
        or missing_income_verified_within_3_months is True
    ):
        text_body += StrictStringFormatter().format(
            MISSING_NEGATIVE_UA_OR_INCOME_CLOSER, po_name=po_name
        )

    text_body += ALL_CLOSER
    return text_body


def update_status_helper(
    message_status: Optional[str],
    firestore_client: FirestoreClientImpl,
    jii_updates_docs: Generator,
    error_code: Optional[str],
) -> set:
    """
    Iterates through documents from the JII Firestore database and updates the document's
    status, status_last_updated, and raw_status fields if the document's raw_status does
    not match the message_status from Twilio.

    Additionally, this helper returns a set of external_ids in which the previously sent
    message has the status 'undelivered'. This set of external_ids will be used to
    attempt to resend previously undelivered messages.
    """
    external_ids = set()
    for doc in jii_updates_docs:
        jii_message = doc.to_dict()

        if jii_message is None:
            continue

        if message_status == TwilioStatus.UNDELIVERED.value:
            external_id = doc.reference.path.split("/")[1]
            external_ids.add(external_id)

        # This endpoint will be hit multiple times per message, so check here if this is a new status change from
        # what we already have in Firestore.
        if jii_message.get("raw_status", "") != message_status:
            logging.info(
                "Updating Twilio message status for doc: [%s] with status: [%s]",
                doc.reference.path,
                message_status,
            )
            doc_update = {
                "status": get_consolidated_status(message_status),
                "status_last_updated": datetime.datetime.now(datetime.timezone.utc),
                "raw_status": message_status,
            }
            if error_code:
                doc_update["error_code"] = error_code
                error_message = get_jii_texting_error_message(error_code)
                doc_update["errors"] = [error_message]
            firestore_client.set_document(
                doc.reference.path,
                doc_update,
                merge=True,
            )

    return external_ids
