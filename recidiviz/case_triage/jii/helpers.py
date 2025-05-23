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
File with helper functions used by recidiviz/case_triage/jii/send_jii_texts.py
"""
import datetime
import enum
import logging
from ast import literal_eval
from collections import defaultdict
from typing import Any, Dict, Generator, List, Optional

from google.cloud import bigquery
from google.cloud.firestore_v1 import DocumentSnapshot

from recidiviz.case_triage.workflows.utils import (
    TwilioStatus,
    get_consolidated_status,
    get_jii_texting_error_message,
)
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.string import StrictStringFormatter

INITIAL_TEXT = "Hi {given_name}, we’re reaching out on behalf of the Idaho Department of Correction (IDOC). You’re now subscribed to receive updates about potential opportunities such as the Limited Supervision Unit (LSU), which offers a lower level of supervision.\n\nWe’ll let you know by texting this number if you meet the criteria for specific programs. Receiving this message does not mean you’re already eligible for any opportunity.\n\nIf you have questions, reach out to {po_name}."
FULLY_ELIGIBLE_TEXT = "Hi {given_name}, IDOC records show that you have met most of the requirements to be considered for the Limited Supervision Unit (LSU). LSU is a lower level of supervision with monthly online check-ins for those in good standing (for example, no new misdemeanors).\n\nThis message does not mean that you have already been transferred to LSU. To fully qualify, your PO will need to check that:\n1. You have no active no-contact orders.\n2. You have made payments towards your fines/fees at least 3 months in a row (even small payments count).\n\nIf you believe you meet these conditions or have questions, please contact {po_name}{additional_contact}; they can confirm your eligibility and help you apply. Approval for LSU is not guaranteed."
FULLY_ELIGIBLE_EXCEPT_FFR = "Hi {given_name}, IDOC records show you meet most requirements but have a remaining step to be considered for the Limited Supervision Unit (LSU). If you make fine/fee payments for 3 months in a row (even small payments count), you may qualify.\n\nLSU is a lower level of supervision with monthly online check-ins for those in good standing (for example, no active no-contact orders, and no new misdemeanors). It reduces your monthly supervision fee from $60 to $30. LSU is optional, and this message does not mean you have already been transferred.\n\nYou can reach out to {po_name}{additional_contact} to make payments or with questions about LSU. They must verify that you are in compliance with your conditions of supervision. If you are, they can help you apply."
MISSING_INCOME = "Hi {given_name}, IDOC records show you may soon be eligible to apply for the Limited Supervision Unit (LSU), a lower level of supervision for those meeting all their required conditions. This message does not mean you are already eligible.\n\nLSU is optional but offers benefits like monthly online check-ins and reduced supervision fees ($30 vs. $60/month).\n\nTo qualify, you’ll need to provide your PO with documents like pay-stubs proving you have full-time employment, are a student, or other income sources like a pension.\n\nYou must also have paid towards your fines/fees at least 3 months in a row (even small payments count).\n\nIf interested, contact {po_name}{additional_contact}. They must first confirm your eligibility, then can help you apply."
MISSING_NEGATIVE_DA = "Hi {given_name}, IDOC records show you may soon be eligible to apply for the Limited Supervision Unit (LSU), a lower level of supervision for those meeting all their required conditions. This message does not mean you are already eligible.\n\nLSU is optional but offers benefits like monthly online check-ins and reduced supervision fees ($30 instead of $60/month).\n\nTo qualify, you’ll need to provide your PO with a negative urine analysis test.\n\nAdditionally, you must have paid towards your fines/fees at least 3 months in a row (even small payments count).\n\nIf interested, contact {po_name}{additional_contact}. They must verify that you are in compliance with your conditions of supervision. If you are, they can help you apply."
MISSING_NEGATIVE_DA_AND_INCOME = "Hi {given_name}, IDOC records show you may soon be eligible to apply for the Limited Supervision Unit (LSU), a lower level of supervision for those meeting all their required conditions.\n\nLSU is optional but offers benefits like monthly online check-ins and reduced supervision fees ($30 instead of $60/month).\n\nTo qualify, you’ll need to provide your PO with:\n1. Documents like pay-stubs proving you have full-time employment, are a student, or other income sources like a pension, and\n2. A negative urine analysis test.\n\nAdditionally, you must have paid towards your fines/fees at least 3 months in a row (even small payments count).\n\nIf interested, contact {po_name}{additional_contact}. They must first confirm your eligibility, then can help you apply."
VISIT = "\n\nSee all requirements at rviz.co/id_lsu."
LEARN_MORE = "\n\nLearn more at rviz.co/id_lsu."
ALL_CLOSER = "\n\nReply STOP to stop receiving these messages at any time. We’re unable to respond to messages sent to this number."

D1_ADDITIONAL_CONTACT = " or email D1Connect@idoc.idaho.gov"
D2_ADDITIONAL_CONTACT = " or contact a specialist at district2Admin@idoc.idaho.gov"
D3_ADDITIONAL_CONTACT = (
    " or a specialist at specialistsd3@idoc.idaho.gov or (208) 454-7601"
)
D4_ADDITIONAL_CONTACT = (
    " or a specialist at d4ppspecialists@idoc.idaho.gov or 208-327-7008"
)
D5_ADDITIONAL_CONTACT = " or a specialist at D5general@idoc.idaho.gov or 208-644-7268"
D6_ADDITIONAL_CONTACT = ""
D7_ADDITIONAL_CONTACT = (
    " or a specialist at d7.pp.specialist@idoc.idaho.gov or (208) 701-7130"
)

GOOD_DISTRICTS = ["1", "2", "3", "4", "5", "6", "7"]


class GroupIds(enum.Enum):
    ELIGIBLE_MISSING_FINES_AND_FEES = "ELIGIBLE_MISSING_FINES_AND_FEES"
    FULLY_ELIGIBLE = "FULLY_ELIGIBLE"
    MISSING_DA = "MISSING_DA"
    MISSING_INCOME_VERIFICATION = "MISSING_INCOME_VERIFICATION"
    TWO_MISSING_CRITERIA = "TWO_MISSING_CRITERIA"


def generate_initial_text_messages_dict(
    bq_output: bigquery.QueryJob,
) -> List[Dict[str, str]]:
    """Iterates through the data (bigquery output). For each bigquery row (individual),
    we construct an initial text message body for that individual, regardless of their
    eligibility status. This allows jii to opt-out of future text messages.

    This function returns a list of dictionaries. Each dictionary contains an individual's
    external_id, phone number, the text body, their po name, and their district.
    """
    initial_text_messages_dicts: List = []

    for individual in bq_output:
        initial_text_messages_dict: Dict[str, str] = defaultdict()
        external_id = str(individual["external_id"])
        phone_num = str(individual["phone_number"])
        given_name = literal_eval(individual["person_name"])["given_names"].title()
        po_name = individual["po_name"].title()
        group_id = individual["group_id"]

        district = individual["district"].lower().strip()
        if district.split("district")[-1].strip() not in GOOD_DISTRICTS:
            continue

        text_body = construct_initial_text_body(given_name=given_name, po_name=po_name)

        initial_text_messages_dict["external_id"] = external_id
        initial_text_messages_dict["phone_num"] = phone_num
        initial_text_messages_dict["text_body"] = text_body
        initial_text_messages_dict["po_name"] = po_name
        initial_text_messages_dict["district"] = district
        initial_text_messages_dict["group_id"] = group_id
        initial_text_messages_dicts.append(initial_text_messages_dict)
        logging.info("Initial text constructed for external_id: %s", external_id)

    return initial_text_messages_dicts


def construct_initial_text_body(
    given_name: str,
    po_name: str,
) -> str:
    text_body = """"""
    text_body += StrictStringFormatter().format(
        INITIAL_TEXT, given_name=given_name, po_name=po_name
    )
    text_body += ALL_CLOSER
    return text_body


def generate_eligibility_text_messages_dict(
    bq_output: bigquery.QueryJob,
) -> List[Dict[str, str]]:
    """Iterates through the data (bigquery output). For each bigquery row (individual),
    we check if the individual is either fully eligible, missing ua, and or missing
    employment eligibility. Depending on these criteria, we then call
    construct_eligibility_text_body() to construct a text message body for that individual.

    This function returns a list of dictionaries. Each dictionary contains an individual's
    external_id, phone number, the text body, their po name, and their district.
    """
    eligibility_text_messages_dicts: List = []

    for individual in bq_output:
        district = individual["district"].lower().strip()
        if district.split("district")[-1].strip() not in GOOD_DISTRICTS:
            continue

        eligibility_text_messages_dict: Dict[str, str] = defaultdict()

        external_id = str(individual["external_id"])
        phone_num = str(individual["phone_number"])

        po_name = individual["po_name"].title()
        text_body = construct_eligibility_text_body(
            individual=individual,
            po_name=po_name,
            district=district,
        )
        eligibility_text_messages_dict["external_id"] = external_id
        eligibility_text_messages_dict["phone_num"] = phone_num
        eligibility_text_messages_dict["text_body"] = text_body
        eligibility_text_messages_dict["po_name"] = po_name
        eligibility_text_messages_dict["district"] = district
        eligibility_text_messages_dicts.append(eligibility_text_messages_dict)
        logging.info("Eligibility text constructed for external_id: %s", external_id)

    return eligibility_text_messages_dicts


def construct_eligibility_text_body(
    individual: Dict[str, str],
    po_name: str,
    district: str,
) -> str:
    """Constructs a text message (string) to be sent to a given individual based on their
    eligibility criteria.
    """
    text_body = """"""
    given_name = literal_eval(individual["person_name"])["given_names"].title()

    if district == "district 1":
        additional_contact = D1_ADDITIONAL_CONTACT
    elif district == "district 2":
        additional_contact = D2_ADDITIONAL_CONTACT
    elif district == "district 3":
        additional_contact = D3_ADDITIONAL_CONTACT
    elif district == "district 4":
        additional_contact = D4_ADDITIONAL_CONTACT
    elif district == "district 5":
        additional_contact = D5_ADDITIONAL_CONTACT
    elif district == "district 6":
        additional_contact = D6_ADDITIONAL_CONTACT
    elif district == "district 7":
        additional_contact = D7_ADDITIONAL_CONTACT
    else:
        raise ValueError(
            f"Unexpected district. We expected {individual} to belong to districts 1, 2, 3, 4, 5, 6 or 7. They belong to: {district}."
        )

    group_id = individual["group_id"]

    if group_id == GroupIds.FULLY_ELIGIBLE.value:
        text_body += StrictStringFormatter().format(
            FULLY_ELIGIBLE_TEXT,
            given_name=given_name,
            po_name=po_name,
            additional_contact=additional_contact,
        )
        text_body += LEARN_MORE
    elif group_id == GroupIds.TWO_MISSING_CRITERIA.value:
        text_body += StrictStringFormatter().format(
            MISSING_NEGATIVE_DA_AND_INCOME,
            given_name=given_name,
            po_name=po_name,
            additional_contact=additional_contact,
        )
        text_body += VISIT
    elif group_id == GroupIds.MISSING_DA.value:
        text_body += StrictStringFormatter().format(
            MISSING_NEGATIVE_DA,
            given_name=given_name,
            po_name=po_name,
            additional_contact=additional_contact,
        )
        text_body += VISIT
    elif group_id == GroupIds.MISSING_INCOME_VERIFICATION.value:
        text_body += StrictStringFormatter().format(
            MISSING_INCOME,
            given_name=given_name,
            po_name=po_name,
            additional_contact=additional_contact,
        )
        text_body += VISIT
    elif group_id == GroupIds.ELIGIBLE_MISSING_FINES_AND_FEES.value:
        text_body += StrictStringFormatter().format(
            FULLY_ELIGIBLE_EXCEPT_FFR,
            given_name=given_name,
            po_name=po_name,
            additional_contact=additional_contact,
        )
        text_body += LEARN_MORE
    else:
        raise ValueError(
            f"Unexpected group_id. {individual} belongs to group_id: {group_id}."
        )

    text_body += ALL_CLOSER
    return text_body


def update_status_helper(
    message_status: Optional[str],
    firestore_client: FirestoreClientImpl,
    jii_updates_docs: Generator[DocumentSnapshot, Any, Any],
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
                error_message = get_jii_texting_error_message(str(error_code))
                doc_update["errors"] = [error_message]
            firestore_client.set_document(
                doc.reference.path,
                doc_update,
                merge=True,
            )

    return external_ids
