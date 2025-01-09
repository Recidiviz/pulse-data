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

INITIAL_TEXT = "Hi {given_name}, we’re reaching out on behalf of the Idaho Department of Correction (IDOC). We will send information to this number about your eligibility for opportunities such as the Limited Supervision Unit (LSU), which offers a lower level of supervision.\n\nIf you have questions, reach out to {po_name}."
FULLY_ELIGIBLE_TEXT = "Hi {given_name}, IDOC records show that you have met most of the requirements to be considered for the Limited Supervision Unit (LSU). LSU is a lower level of supervision with monthly online check-ins for those in good standing (for example, no new misdemeanors).\n\nThis message does not mean that you have already been transferred to LSU. To fully qualify, your PO will need to check that:\n1. You have no active no-contact orders.\n2. You have made payments towards your fines/fees at least 3 months in a row (even small payments count).\n\nIf you believe you meet these conditions or have questions, please contact {po_name}{additional_contact}; they can confirm your eligibility and help you apply. Approval for LSU is not guaranteed."
FULLY_ELIGIBLE_EXCEPT_FFR = "Hi {given_name}, IDOC records show that you meet most requirements, but have a remaining step in order to be considered for the Limited Supervision Unit (LSU). If you make fine/fee payments at least 3 months in a row (even small payments count), you may qualify.\n\nLSU is a lower level of supervision with monthly online check-ins for those in good standing (for example, no active no-contact orders, and no new misdemeanors). It also reduces your monthly supervision fee from $60 to $30. LSU is optional, and this message does not mean you have already been transferred.\n\nYou can reach out to {po_name}{additional_contact} to make payments or with questions about LSU. If you meet all the criteria, they can help you apply."
MISSING_NEGATIVE_DA_OR_INCOME = "Hi {given_name}, IDOC records show you may soon be eligible to apply for the Limited Supervision Unit (LSU), a lower level of supervision for those meeting all their required conditions (for example, no active no-contact orders and no new misdemeanors).\n\nLSU is optional but offers benefits like monthly online check-ins and reduced supervision fees ($30 instead of $60/month).\n\nTo qualify, you’ll need to provide your PO with {missing_documentation}.\n\nAdditionally, you must have paid towards your fines/fees at least 3 months in a row (even small payments count).\n\nIf interested, contact {po_name}{additional_contact} to verify your eligibility and apply."
MISSING_NEGATIVE_DA_DOCUMENTATION = "a negative urine analysis test"
MISSING_INCOME_DOCUMENTATION = "documents like pay-stubs proving you have full-time employment, are a student, or other income sources like a pension"
MISSING_NEGATIVE_DA_AND_INCOME = "Hi {given_name}, IDOC records show you may soon be eligible to apply for the Limited Supervision Unit (LSU), a lower level of supervision for those meeting all their required conditions.\n\nLSU is optional but offers benefits like monthly online check-ins and reduced supervision fees ($30 instead of $60/month).\n\nTo qualify, you’ll need to provide your PO with:\n1. Documents like pay-stubs proving you have full-time employment, are a student, or other income sources like a pension, and\n2. A negative urine analysis test.\n\nAdditionally, you must have paid towards your fines/fees at least 3 months in a row (even small payments count).\n\nIf interested, contact {po_name}{additional_contact} to verify your eligibility and apply."
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
        district = individual["district"].lower().strip()
        fully_eligible = individual["is_eligible"]
        fines_n_fees_denials = individual["fines_n_fees_denials"]
        # We do not expect jii with fully_eligible=False and fines_n_fees_denials=True to be included in the BigQuery table as we do not want to text these individuals
        if fully_eligible is False and fines_n_fees_denials is True:
            logging.info(
                "JII with external_id %s has fully_eligible=False and fines_n_fees_denials=True. We do not want to text this individual.",
                external_id,
            )
            continue

        text_body = """"""
        text_body += StrictStringFormatter().format(
            INITIAL_TEXT, given_name=given_name, po_name=po_name
        )
        text_body += ALL_CLOSER

        initial_text_messages_dict["external_id"] = external_id
        initial_text_messages_dict["phone_num"] = phone_num
        initial_text_messages_dict["text_body"] = text_body
        initial_text_messages_dict["po_name"] = po_name
        initial_text_messages_dict["district"] = district
        initial_text_messages_dicts.append(initial_text_messages_dict)
        logging.info("Initial text constructed for external_id: %s", external_id)

    return initial_text_messages_dicts


def generate_eligibility_text_messages_dict(
    bq_output: bigquery.QueryJob,
) -> List[Dict[str, str]]:
    """Iterates through the data (bigquery output). For each bigquery row (individual),
    we check if the individual is either fully eligible, missing ua, and or missing
    employment eligibility. Depending on these criteria, we then call
    construct_text_body() to construct a text message body for that individual.

    This function returns a list of dictionaries. Each dictionary contains an individual's
    external_id, phone number, the text body, their po name, and their district.
    """
    eligibility_text_messages_dicts: List = []

    for individual in bq_output:
        eligibility_text_messages_dict: Dict[str, str] = defaultdict()
        fully_eligible = False
        missing_negative_da_within_90_days = False
        missing_income_verified_within_3_months = False
        fines_n_fees_denials = individual["fines_n_fees_denials"]

        if individual["is_eligible"] is True:
            fully_eligible = True
        elif set(individual["ineligible_criteria"]) == {
            "NEGATIVE_DA_WITHIN_90_DAYS",
            "US_IX_INCOME_VERIFIED_WITHIN_3_MONTHS",
        }:
            missing_negative_da_within_90_days = True
            missing_income_verified_within_3_months = True
        elif individual["ineligible_criteria"] == ["NEGATIVE_DA_WITHIN_90_DAYS"]:
            missing_negative_da_within_90_days = True
        elif individual["ineligible_criteria"] == [
            "US_IX_INCOME_VERIFIED_WITHIN_3_MONTHS"
        ]:
            missing_income_verified_within_3_months = True
        else:
            continue

        external_id = str(individual["external_id"])
        phone_num = str(individual["phone_number"])

        # We do not expect jii with fully_eligible=False and fines_n_fees_denials=True to be included in the BigQuery table as we do not want to text these individuals
        if fully_eligible is False and fines_n_fees_denials is True:
            logging.info(
                "JII with external_id %s has fully_eligible=False and fines_n_fees_denials=True. We do not want to text this individual.",
                external_id,
            )
            continue

        po_name = individual["po_name"].title()
        district = individual["district"].lower().strip()
        text_body = construct_text_body(
            individual=individual,
            fully_eligible=fully_eligible,
            missing_negative_da_within_90_days=missing_negative_da_within_90_days,
            missing_income_verified_within_3_months=missing_income_verified_within_3_months,
            fines_n_fees_denials=fines_n_fees_denials,
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
        logging.info("fully_eligible: %s", fully_eligible)
        logging.info("fines_n_fees_denials: %s", fines_n_fees_denials)
        logging.info(
            "missing_negative_da_within_90_days: %s", missing_negative_da_within_90_days
        )
        logging.info(
            "missing_income_verified_within_3_months: %s",
            missing_income_verified_within_3_months,
        )

    return eligibility_text_messages_dicts


def construct_text_body(
    individual: Dict[str, str],
    fully_eligible: bool,
    missing_negative_da_within_90_days: bool,
    missing_income_verified_within_3_months: bool,
    fines_n_fees_denials: bool,
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

    if fully_eligible is True and fines_n_fees_denials is False:
        text_body += StrictStringFormatter().format(
            FULLY_ELIGIBLE_TEXT,
            given_name=given_name,
            po_name=po_name,
            additional_contact=additional_contact,
        )
    elif fully_eligible is True and fines_n_fees_denials is True:
        text_body += StrictStringFormatter().format(
            FULLY_ELIGIBLE_EXCEPT_FFR,
            given_name=given_name,
            po_name=po_name,
            additional_contact=additional_contact,
        )
    elif (
        missing_negative_da_within_90_days is True
        and missing_income_verified_within_3_months is False
    ):
        text_body += StrictStringFormatter().format(
            MISSING_NEGATIVE_DA_OR_INCOME,
            given_name=given_name,
            missing_documentation=MISSING_NEGATIVE_DA_DOCUMENTATION,
            po_name=po_name,
            additional_contact=additional_contact,
        )
    elif (
        missing_negative_da_within_90_days is False
        and missing_income_verified_within_3_months is True
    ):
        text_body += StrictStringFormatter().format(
            MISSING_NEGATIVE_DA_OR_INCOME,
            given_name=given_name,
            missing_documentation=MISSING_INCOME_DOCUMENTATION,
            po_name=po_name,
            additional_contact=additional_contact,
        )
    elif (
        missing_negative_da_within_90_days is True
        and missing_income_verified_within_3_months is True
    ):
        text_body += StrictStringFormatter().format(
            MISSING_NEGATIVE_DA_AND_INCOME,
            given_name=given_name,
            po_name=po_name,
            additional_contact=additional_contact,
        )

    text_body += LEARN_MORE
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
