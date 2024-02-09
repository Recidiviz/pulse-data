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
from ast import literal_eval
from collections import defaultdict
from typing import Dict

from google.cloud import bigquery

from recidiviz.utils.string import StrictStringFormatter

INITIAL_TEXT = "Hi {given_name}, we are reaching out on behalf of the Idaho Department of Corrections to provide you with information about your supervision. You may reply STOP if you would love to stop receiving these messages at any time."
FULLY_ELIGIBLE_TEXT = "Hi {given_name}, according to IDOC records, you may meet the criteria for the Limited Supervision Unit (LSU). To confirm whether you’re eligible, please reach out to your PO, {po_name}, so that they can conduct a review of your case and ensure that all criteria are met. Approval for LSU is not guaranteed.\n"
MISSING_NEGATIVE_UA_OR_INCOME_OPENER = "Hi {given_name}, according to IDOC records, you have just a few things left to do before you possibly meet the criteria for the Limited Supervision Unit (LSU). To become eligible, please do the following when possible:\n"
MISSING_INCOME_BULLET = "- Verify your employment status, full-time student status, or adequate lawful income from non-employment sources. You can do so by sharing a recent paycheck stub or other documentation with your PO, {po_name}.\n"
MISSING_NEGATIVE_UA_BULLET = (
    "- Visit the office to provide a negative urinary analysis test\n"
)
MISSING_NEGATIVE_UA_OR_INCOME_CLOSER = "Please reach out to {po_name} so that they can conduct a review of your case and ensure that all criteria is met. Approval for LSU is not guaranteed.\n"
ALL_CLOSER = "To stop receiving these messages, reply STOP. To provide feedback, reply to this number. Note that we will not respond to any messages received at this time, please reach out to {po_name} with any additional questions or comments."


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
        text_body = StrictStringFormatter().format(INITIAL_TEXT, given_name=given_name)
        external_id_to_phone_num_to_text_dict[external_id] = {phone_num: text_body}

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

    text_body += StrictStringFormatter().format(ALL_CLOSER, po_name=po_name)
    return text_body
