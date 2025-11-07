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
"""Custom enum parsers functions for US_CA. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_ca_custom_enum_parsers.<function name>
"""

import re
from typing import Optional

from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)


def parse_supervision_violation_response_decision_type(
    raw_text: str,
) -> Optional[StateSupervisionViolationResponseDecision]:
    """Parses the violation response decision type from the ResponseType and the
    ResponseDecisionType.
    """
    (
        response_type,
        response_decision_type,
    ) = raw_text.upper().split("@@")

    # We always expect "COP@@COP", but overspecifying so that if something unexpected
    # comes through, we will fail.
    if response_type == "COP" and response_decision_type == "COP":
        return StateSupervisionViolationResponseDecision.CONTINUANCE

    if response_type == "DISMISS" and response_decision_type == "DISMISS":
        return StateSupervisionViolationResponseDecision.VIOLATION_UNFOUNDED

    # If someone dies while there is a violation being levied against them, we mark this
    # as OTHER. To me, this is intended to reflect the fact that the violation wasn't
    # withdrawn, but is no longer going to proceed.
    if response_type == "DEATH":
        return StateSupervisionViolationResponseDecision.OTHER

    # If someone is referred for revocation, that violation can be dismissed, they can
    # be returned to parole but found guilty of the violation, or they can be revoked
    if response_type == "REFER":
        if re.search("DISMISS|NOT TRUE", response_decision_type):
            return StateSupervisionViolationResponseDecision.VIOLATION_UNFOUNDED

        if response_decision_type == "RETURN TO PAROLE":
            # TODO(#20387) #1. may actually be "extended" or "conditions change"? unsure
            return StateSupervisionViolationResponseDecision.CONTINUANCE

        if re.search("REVOKE", response_decision_type):
            return StateSupervisionViolationResponseDecision.REVOCATION

        if response_decision_type == "TRUE":
            return StateSupervisionViolationResponseDecision.REVOCATION

        # GOOD CAUSE is an odd state to appear as a "Final" decision -- we generally
        # need casenotes to figure out what is going on in their situation. Only around
        # %0.1 of individuals end up in this state.
        # CRIMINAL CONVICTION means that the person was convicted of a new crime during
        # the revocation process. Our SME told us this was more like "DEFER". Even fewer
        # of these than GOOD CAUSE, so also not worrying.
        # MAX CDD/EXPIRED JURISDICTION means the revocation was not completed because
        # the person was already relased due to their controlling discharge date.
        if response_decision_type in (
            "GOOD CAUSE",
            "CRIMINAL CONVICTION",
            "MAX CDD/EXPIRED JURISDICTION",
        ):
            return StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN

    # TODO(#20387) #3 We don't get this data yet -- it's in "Case Closed" data.
    if response_type == "DEFER" and response_decision_type == "DEFER":
        return StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN

    return None


def parse_employment_status(
    raw_text: str,
) -> StateEmploymentPeriodEmploymentStatus:
    """
    Parses a given raw text to determine the employment status based on keywords.

    Args:
        raw_text (str): The input text to be parsed for employment status.

    Returns:
        StateEmploymentPeriodEmploymentStatus: An enumeration representing the employment status.

    This function analyzes the `raw_text` to determine the employment status of an individual.
    It checks for specific keywords and patterns in the text to classify the employment status.

    Possible return values:
    - UNEMPLOYED: If "UNEMPLOYED" or "UN-EMPLOYED" is found in the text.
    - STUDENT: If the text exactly matches "STUDENT".
    - INTERNAL_UNKNOWN: If the text exactly matches "RETIRED".
    - EMPLOYED: If the text matches specific employment-related keywords, such as
      "SSI OFFICE", "SOCIAL SECURITY OFFICE", or "SOCIAL SECURITY ADMINISTRATION".
    - ALTERNATE_INCOME_SOURCE: If the text contains keywords related to alternate income sources,
      such as "GENERAL RELIEF", "BENEFITS", "GENERAL ASSISTANCE", "GEN ASSISTANCE",
      "VA ASSISTANCE", "DISABILITY ASSISTANCE", "SSI", "SOCIAL SECURITY", or "UNEMPLOYMENT".

    If none of the above conditions are met, it defaults to EMPLOYED as a conservative estimate.
    """
    if "UNEMPLOYED" in raw_text or "UN-EMPLOYED" in raw_text:
        return StateEmploymentPeriodEmploymentStatus.UNEMPLOYED

    if raw_text == "STUDENT":
        return StateEmploymentPeriodEmploymentStatus.STUDENT

    if raw_text == "RETIRED":
        return StateEmploymentPeriodEmploymentStatus.INTERNAL_UNKNOWN

    # it appears some people are employed by the Social Security Administration
    # so we identify them as employed here to simplify the logic below
    if raw_text in (
        "SSI OFFICE",
        "SOCIAL SECURITY OFFICE",
        "SOCIAL SECURITY ADMINISTRATION",
    ):
        return StateEmploymentPeriodEmploymentStatus.EMPLOYED_UNKNOWN_AMOUNT

    is_general_relief = "GENERAL RELIEF" in raw_text or "GENERALRELIEF" in raw_text
    is_benefits = "BENEFITS" in raw_text
    is_general_assistance = (
        "GENERAL ASSISTANCE" in raw_text
        or "GEN ASSISTANCE" in raw_text
        or "VA ASSISTANCE" in raw_text
        or "DISABILITY ASSISTANCE" in raw_text
    )
    is_ssi = (
        raw_text[:3] == "SSI"
        or "SOCIAL SECURITY" in raw_text
        or raw_text == "UNEMPLOYMENT"
    )

    if is_general_relief or is_benefits or is_general_assistance or is_ssi:
        return StateEmploymentPeriodEmploymentStatus.ALTERNATE_INCOME_SOURCE

    return StateEmploymentPeriodEmploymentStatus.EMPLOYED_UNKNOWN_AMOUNT
