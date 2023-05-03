# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
