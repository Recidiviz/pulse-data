# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_MI. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_mi_custom_enum_parsers.<function name>
"""
import re
from typing import Optional

from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_supervision_violated_condition import (
    StateSupervisionViolatedConditionType,
)


def parse_agent_type(
    raw_text: str,
) -> Optional[StateAgentType]:
    """Parse agent type based on employee position raw text"""

    supervision_officer_positions = [
        "PAROLE PROBATION",
        "PAROLE/PRBTN",
        "PAROLE/PROBATION",
    ]

    # if position indicates parole/probation, then set as supervision officer
    if any(position in raw_text.upper() for position in supervision_officer_positions):
        return StateAgentType.SUPERVISION_OFFICER

    # else if not the above case but position is provided, set as internal unknown
    if raw_text:
        return StateAgentType.INTERNAL_UNKNOWN

    return None


def parse_condition(
    raw_text: str,
) -> Optional[StateSupervisionViolatedConditionType]:
    """Parse condition type based on special condition descriptions"""

    # BL NOTE: I went through the list of special condition descriptions and categorized
    #          them accordingly and determined the following set of rules.  However, this
    #          might not necessarily categorize correctly for new special conditions that
    #          are written and then added later...

    raw_text = raw_text.upper()
    short_description = raw_text.split("@@")[1]
    description = raw_text.split("@@")[2]

    if (
        re.search("FEE|PAY|COST|FINE", description)
        # there's probably a more elegant way to do this, but we don't want to flag descriptions with words
        # that simply have the above search terms in them, and within the condition descriptions, the
        # relevant words that occur in this scenario are "COSTUME" and "DEFINE"
        and re.search("COSTUME|DEFINE", description) is None
    ):
        return StateSupervisionViolatedConditionType.FINANCIAL
    if "TREATMENT" in raw_text:
        return StateSupervisionViolatedConditionType.TREATMENT_COMPLIANCE
    if (
        re.search("CONTACT FIELD AGENT|CONTACT W/COURT", short_description)
        or re.search("CONTACT AGENT| MONTHLY REPORTING", description)
        or re.search("NOTIFY|REPORT ANY", description)
    ):
        return StateSupervisionViolatedConditionType.FAILURE_TO_NOTIFY
    # BL NOTE: I wasn't sure if we should include failures to complete drug use tests as part of
    #          StateSupervisionViolatedConditionType.SUBSTANCE? For now I'm excluding them since
    #          the gitbook description includes just substance use
    if re.search("NO DRUGS|NOT USE ALCOHOL", short_description) or (
        re.search("DRUG|SUBSTANCE", description)
        and re.search("TEST|ASSESSMENT", description) is None
    ):
        return StateSupervisionViolatedConditionType.SUBSTANCE
    # There were conditions like "Do not have contact with so-and-so's place of employment" so
    # excluing those here because I'm assuming those shouldn't fall under this category
    if ("EMPLOY" in description and "CONTACT" not in description) or (
        re.search("NOT WORK/NO WORK", short_description)
    ):
        return StateSupervisionViolatedConditionType.EMPLOYMENT
    if re.search("PERMISSION|APPROVAL|AGENT CONSENT", description):
        return StateSupervisionViolatedConditionType.FAILURE_TO_NOTIFY
    if re.search("CONVICTED|CRIMINAL LAW", description):
        return StateSupervisionViolatedConditionType.LAW

    # if the raw_text is not null and didn't get categorized anywhere above, default to SPECIAL_CONDITIONS
    if raw_text:
        return StateSupervisionViolatedConditionType.SPECIAL_CONDITIONS

    return None
