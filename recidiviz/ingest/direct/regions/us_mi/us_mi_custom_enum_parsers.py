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

from recidiviz.common.constants.state.state_staff_role_period import (
    StateStaffRoleSubtype,
    StateStaffRoleType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violated_condition import (
    StateSupervisionViolatedConditionType,
)


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


limited_levels = [
    "14165",  # Probation Minimum Telephone Employed
    "14164",  # Probation Minimum Telephone Unemployed
    "14052",  # Parole Minimum Telephone Unemployed
    "14051",  # Parole Minimum Telephone Employed
]

minimum_levels = [
    "2294",  # Parole Minimum
    "13560",  # Parole Minimum Employed
    "13561",  # Parole Minimum Unemployed
    "14196",  # Parole Minimum In-Person Employed
    "2293",  # Parole Minimum Mail-In
    "14197",  # Parole Minimum In-Person Unemployed
    "3626",  # Probation Minimum
    "3625",  # Probation Minimum Mailin
    "14050",  # VO Parole Minimum InPerson Unemployed
    "14049",  # VO Parole Minimum InPerson Employed
    "13571",  # VO Parole Minimum Unemployed
    "13572",  # VO Parole Minimum Employed
    "13562",  # Probation Minimum Employed
    "13563",  # Probation Minimum Unemployed
    "19612",  # Parole Minimum Low Unemployed
    "13624",  # Parole Minimum Mailin Employed
    "13623",  # Parole Minimum Mailin Unemployed
    "13833",  # Parole Minimum Low Risk Employed
    "14199",  # Probation Minimum InPerson Employed
    "3795",  # Probation Minimum SSU Inpatient
    "14198",  # Probation Minimum InPerson Unemployed
    "13832",  # Parole Minimum Low Risk Unemployed
    "13622",  # Probation Minimum Mailin Employed
    "19616",  # VO Parole Minimum Low Unemployed
    "13621",  # Probation Minimum Mailin Unemployed
    "13819",  # Probation Minimum Low Risk Employed
    "13820",  # Probation Minimum Low Risk Unemployed
    "19613",  # Parole Minimum Low Employed
    "19615",  # Probation Minimum Low Employed
    "19617",  # VO Parole Minimum Low Employed
    "19614",  # Probation Minimum Low Unemployed
]

medium_levels = [
    "3627",  # Probation Medium Unemployed
    "3628",  # Probation Medium Employed
    "2282",  # Parole Medium Unemployed
    "2283",  # Parole Medium Employed
    "13575",  # VO Parole Medium Unemployed
    "13576",  # VO Parole Medium Employed
    "13577",  # SA Parole Medium Unemployed
    "13578",  # SA Parole Medium Employed
    "13573",  # VO/SA Parole Medium Unemployed
    "13574",  # VO/SA Parole Medium Employed
    "Level A (Medium)",  # From COMS
]

# GPS/EMS levels should be categorized under maximum as well if they're not marked as intensive
maximum_levels = [
    "2281",  # Parole Maximum Unemployed
    "3630",  # Probation Maximum Employed
    "3629",  # Probation Maximum Unemployed
    "7503",  # Probation Maximum SSU Unemployed
    "13584",  # SA Parole Maximum Unemployed
    "13583",  # SA Parole Maximum Employed
    "13579",  # VO/SA Parole Maximum Unemployed
    "13582",  # VO Parole Maximum Employed
    "13581",  # VO Parole Maximum Unemployed
    "2280",  # Parole Maximum Employed
    "13580",  # VO/SA Parole Maximum Employed
    "7502",  # Probation Maximum SSU Employed
    "13589",  # VO/SA Probation Parole Maximum SSU Unemployed
    "13586",  # SA Probation Parole Maximum SSU Unemployed
    "13585",  # SA Probation Parole Maximum SSU Employed
    "13587",  # VO Probation Parole Maximum SSU Unemployed
    "13588",  # VO Probation Parole Maximum SSU Employed
    "13590",  # VO/SA Probation Parole Maximum SSU Employed
    "3796",  # Probation Maximum SS
    "7318",  # CRP - EMS Combination Office Employed
    "7316",  # CRP - EMS Unemployed
    "7317",  # CRP - EMS Combination Office Unemployed
    "7315",  # CRP - EMS Employed
    "2288",  # CRP EMS
    "7173",  # CRP EMS Combination Office
    "7171",  # CRP - EMS Waiver- "13567", # VO/SA Parole EMS Employed
    "13827",  # VO Parole GPS Unemployed
    "13610",  # VO Parole SAI (Team) EMS Employed
    "13824",  # Probation GPS Unemployed
    "4057",  # Parole SAI EMS Unemployed Sub Abuse
    "13821",  # Parole GPS Unemployed
    "7507",  # Probation SAI (Team) EMS Unemployed
    "4053",  # Probation SAI EMS Unemployed Sub Abuse
    "4054",  # Probation SAI EMS Employed Sub Abuse
    "13614",  # VO/SA Parole SAI (Team) EMS Employed
    "13613",  # VO/SA Parole SAI (Team) EMS Unemployed
    "13825",  # SA Parole GPS Unemployed
    "2289",  # Probation EMS Employed
    "4056",  # Parole SAI EMS Employed Sub Abuse
    "13822",  # Parole GPS Employed
    "13568",  # VO Parole EMS Employed
    "13826",  # SA Parole GPS Employed
    "13601",  # VO/SA Parole SAI EMS Unemployed
    "13564",  # VO/SA Parole EMS Unemployed
    "13829",  # VO/SA Parole GPS Unemployed
    "13606",  # SA Parole SAI (Team) EMS Employed
    "13599",  # VO Parole SAI EMS Unemployed
    "5769",  # Probation EMS Unemployed
    "13566",  # SA Parole EMS Unemployed
    "13597",  # SA Parole SAI EMS Unemployed
    "7501",  # Parole SAI (Team) EMS Unemployed
    "7506",  # Probation SAI (Team) EMS Employed
    "13598",  # SA Parole SAI EMS Employed
    "13602",  # VO/SA Parole SAI EMS Employed
    "13830",  # VO/SA Parole GPS Employed
    "13569",  # SA Parole EMS Employed
    "3810",  # Parole EMS Unemployed
    "3803",  # Parole SAI Employed
    "3635",  # Parole SAI EMS Employed
    "13600",  # VO Parole SAI EMS Employed
    "13828",  # VO Parole GPS Employed
    "3636",  # Parole EMS Employed
    "13605",  # SA Parole SAI (Team) EMS Unemployed
    "13823",  # Probation GPS Employed
    "13565",  # VO Parole EMS Unemployed
    "4058",  # Parole SAI EMS Unemployed
    "2395",  # Probation SAI EMS Employed
    "7500",  # Parole SAI (Team) EMS Employed
]

# NOTE: categorizing all "Intensive" levels as high
high_levels = [
    "14193",  # VO/SA Parole Intensive Unemployed
    "14192",  # VO/SA Parole Intensive Employed
    "14168",  # VO/SA Parole Intensive EMS Employed
    "14189",  # SA Parole Intensive Unemployed
    "14218",  # SA Parole SAI Intensive EMS Unemployed
    "14213",  # VO/SA Parole Intensive GPS Unemployed
    "14187",  # Parole Intensive Unemployed
    "14191",  # VO Parole Intensive Unemployed
    "14169",  # VO/SA Parole Intensive EMS Unemployed
    "14209",  # VO Parole Intensive GPS Unemployed
    "14210",  # VO Parole Intensive EMS Unemployed
    "14207",  # SA Parole Intensive EMS Unemployed
    "14208",  # VO Parole Intensive GPS Employed
    "14188",  # SA Parole Intensive Employed
    "14212",  # VO/SA Parole Intensive GPS Employed
    "14204",  # SA Parole Intensive GPS Unemployed
    "14181",  # SA Parole SAI Intensive NonEMS Unemployed
    "14238",  # SA Probation Parole Intensive SSU NonEMS Employed
    "14237",  # Probation Intensive SSU NonEMS Unemployed
    "14190",  # VO Parole Intensive Employed
    "14221",  # VO/SA Parole SAI Intensive EMS Unemployed
    "14211",  # VO Parole Intensive EMS Employed
    "14241",  # VO Probation Parole Intensive SSU NonEMS Unemployed
    "14186",  # Parole Intensive Employed
    "14182",  # VO/SA Parole SAI Intensive NonEMS Unemployed
    "14205",  # SA Parole Intensive GPS Employed
    "14217",  # VO Parole SAI Intensive EMS Unemployed
    "14195",  # Probation Intensive Unemployed
    "14206",  # SA Parole Intensive EMS Employed
    "14185",  # Probation SAI Intensive NonEMS Unemployed
    "14219",  # SA Parole SAI Intensive EMS Employed
    "14202",  # Parole Intensive EMS Employed
    "14243",  # VO/SA Probation Parole Intensive SSU NonEMS Unemployed
    "14239",  # SA Probation Parole Intensive SSU NonEMS Unemployed
    "14203",  # Parole Intensive EMS Unemployed
    "14201",  # Parole Intensive GPS Unemployed
    "14249",  # VO Probation Parole Intensive SSU EMS Unemployed
    "14194",  # Probation Intensive Employed
    "14215",  # Parole SAI Intensive EMS Unemployed
    "14220",  # VO/SA Parole SAI Intensive EMS Employed
    "14223",  # Probation SAI Intensive EMS Unemployed
    "14236",  # Probation Intensive SSU NonEMS Employed
    "14216",  # VO Parole SAI Intensive EMS Employed
    "14247",  # SA Probation Parole Intensive SSU EMS Unemployed
    "14245",  # Probation Intensive SSU EMS Unemployed
    "14180",  # SA Parole SAI Intensive NonEMS Employed
    "14183",  # VO/SA Parole SAI Intensive NonEMS Employed
    "14173",  # Probation Intensive EMS Unemployed
    "14222",  # Probation SAI Intensive EMS Employed
    "14177",  # Parole SAI Intensive NonEMS Unemployed
    "14172",  # Probation Intensive EMS Employed
    "14176",  # Parole SAI Intensive NonEMS Employed
    "14178",  # VO Parole SAI Intensive NonEMS Employed
    "14179",  # VO Parole SAI Intensive NonEMS Unemployed
    "14184",  # Probation SAI Intensive NonEMS Employed
    "14171",  # Probation Intensive GPS Unemployed
    "14242",  # VO/SA Probation Parole Intensive SSU NonEMS Employed
    "14244",  # Probation Intensive SSU EMS Employed
    "14200",  # Parole Intensive GPS Employed
    "14214",  # Parole SAI Intensive EMS Employed
    "14250",  # VO/SA Probation Parole Intensive SSU EMS Employed
    "14170",  # Probation Intensive GPS Employed
]

unsupervised_levels = [
    "13457",  # Unavailable For Supervision
    "2285",  # Unsupervised Probation
]

warrant_levels = [
    "2286",  # Warrant Status
    "2394",  # Absconder Warrant Status
]

absconcion_levels = [
    "7483",  # Probation Absconder Warrant Status
    "7482",  # Parole Absconder Warrant Status
]

in_custody_levels = [
    "7405",  # Paroled in Custody
    "2292",  # Parole Minimum Administrative
    "3624",  # Probation Minimum Administrative
    "20007",  # Parole MPVU/PRF Placement
]


def parse_supervision_level(
    raw_text: str,
) -> Optional[StateSupervisionLevel]:
    """Parse supervision level based on assigned supervision level"""

    # If the supervision level raw text has "IN JAIL", let's map to IN_CUSTODY regardless of
    # what else the raw text says:
    if "IN JAIL" in raw_text:
        return StateSupervisionLevel.IN_CUSTODY

    # By default, use the first part of raw text for mapping
    #   For supervision levels coming from OMNI, this will be the whole raw text string
    #   For supervision levels coming from COMS, this will be the value coming from COMS_Supervision_Levels
    supervision_level_value = (raw_text.split("##")[0]).split("_")[0]

    # If the first part of the raw text is NONE, that means this is a supervision level that
    # came from COMS and the first part is NULL while the second part is non-NULL, so we should
    # use the second part (which comes from COMS_Supervision_Schedules)
    if supervision_level_value == "NONE":
        supervision_level_value = (raw_text.split("##")[0]).split("_")[1]

    # For OMNI
    if supervision_level_value in limited_levels:
        return StateSupervisionLevel.LIMITED

    if supervision_level_value in minimum_levels:
        return StateSupervisionLevel.MINIMUM

    if supervision_level_value in medium_levels:
        return StateSupervisionLevel.MEDIUM

    if supervision_level_value in maximum_levels:
        return StateSupervisionLevel.MAXIMUM

    if supervision_level_value in high_levels:
        return StateSupervisionLevel.HIGH

    if supervision_level_value in unsupervised_levels:
        return StateSupervisionLevel.UNSUPERVISED

    if supervision_level_value in warrant_levels:
        return StateSupervisionLevel.WARRANT

    if supervision_level_value in absconcion_levels:
        return StateSupervisionLevel.ABSCONSION

    if supervision_level_value in in_custody_levels:
        return StateSupervisionLevel.IN_CUSTODY

    # For COMS
    if "Minimum Administrative" in supervision_level_value:
        return StateSupervisionLevel.IN_CUSTODY

    if "Minimum" in supervision_level_value:
        return StateSupervisionLevel.MINIMUM

    if "Medium" in supervision_level_value:
        return StateSupervisionLevel.MEDIUM

    if "Maximum" in supervision_level_value:
        return StateSupervisionLevel.MAXIMUM

    if "Intensive" in supervision_level_value:
        return StateSupervisionLevel.HIGH

    if "Absconder" in supervision_level_value:
        return StateSupervisionLevel.ABSCONSION

    if "Warrant" in supervision_level_value:
        return StateSupervisionLevel.WARRANT

    # If values fall into none of these categories
    if supervision_level_value:
        return StateSupervisionLevel.INTERNAL_UNKNOWN

    return None


def map_supervision_type_based_on_coms_level(
    raw_text: str,
) -> Optional[StateSupervisionPeriodSupervisionType]:

    if "parole" in raw_text.lower() and "probation" in raw_text.lower():
        return StateSupervisionPeriodSupervisionType.DUAL

    if "parole" in raw_text.lower():
        return StateSupervisionPeriodSupervisionType.PAROLE

    if "probation" in raw_text.lower():
        return StateSupervisionPeriodSupervisionType.PROBATION

    if raw_text:
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN

    return None


def parse_staff_role_type(
    raw_text: str,
) -> Optional[StateStaffRoleType]:

    if "parole" in raw_text.lower() and (
        "probation" in raw_text.lower() or "prbtn" in raw_text.lower()
    ):
        return StateStaffRoleType.SUPERVISION_OFFICER

    if raw_text:
        # currently we only have role_type enums for supervision officer related roles
        return StateStaffRoleType.INTERNAL_UNKNOWN

    return None


def parse_staff_role_subtype(
    raw_text: str,
) -> Optional[StateStaffRoleSubtype]:

    if parse_staff_role_type(raw_text) == StateStaffRoleType.SUPERVISION_OFFICER:
        if "manager" in raw_text.lower():
            return StateStaffRoleSubtype.SUPERVISION_OFFICER_SUPERVISOR

        return StateStaffRoleSubtype.SUPERVISION_OFFICER

    if raw_text:
        # we'll ingest all other roles (like leadership roles) via a separate view/mapping using a roster
        return StateStaffRoleSubtype.INTERNAL_UNKNOWN

    return None
