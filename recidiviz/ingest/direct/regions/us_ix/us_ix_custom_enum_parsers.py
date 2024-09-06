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
"""Custom enum parsers functions for US_IX. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_ix_custom_enum_parsers.<function name>
"""

from datetime import date

from recidiviz.common.constants.state.state_sentence import StateSentencingAuthority
from recidiviz.common.constants.state.state_shared_enums import StateActingBodyType
from recidiviz.common.constants.state.state_staff_caseload_type import (
    StateStaffCaseloadType,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.str_field_utils import parse_date

# Mappings list for contact location

ALTERNATIVE_EMPLOYMENT_LOCATION = ["ALTERNATE WORK SITE"]

JAIL_LOCATION = [
    "JAIL",
]

COURT_LOCATION = [
    "COURT",
    "DRUG COURT",
]

FIELD_LOCATION = [
    "FIELD",
    "COMMUNITY SERVICE SITE",
]

LAW_ENFORCEMENT_LOCATION = [
    "LAW ENFORCEMENT AGENCY",
]

OFFICE_LOCATION = [
    "OFFICE",
    "INTERSTATE OFFICE",
]

PAROLE_COMMISSION_LOCATION = [
    "PAROLE COMMISSION",
    "IDAHO COMMISSION OF PARDONS AND PAROLE",
]

RESIDENCE_LOCATION = [
    "RESIDENCE",
    "OTHER RESIDENCE",
]

EMPLOYMENT_LOCATION = [
    "EMPLOYER",
]

TREATMENT_PROVIDER_LOCATION = [
    "TREATMENT PROVIDER",
]


def contact_location_from_contact_locations_list(
    raw_text: str,
) -> StateSupervisionContactLocation:
    """Determines which supervision contact location to map to based on a list of concatenated supervision contact locations"""

    locations = raw_text.split(",")

    if any(location in RESIDENCE_LOCATION for location in locations):
        return StateSupervisionContactLocation.RESIDENCE

    if any(location in TREATMENT_PROVIDER_LOCATION for location in locations):
        return StateSupervisionContactLocation.TREATMENT_PROVIDER

    if any(location in OFFICE_LOCATION for location in locations):
        return StateSupervisionContactLocation.SUPERVISION_OFFICE

    if any(location in FIELD_LOCATION for location in locations):
        return StateSupervisionContactLocation.FIELD

    if any(location in PAROLE_COMMISSION_LOCATION for location in locations):
        return StateSupervisionContactLocation.PAROLE_COMMISSION

    if any(location in EMPLOYMENT_LOCATION for location in locations):
        return StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT

    if any(location in COURT_LOCATION for location in locations):
        return StateSupervisionContactLocation.COURT

    if any(location in LAW_ENFORCEMENT_LOCATION for location in locations):
        return StateSupervisionContactLocation.LAW_ENFORCEMENT_AGENCY

    if any(location in ALTERNATIVE_EMPLOYMENT_LOCATION for location in locations):
        return StateSupervisionContactLocation.ALTERNATIVE_PLACE_OF_EMPLOYMENT

    if any(location in JAIL_LOCATION for location in locations):
        return StateSupervisionContactLocation.JAIL

    return StateSupervisionContactLocation.INTERNAL_UNKNOWN


def contact_method_from_contact_methods_list(
    raw_text: str,
) -> StateSupervisionContactMethod:
    """Determines which supervision contact method to map to based on a list of concatenated supervision contact methods"""

    methods = raw_text.split(",")

    if "TELEPHONE" in methods:
        return StateSupervisionContactMethod.TELEPHONE

    if "WRITTEN_MESSAGE" in methods:
        return StateSupervisionContactMethod.WRITTEN_MESSAGE

    if "VIRTUAL" in methods:
        return StateSupervisionContactMethod.VIRTUAL

    if "IN_PERSON" in methods:
        return StateSupervisionContactMethod.IN_PERSON

    return StateSupervisionContactMethod.INTERNAL_UNKNOWN


def ws_supervision_type(raw_text: str) -> StateSupervisionPeriodSupervisionType:
    """Maps supervision type to WARRANT_STATUS for cases where we've already identified via the supervising officer as being bench warrant;
    used instead of a literal enum so that raw text can be preserved."""
    if raw_text:
        return StateSupervisionPeriodSupervisionType.WARRANT_STATUS
    raise ValueError("This parser should never be called on missing raw text.")


def lsu_supervision_level(raw_text: str) -> StateSupervisionLevel:
    """Maps supervision level to LIMITED for cases where we've already determined supervision site to be district 0 or LSU;
    used instead of a literal enum so that raw text can be preserved."""
    if raw_text:
        return StateSupervisionLevel.LIMITED
    raise ValueError("This parser should never be called on missing raw text.")


def determine_ed_requesting_body(raw_text: str) -> StateActingBodyType:
    """Maps parole early discharge requesting body based on when the request date was since there was a policy change Oct 2019 that
    changed it such that all parole early discharge requests are now made by the client instead of by the supervision officer
    """
    request_date = parse_date(raw_text)
    if request_date:
        if request_date < date(2019, 10, 1):
            return StateActingBodyType.SUPERVISION_OFFICER
        return StateActingBodyType.SENTENCED_PERSON

    return StateActingBodyType.INTERNAL_UNKNOWN


def parse_caseload_type(raw_text: str) -> StateStaffCaseloadType:
    """Maps an officer's caseload type to the appropriate enum based on raw text in the supervisor roster."""
    caseload_type = raw_text.split("-")[0]

    if caseload_type:
        if caseload_type == "ADMINSTRATIVE_SUPERVISION":
            return StateStaffCaseloadType.ADMINISTRATIVE_SUPERVISION
        if caseload_type == "COMMUNITY_FACILITY":
            return StateStaffCaseloadType.COMMUNITY_FACILITY
        if caseload_type == "DRUG_COURT":
            return StateStaffCaseloadType.DRUG_COURT
        if caseload_type == "ELECTRONIC_MONITORING":
            return StateStaffCaseloadType.ELECTRONIC_MONITORING
        if caseload_type == "INTENSIVE":
            return StateStaffCaseloadType.INTENSIVE
        if caseload_type == "MENTAL_HEALTH":
            return StateStaffCaseloadType.MENTAL_HEALTH
        if caseload_type == "OTHER_COURT":
            return StateStaffCaseloadType.OTHER_COURT
        if caseload_type == "SEX_OFFENSE":
            return StateStaffCaseloadType.SEX_OFFENSE
        if caseload_type == "VETERANS_COURT":
            return StateStaffCaseloadType.VETERANS_COURT
        if caseload_type == "DOMESTIC_VIOLENCE":
            return StateStaffCaseloadType.DOMESTIC_VIOLENCE
        if caseload_type == "TRANSITIONAL":
            return StateStaffCaseloadType.TRANSITIONAL
        if caseload_type == "OTHER":
            return StateStaffCaseloadType.OTHER
        if caseload_type == "GENERAL":
            return StateStaffCaseloadType.GENERAL
        return StateStaffCaseloadType.INTERNAL_UNKNOWN
    return StateStaffCaseloadType.INTERNAL_UNKNOWN


def county_jail_supervision_level(raw_text: str) -> StateSupervisionLevel:
    """Maps supervision level to IN_CUSTODY for cases where we've already identified supervision site as county jail;
    used instead of a literal enum so that raw text of the county jail name can be preserved."""
    if raw_text:
        return StateSupervisionLevel.IN_CUSTODY
    raise ValueError("This parser should never be called on missing raw text.")


def parse_sentencing_authority(raw_text: str) -> StateSentencingAuthority:
    """Maps sentencing authroity"""
    if raw_text == "Idaho":
        return StateSentencingAuthority.STATE
    return StateSentencingAuthority.OTHER_STATE
