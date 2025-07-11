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
"""Custom enum parsers functions for US_ND. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_nd_custom_enum_parsers.<function name>
"""
import datetime
from typing import Dict, List, Type

from recidiviz.common.constants.enum_parser import EnumParsingError
from recidiviz.common.constants.state.state_charge import (
    StateChargeClassificationType,
    StateChargeV2ClassificationType,
)
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodCustodyLevel,
    StateIncarcerationPeriodHousingUnitCategory,
    StateIncarcerationPeriodHousingUnitType,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person import StateResidencyStatus
from recidiviz.common.constants.state.state_person_address_period import (
    StatePersonAddressType,
)
from recidiviz.common.constants.state.state_sentence import StateSentencingAuthority
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_staff_caseload_type import (
    StateStaffCaseloadType,
)
from recidiviz.common.constants.state.state_staff_role_period import (
    StateStaffRoleSubtype,
    StateStaffRoleType,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.str_field_utils import parse_datetime

SOLITARY_UNIT_CODES = list(
    [
        "NDSP-BIU-A",
        "NDSP-BIU-ATU",
        "NDSP-BIU-B",
        "NDSP-BIU-C",
        "NDSP-BIU-D",
    ]
)
HOSPITAL_UNIT_CODES = list(["HOSP", "TU", "MTU", "HSU", "INF"])
STATE_CODES = list(
    [
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "DC",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "PR",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "VI",
        "WA",
        "WV",
        "WI",
        "WY",
    ]
)
OTHER_STATE_FACILITY = ("OOS", "OS", "MINN", "SD")

POST_JULY_2017_CUSTODIAL_AUTHORITY_ENUM_MAP: Dict[
    StateCustodialAuthority, List[str]
] = {
    StateCustodialAuthority.COUNTY: [
        "CJ",
        "DEFP",
        # There are only a few of these, and they seem to represent judicial
        # districts in ND
        "NW",
        "SC",
        "SW",
        "SE",
        "EAST",
        "NE",
        "NEC",
        "NC",
        "FD",  # Federal court
    ],
    StateCustodialAuthority.EXTERNAL_UNKNOWN: [
        # TODO(#32122): Move this to STATE_PRISON once #32123 is complete.
        "NTAD",  # Resident Not Admitted
    ],
    StateCustodialAuthority.STATE_PRISON: [
        "BTC",
        "BTCWTR",
        "CONT",
        "CPP",
        "DWCRC",
        "FTPFAR",
        "FTPMND",
        "GFC",
        "HACTC",
        "HRCC",
        "INACT",
        "JRCC",
        "LRRP",
        "MRCC",
        "MTPFAR",
        "MTPMDN",
        "MTPMND",
        "NCCRC",
        "NDSP",
        "OUT",
        "PREA",
        "PROB",
        # TODO(#10432): We don't know what facility "TABLET" is - ask ND how to
        #   properly map this.
        "TABLET",
        "TRC",
        "TRCC",
        "TRN",
        "YCC",
        "JRMU",
        "DWCRC1",
        "WCJWRP",
        "SWMCCC",
    ],
}

POST_JULY_2017_PFI_ENUM_MAP: Dict[
    StateSpecializedPurposeForIncarceration, List[str]
] = {
    StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY: [
        "DEFP",
        "NTAD",
        # There are only a few of these, and they seem to represent judicial
        # districts in ND
        "NW",
        "SC",
        "SW",
        "SE",
        "EAST",
        "NE",
        "NEC",
        "NC",
        "FD",  # Federal court
    ],
    StateSpecializedPurposeForIncarceration.GENERAL: [
        "CJ",  # All temporary CJ stays are caught before this. These are all DOCR sentences being served physically in a County Jail to avoid overcrowding.
        "BTC",
        "BTCWTR",
        "CONT",
        "CPP",
        "DWCRC",
        "FTPFAR",
        "FTPMND",
        "GFC",
        "HACTC",
        "HRCC",
        "INACT",
        "JRCC",
        "LRRP",
        "MRCC",
        "MTPFAR",
        "MTPMDN",
        "MTPMND",
        "NCCRC",
        "NDSP",
        "OUT",
        "PREA",
        "PROB",
        # TODO(#10432): We don't know what facility "TABLET" is - ask ND how to
        #   properly map this.
        "TABLET",
        "TRC",
        "TRCC",
        "TRN",
        "YCC",
        "JRMU",
        "DWCRC1",
        "WCJWRP",
        "SWMCCC",
    ],
}

POST_JULY_2017_CUSTODIAL_AUTHORITY_RAW_TEXT_TO_ENUM_MAP: Dict[
    str, StateCustodialAuthority
] = {
    raw_text_value: custodial_authority
    for custodial_authority, raw_text_values in POST_JULY_2017_CUSTODIAL_AUTHORITY_ENUM_MAP.items()
    for raw_text_value in raw_text_values
}

POST_JULY_2017_PFI_RAW_TEXT_TO_ENUM_MAP: Dict[
    str, StateSpecializedPurposeForIncarceration
] = {
    raw_text_value: custodial_authority
    for custodial_authority, raw_text_values in POST_JULY_2017_PFI_ENUM_MAP.items()
    for raw_text_value in raw_text_values
}


def _datetime_str_is_before_2017_custodial_authority_cutoff(
    datetime_str: str, enum_type_being_parsed: Type[StateEntityEnum]
) -> bool:
    """A helper function to determine if a datetime is prior to or after 7/1/2017, when
    options for custodial authority mappings changed."""
    comparison_date = parse_datetime(datetime_str)

    if not comparison_date:
        raise EnumParsingError(
            enum_type_being_parsed,
            "Unable to parse custodial authority without a valid date on the IP. "
            f"Found: {datetime_str}.",
        )

    return comparison_date < datetime.datetime(year=2017, month=7, day=1)


def custodial_authority_from_facility_and_dates(
    raw_text: str,
) -> StateCustodialAuthority:
    """Parse the StateCustodialAuthority for a given stay using a combination of
    bed assignment, facility, and date of stay."""
    bed_assignment, facility, datetime_str_for_comparison = raw_text.split("|")
    # Ensure that work release programs have StateCustodialAuthority.STATE_PRISON
    if bed_assignment in ("CJ-WRK-WAR", "CJ-WRK-STA"):
        return StateCustodialAuthority.STATE_PRISON

    if facility in OTHER_STATE_FACILITY:
        return StateCustodialAuthority.OTHER_STATE

    # Everything except OOS and OS (checked above) was overseen by DOCR before July 1, 2017.
    if _datetime_str_is_before_2017_custodial_authority_cutoff(
        datetime_str_for_comparison, StateCustodialAuthority
    ):
        return StateCustodialAuthority.STATE_PRISON

    if facility not in POST_JULY_2017_CUSTODIAL_AUTHORITY_RAW_TEXT_TO_ENUM_MAP:
        raise EnumParsingError(
            StateCustodialAuthority,
            "Found facility without a mapping to a custodial authority: "
            f"{facility}.",
        )

    if (
        bed_assignment.count("CJ-SS-") == 0
        and bed_assignment.count("CJ-PV-") == 0
        and bed_assignment.count("CJ-ESCAPE-") == 0
    ) and facility == "CJ":
        # This is not a period of temporary custody, but it is being served in a county jail.
        return StateCustodialAuthority.STATE_PRISON

    return POST_JULY_2017_CUSTODIAL_AUTHORITY_RAW_TEXT_TO_ENUM_MAP[facility]


def pfi_from_facility_and_dates(
    raw_text: str,
) -> StateSpecializedPurposeForIncarceration:
    """
    Parses the specialized purpose for incarceration from a combination of a person's
    bed assignment, facility of residence, and date of incarceration.

    The bed assignment is a necessary component for this
    classification because some people are physically housed at a County Jail while serving
    standard DOCR sentences in order to avoid overcrowding. People who are in a county jail
    in those circumstances can be identified specifically by their bed assignments.

    Otherwise, a combination of facility and date of stay is used to determine the PFI.
    """
    bed_assignment, facility, datetime_str_for_comparison = raw_text.split("|")
    if (
        bed_assignment.count("CJ-SS-") > 0
        or bed_assignment.count("CJ-PV-") > 0
        or bed_assignment.count("CJ-ESCAPE-") > 0
    ):
        # These are the only instances of a person being in county jail who has not
        # been taken into DOCR custody. These are short sentences, parole violators, or returns
        # from escape. These folks are typically transferred to a DOCR orientation unit eventually,
        # but sometimes serve the entirety of their time in county jail.
        return StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY

    if facility in OTHER_STATE_FACILITY:
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN

    # There were no periods of temporary custody before July 1, 2017.
    if _datetime_str_is_before_2017_custodial_authority_cutoff(
        datetime_str_for_comparison, StateSpecializedPurposeForIncarceration
    ):
        return StateSpecializedPurposeForIncarceration.GENERAL

    if facility not in POST_JULY_2017_PFI_RAW_TEXT_TO_ENUM_MAP:
        raise EnumParsingError(
            StateSpecializedPurposeForIncarceration,
            "Found facility without a mapping to a pfi: " f"{facility}.",
        )

    return POST_JULY_2017_PFI_RAW_TEXT_TO_ENUM_MAP[facility]


def parse_residency_status_from_address(
    raw_text: str,
) -> StateResidencyStatus:
    """Parses a person's address to determine their StateResidencyStatus."""
    if "HOMELESS" in raw_text.upper():
        return StateResidencyStatus.HOMELESS
    return StateResidencyStatus.PERMANENT


def parse_classification_type_from_raw_text(
    raw_text: str,
) -> StateChargeClassificationType:
    """Parses a raw text field with the level of an offense to determine classification type."""
    if raw_text.startswith("F"):
        return StateChargeClassificationType.FELONY
    if raw_text.startswith("M"):
        return StateChargeClassificationType.MISDEMEANOR
    return StateChargeClassificationType.INTERNAL_UNKNOWN


def parse_classification_type_from_raw_text_v2(
    raw_text: str,
) -> StateChargeV2ClassificationType:
    """Parses a raw text field with the level of an offense to determine classification type."""
    if raw_text.startswith("F"):
        return StateChargeV2ClassificationType.FELONY
    if raw_text.startswith("M"):
        return StateChargeV2ClassificationType.MISDEMEANOR
    return StateChargeV2ClassificationType.INTERNAL_UNKNOWN


def parse_sentencing_authority(raw_text: str) -> StateSentencingAuthority:
    """Parses sentencing authority from the county code of the judicial district where
    sentencing occurred."""
    if raw_text.startswith("US_ND") or raw_text == "PAROLE":
        return StateSentencingAuthority.STATE
    if raw_text == "FEDERAL":
        return StateSentencingAuthority.FEDERAL
    if raw_text in ("INTERNATIONAL", "ERROR"):
        return StateSentencingAuthority.INTERNAL_UNKNOWN
    return StateSentencingAuthority.OTHER_STATE


def supervision_contact_type_mapper(raw_text: str) -> StateSupervisionContactType:
    """Parses the contact type from a string containing the contact codes."""
    codes = raw_text.split("-")
    # ND confirmed that "HV", "OV", and "OO" are placeholders for the “face to face” code,
    # and that we should not prioritize the collateral contact code over others.

    if any(code in ["FF", "HV", "OO", "OV"] for code in codes):
        if "CC" in codes:
            return StateSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT
        return StateSupervisionContactType.DIRECT

    if "CC" in codes:
        return StateSupervisionContactType.COLLATERAL

    return StateSupervisionContactType.INTERNAL_UNKNOWN


def supervision_contact_location_mapper(
    raw_text: str,
) -> StateSupervisionContactLocation:
    """Parses the contact location from a string containing the contact codes."""
    codes = raw_text.split("-")

    # There may multiple codes that indicate multiple locations.
    # This prioritizes home visits, then employment visits and then supervising office visits.
    if "HV" in codes:
        return StateSupervisionContactLocation.RESIDENCE
    if "OO" in codes:
        return StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT
    if "OV" in codes:
        return StateSupervisionContactLocation.SUPERVISION_OFFICE
    return StateSupervisionContactLocation.INTERNAL_UNKNOWN


def supervision_contact_status_mapper(raw_text: str) -> StateSupervisionContactStatus:
    """Parses the contact status from a string containing the contact codes."""
    codes = raw_text.split("-")

    # If explicitly set as attempted, we'll use the direct mapping.
    # Otherwise, we assume the contact was completed.
    if any(code in ["AC", "NS"] for code in codes):
        return StateSupervisionContactStatus.ATTEMPTED

    return StateSupervisionContactStatus.COMPLETED


def supervision_contact_method_mapper(raw_text: str) -> StateSupervisionContactMethod:
    """Parses the contact method from a string containing the contact codes."""
    codes = raw_text.split("-")

    # We assume that a visit is done in person. Otherwise, if we find a notion of communication, then
    # we assume virtual.
    if any(code in ["FF", "HV", "OO", "OV"] for code in codes):
        return StateSupervisionContactMethod.IN_PERSON
    if "OC" in codes:  # Offender Communication
        return StateSupervisionContactMethod.VIRTUAL
    return StateSupervisionContactMethod.INTERNAL_UNKNOWN


def parse_housing_unit_category(
    raw_text: str,
) -> StateIncarcerationPeriodHousingUnitCategory:
    """Parses the category of a housing unit given a bed assignment."""
    if raw_text:
        unit = raw_text.split("-")
        if any(code in raw_text for code in SOLITARY_UNIT_CODES):
            return StateIncarcerationPeriodHousingUnitCategory.SOLITARY_CONFINEMENT
        if unit[1] in STATE_CODES or unit[1] in OTHER_STATE_FACILITY:
            # We do not have specific information about housing units for people
            # who are incarcerated outside of North Dakota.
            return StateIncarcerationPeriodHousingUnitCategory.INTERNAL_UNKNOWN
        return StateIncarcerationPeriodHousingUnitCategory.GENERAL
    return StateIncarcerationPeriodHousingUnitCategory.EXTERNAL_UNKNOWN


def parse_housing_unit_type(
    raw_text: str,
) -> StateIncarcerationPeriodHousingUnitType:
    """Parses the type of a housing unit given a bed assignment.
    TODO(#26928): Add granularity to these housing types using assessments data
    if and when it becomes necessary."""
    if raw_text:
        unit = raw_text.split("-")
        if any((code in raw_text) for code in HOSPITAL_UNIT_CODES):
            return StateIncarcerationPeriodHousingUnitType.HOSPITAL
        if any((code in raw_text) for code in SOLITARY_UNIT_CODES):
            # Temporarily map all remaining bed assignments that indicate a person is in solitary
            # to OTHER_SOLITARY_CONFINEMENT until we can clarify with ND how to join the
            # elite_offenderprogramprofiles table to this view to get more information
            # about the nature of the assignment.
            return (
                StateIncarcerationPeriodHousingUnitType.PERMANENT_SOLITARY_CONFINEMENT
            )
        if unit[1] in STATE_CODES or unit[1] in OTHER_STATE_FACILITY:
            # We do not have specific information about housing units for people
            # who are incarcerated outside of North Dakota.
            return StateIncarcerationPeriodHousingUnitType.INTERNAL_UNKNOWN
        if len(unit) > 2:
            if unit[2] in STATE_CODES or unit[2] in OTHER_STATE_FACILITY:
                # We do not have specific information about housing units for people
                # who are incarcerated outside of North Dakota.
                return StateIncarcerationPeriodHousingUnitType.INTERNAL_UNKNOWN
        return StateIncarcerationPeriodHousingUnitType.GENERAL
    return StateIncarcerationPeriodHousingUnitType.EXTERNAL_UNKNOWN


def parse_caseload_type(raw_text: str) -> StateStaffCaseloadType:
    """Parse an officer's caseload type using a raw text field with their job title."""
    if raw_text:
        if "DRUG COURT" in raw_text:
            return StateStaffCaseloadType.DRUG_COURT
        if "MENTAL HEALTH" in raw_text:
            return StateStaffCaseloadType.MENTAL_HEALTH
        if "SEX OFFENDER" in raw_text:
            return StateStaffCaseloadType.SEX_OFFENSE
        if "DOMESTIC VIOLENCE" in raw_text:
            return StateStaffCaseloadType.DOMESTIC_VIOLENCE
        if "COMMUNITY CORRECTIONS AGENT" in raw_text:
            return StateStaffCaseloadType.ADMINISTRATIVE_SUPERVISION
        if "PRETRIAL" in raw_text:
            return StateStaffCaseloadType.OTHER_COURT
        return StateStaffCaseloadType.GENERAL
    return StateStaffCaseloadType.INTERNAL_UNKNOWN


def parse_role_subtype(raw_text: str) -> StateStaffRoleSubtype:
    """
    "Lead Officer" is a supervisor of other officers.
    It is common for these staff members to also supervise clients directly.
    "Case Manager" and "Community Corrections Agent" are designations for staff members
    who only supervise clients, not other officers.
    "Region X Program Manager" is a district manager.
    """
    if raw_text:
        if "LEAD OFFICER" in raw_text:
            return StateStaffRoleSubtype.SUPERVISION_OFFICER_SUPERVISOR
        if (
            "PO" in raw_text
            or "CASE MANAGER" in raw_text
            or "CORRECTIONS AGENT" in raw_text
            or "GENERAL" in raw_text
        ):
            return StateStaffRoleSubtype.SUPERVISION_OFFICER
        if "REGION" in raw_text and "PROGRAM MANAGER" in raw_text:
            return StateStaffRoleSubtype.SUPERVISION_DISTRICT_MANAGER
        if "DIRECTOR" in raw_text:
            return StateStaffRoleSubtype.SUPERVISION_STATE_LEADERSHIP
    return StateStaffRoleSubtype.INTERNAL_UNKNOWN


def parse_custody_level(raw_text: str) -> StateIncarcerationPeriodCustodyLevel:
    """
    Comments on the supervision level are included in the raw text for this field
    so that we can determine if a person has a warrant or detainer downstream.

    This parser pulls only the supervision level from the raw text and assigns a custody
    level accordingly.
    """
    level = raw_text.split("|")[0]
    if level in ("MIN", "MHI", "MH", "MLOW"):
        return StateIncarcerationPeriodCustodyLevel.MINIMUM
    if level in ("X", "ESCAPE", "COM"):
        return StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN
    if level in ("N/A", "UNCLASS", "INFERRED-UNCLASS"):
        return StateIncarcerationPeriodCustodyLevel.EXTERNAL_UNKNOWN
    if level == "INFERRED-INTAKE":
        return StateIncarcerationPeriodCustodyLevel.INTAKE
    if level in ("MEDR", "MED"):
        return StateIncarcerationPeriodCustodyLevel.MEDIUM
    if level in ("MAX", "MAXF"):
        return StateIncarcerationPeriodCustodyLevel.MAXIMUM
    if level == "CLO":
        return StateIncarcerationPeriodCustodyLevel.CLOSE
    return StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN


def incarceration_type_from_unit_or_facility(raw_text: str) -> StateIncarcerationType:
    """
    A function to parse the StateIncarcerationType of a stay based on a person's bed assignment
    and facility of residence. The bed assignment is a necessary component for this
    classification because some people are physically housed at a County Jail while serving
    standard DOCR sentences in order to avoid overcrowding. People who are in a county jail
    in those circumstances can be identified specifically by their bed assignments.
    """
    bed_assignment, facility = raw_text.split("|")
    if bed_assignment in ("CJ-WRK-WAR", "CJ-WRK-STA"):
        # Work Release
        return StateIncarcerationType.COUNTY_JAIL
    if (
        bed_assignment.count("CJ-SS-") > 0
        or bed_assignment.count("CJ-PV-") > 0
        or bed_assignment.count("CJ-ESCAPE-") > 0
    ):
        # These are the only instances of a person being in county jail who has not
        # been taken into DOCR custody. These are short sentences, parole violators, or returns
        # from escape. These folks are typically transferred to a DOCR orientation unit eventually,
        # but sometimes serve the entirety of their time in county jail.
        return StateIncarcerationType.COUNTY_JAIL
    if facility in (
        "CJ",
        "DEFP",
        "NW",
        "SC",
        "SW",
        "SE",
        "EAST",
        "NE",
        "NEC",
        "NC",
        "FD",
    ):
        # This will include all bed assignments that start with "CJ" and are folks who have
        # gone through orientation and have been selected to serve their DOCR time in a county jail.
        return StateIncarcerationType.COUNTY_JAIL
    if facility in ("OS", "OOS", "OUT", "NTAD"):
        # Other state facilities can be county jails or prisons; only the state is documented
        # in these cases, not the facility type.
        return StateIncarcerationType.OUT_OF_STATE
    return StateIncarcerationType.STATE_PRISON


def parse_address_type(raw_text: str) -> StatePersonAddressType:
    if raw_text:
        if "PO BOX" in raw_text or "P O " in raw_text or "GENERAL DELIVERY" in raw_text:
            return StatePersonAddressType.MAILING_ONLY
        return StatePersonAddressType.PHYSICAL_RESIDENCE
    return StatePersonAddressType.INTERNAL_UNKNOWN


def parse_role_type_facility_staff(raw_text: str) -> StateStaffRoleType:
    """A parser that only returns INTERNAL_UNKNOWN because it only parses role types
    for facilities staff, for which there is no designated role type currently."""
    if raw_text:
        return StateStaffRoleType.INTERNAL_UNKNOWN
    return StateStaffRoleType.INTERNAL_UNKNOWN
