# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""US_ND specific enum helper methods.

TODO(#8901): This file should become empty and be deleted when we have fully migrated
 this state to new ingest mappings version.
"""
from enum import Enum
from typing import Dict, List, Type

from recidiviz.common.constants.enum_overrides import (
    EnumIgnorePredicate,
    EnumMapperFn,
    EnumOverrides,
)
from recidiviz.common.constants.state.standard_enum_overrides import (
    legacy_mappings_standard_enum_overrides,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentLevel
from recidiviz.common.constants.state.state_court_case import StateCourtCaseStatus
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_person import StateRace
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.direct_ingest_controller_utils import (
    update_overrides_from_maps,
)


def supervision_contact_type_mapper(raw_text: str) -> StateSupervisionContactType:
    """Parses the contact type from a string containing the contact codes."""

    # TODO(#9365): Update to split on a dash instead of spaces when we migrate the view
    #  that uses this function to ingest mappings v2.
    codes = raw_text.split(" ")

    # If collateral or face-to-face is explicitly set in the contact codes, we will use the
    # direct mapping.
    if "CC" in codes:
        return StateSupervisionContactType.COLLATERAL

    if "FF" in codes:
        return StateSupervisionContactType.DIRECT

    # Otherwise, we assume that any home visit, office visit, or employment visit counts as a direct contact.
    if any(code in ["HV", "OO", "OV"] for code in codes):
        return StateSupervisionContactType.DIRECT

    return StateSupervisionContactType.INTERNAL_UNKNOWN


def supervision_contact_method_mapper(raw_text: str) -> StateSupervisionContactMethod:
    """Parses the contact method from a string containing the contact codes."""

    # TODO(#9365): Update to split on a dash instead of spaces when we migrate the view
    #  that uses this function to ingest mappings v2.
    codes = raw_text.split(" ")

    # We assume that a visit is done in person. Otherwise, if we find a notion of communication, then
    # we assume virtual.
    if any(code in ["HV", "OO", "OV"] for code in codes):
        return StateSupervisionContactMethod.IN_PERSON
    if "OC" in codes:  # Offender Communication
        return StateSupervisionContactMethod.VIRTUAL
    return StateSupervisionContactMethod.INTERNAL_UNKNOWN


def supervision_contact_status_mapper(raw_text: str) -> StateSupervisionContactStatus:
    """Parses the contact status from a string containing the contact codes."""

    # TODO(#9365): Update to split on a dash instead of spaces when we migrate the view
    #  that uses this function to ingest mappings v2.
    codes = raw_text.split(" ")

    # If explicitly set as attempted, we'll use the direct mapping.
    # Otherwise, we assume the contact was completed.
    if any(code in ["AC", "NS"] for code in codes):
        return StateSupervisionContactStatus.ATTEMPTED

    return StateSupervisionContactStatus.COMPLETED


def supervision_contact_location_mapper(
    raw_text: str,
) -> StateSupervisionContactLocation:
    """Parses the contact location from a string containing the contact codes."""

    # TODO(#9365): Update to split on a dash instead of spaces when we migrate the view
    #  that uses this function to ingest mappings v2.
    codes = raw_text.split(" ")

    # There may multiple codes that indicate multiple locations.
    # This prioritizes home visits, then employment visits and then supervising office visits.
    if "HV" in codes:
        return StateSupervisionContactLocation.RESIDENCE
    if "OO" in codes:
        return StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT
    if "OV" in codes:
        return StateSupervisionContactLocation.SUPERVISION_OFFICE
    return StateSupervisionContactLocation.INTERNAL_UNKNOWN


IGNORES: Dict[Type[Enum], List[str]] = {
    StateCourtCaseStatus: ["A", "ACC", "STEP"],
}


def generate_enum_overrides() -> EnumOverrides:
    """Provides North Dakota-specific overrides for enum mappings.

    The keys herein are raw strings directly from the source data, and the values are the enums that they are
    mapped to within our schema. The values are a list because a particular string may be used in multiple
    distinct columns in the source data.
    """
    overrides: Dict[Enum, List[str]] = {
        StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE: ["NAT"],
        StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: ["HAW"],
        StateRace.OTHER: ["MUL"],
        StateSentenceStatus.SERVING: ["O"],
        StateIncarcerationIncidentType.DISORDERLY_CONDUCT: [
            "DAMAGE",
            "DISCON",
            "ESCAPE_ATT",
            "INS",
            "SEXCONTACT",
            "UNAUTH",
            "NON",
        ],
        StateIncarcerationIncidentType.CONTRABAND: [
            "CONT",
            "GANG",
            "GANGREL",
            "PROP",
            "TOB",
        ],
        StateIncarcerationIncidentType.MINOR_OFFENSE: ["SWIFT"],
        StateIncarcerationIncidentType.POSITIVE: ["POSREPORT"],
        StateIncarcerationIncidentType.REPORT: ["STAFFREP"],
        StateIncarcerationIncidentType.PRESENT_WITHOUT_INFO: ["CONV"],
        StateIncarcerationIncidentType.VIOLENCE: [
            "IIASSAULT",
            "IIASSAULTINJ",
            "IIFIGHT",
            "FGHT",
            "IISUBNOINJ",
            "ISASSAULT",
            "ISASSAULTINJ",
            "ISSUBNOINJ",
            "SEXUAL",
            "THREAT",
        ],
        StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS: [
            "LCP",
            "LOR",
            "LCO",
            "LVPVV",
            "LOP",
            "LVP",
            "LPJES",
            "FREM",
            "RTQ",
            "UREST",
            "LPH",
            "LSE",
            "CCF",
            "SREM",
        ],
        StateIncarcerationIncidentOutcomeType.FINANCIAL_PENALTY: [
            "RES",
            "PAY",
            "FIN",
            "PRO",
            "LJB",
        ],
        StateIncarcerationIncidentOutcomeType.SOLITARY: ["SEG", "DD", "RAS"],
        StateIncarcerationIncidentOutcomeType.TREATMENT: ["RTX"],
        StateIncarcerationIncidentOutcomeType.DISMISSED: ["DSM"],
        StateIncarcerationIncidentOutcomeType.EXTERNAL_PROSECUTION: ["RSA"],
        StateIncarcerationIncidentOutcomeType.INTERNAL_UNKNOWN: [
            "COMB",
            "DELETED",
            "RED",
            "TRA",
        ],
        StateIncarcerationIncidentOutcomeType.DISCIPLINARY_LABOR: ["EXD"],
        StateIncarcerationIncidentOutcomeType.GOOD_TIME_LOSS: ["LG", "STP"],
        StateIncarcerationIncidentOutcomeType.WARNING: ["WAR", "NS"],
        StateProgramAssignmentParticipationStatus.PENDING: [
            "Submitted",
            "Pending Coordinator",
        ],
        StateProgramAssignmentParticipationStatus.REFUSED: ["Refused"],
        StateAssessmentLevel.EXTERNAL_UNKNOWN: ["NOT APPLICABLE", "UNDETERMINED"],
        StateSupervisionContactReason.GENERAL_CONTACT: ["SUPERVISION"],
    }

    override_mappers: Dict[Type[Enum], EnumMapperFn] = {
        StateSupervisionContactStatus: supervision_contact_status_mapper,
        StateSupervisionContactLocation: supervision_contact_location_mapper,
        StateSupervisionContactType: supervision_contact_type_mapper,
        StateSupervisionContactMethod: supervision_contact_method_mapper,
    }

    ignore_predicates: Dict[Type[Enum], EnumIgnorePredicate] = {}

    base_overrides = legacy_mappings_standard_enum_overrides()
    return update_overrides_from_maps(
        base_overrides, overrides, IGNORES, override_mappers, ignore_predicates
    )
