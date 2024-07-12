#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Custom parser functions for US_TN. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_tn_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""
from typing import Optional

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodHousingUnitType,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.str_field_utils import parse_days_from_duration_pieces


def parse_supervision_type(raw_text: str) -> StateSupervisionSentenceSupervisionType:
    """
    Returns the supervision type of a supervision sentence.
    """
    # TODO(#10923): Remove custom parser once multiple columns can be used to determine enum value.
    sentence_status, suspended_to_probation, sentenced_to = raw_text.split("-")
    if suspended_to_probation == "S" or sentence_status == "PB":
        return StateSupervisionSentenceSupervisionType.PROBATION
    if sentence_status == "CC" or (sentence_status == "IN" and sentenced_to == "CC"):
        return StateSupervisionSentenceSupervisionType.COMMUNITY_CORRECTIONS

    return StateSupervisionSentenceSupervisionType.EXTERNAL_UNKNOWN


def parse_sentence_status(raw_text: str) -> StateSentenceStatus:
    """
    Returns the StateSentenceStatus associated with the sentence action and sentence status columns.
    """
    # TODO(#10923): Remove custom parser once multiple columns can be used to determine enum value.
    sentence_action, sentence_status = raw_text.split("-")
    if sentence_action == "CMTA":
        return StateSentenceStatus.COMMUTED
    if sentence_action == "PARA":
        return StateSentenceStatus.PARDONED
    if sentence_action == "RLSD":
        return StateSentenceStatus.SUSPENDED
    if sentence_action in ("VRVC", "VRVP", "JRPR", "JRCC"):
        return StateSentenceStatus.REVOKED
    if sentence_status in ("AC", "CC", "PB"):
        return StateSentenceStatus.SERVING
    if sentence_status == "IN":
        return StateSentenceStatus.COMPLETED

    return StateSentenceStatus.EXTERNAL_UNKNOWN


def parse_custodial_authority(raw_text: str) -> StateCustodialAuthority:
    """
    Returns the StateCustodialAuthority associated with an incarceration period using the site and site type columns.
    """
    start_movement_type, start_movement_reason, site_type, site = raw_text.split("-")

    if (
        start_movement_type == "FAOJ" and start_movement_reason == "OUTCT"
    ) or start_movement_type == "FACT":
        return StateCustodialAuthority.STATE_PRISON
    if (
        # The following sites are all courts.
        # TODO(#2912): We actually can't be sure what the custodial authority is for people in these courts -
        # they may be the responsibility of the state DOC (STATE_PRISON) or county based on where they've
        # been transferred from. We ideally would use this logic to hydrate a new `facility_type` or `location_type`
        # enum that allows us to differentiate between JAIL/PRISON/COUR and potentially more.
        site in ("019", "033", "046", "047", "054", "057", "075", "079", "082")
        or site_type == "JA"
    ):
        return StateCustodialAuthority.COUNTY
    if site_type == "IN":  # Institution
        return StateCustodialAuthority.STATE_PRISON
    if site_type in (
        "PA",  # Parole Office
        "PR",  # Probation Office
        "PX",
    ):  # Parole and Probation Office
        return StateCustodialAuthority.SUPERVISION_AUTHORITY
    if site_type is None or site_type in (
        "BC",  # Bootcamp (No longer in use)
        "CC",  # Usually, but not always, describes a community corrections facility.
        "CV",  # Conversion (Deprecated after 1992)
        "IJ",  # Institutional Juvenile
        "TC",  # TN DOC Central Office
    ):
        # TODO(#9421): Update mapping when we have support for community corrections
        return StateCustodialAuthority.INTERNAL_UNKNOWN

    return StateCustodialAuthority.INTERNAL_UNKNOWN


def parse_housing_unit_type(raw_text: str) -> StateIncarcerationPeriodHousingUnitType:
    """
    Returns the StateCustodialAuthority associated with an incarceration period using the site and site type columns.
    """
    (
        _,
        segregation_type,
        _,
    ) = raw_text.split("-")

    if (
        # The following codes are the only types of Segregation that are considered permanent stays:
        # -- ASE: ADMINISTRATIVE SEGREGATION
        # -- MSG: MANDATORY SEGREGATION
        # -- PCB: PROTECTIVE CUSTODY
        segregation_type
        in ("ASE", "MSG", "PCB")
    ):
        # TODO(#22252): Remap these values so we can deprecate PERMANENT_SOLITARY_CONFINEMENT
        return StateIncarcerationPeriodHousingUnitType.PERMANENT_SOLITARY_CONFINEMENT
    if (
        # The following codes are considered temporary stays:
        # -- HCE: HOLDING CELL
        # -- INV: SEGREGATION PEND INVESTIGATION
        # -- IPT: IMPOSED PROBATED TIME
        # -- MET: TEMP. MED. OR COURT TRANSIENT
        # -- PUN: PUNITIVE SEGREGATION
        # -- QUA: QUARANTINE
        # -- SIP: STAFF/INMATE PROVOCATION
        # -- SPD: SEGREGATION PEND DISC. HEARING
        # -- TSD: TAMPERING W/SECURITY EQUIP
        # -- TSE: THERAPEUTIC SEGREGATION
        segregation_type
        in ("HCE", "INV", "IPT", "MET", "PUN", "QUA", "SIP", "SPD", "TSD", "TSE")
    ):
        return StateIncarcerationPeriodHousingUnitType.TEMPORARY_SOLITARY_CONFINEMENT

    # We should not return any INTERNAL_UNKNOWN since we are assigning all segregation stays to either PERMANENT or TEMPORARY.
    # For incarceration periods with housing units that are not seg, we default them to GENERAL.
    return StateIncarcerationPeriodHousingUnitType.INTERNAL_UNKNOWN


def parse_segregation_admission_reason(
    raw_text: str,
) -> StateIncarcerationPeriodAdmissionReason:
    """
    Returns the StateIncarcerationPeriodAdmissionReason of TRANSFER for all Segregation periods
    """
    if raw_text:
        return StateIncarcerationPeriodAdmissionReason.TRANSFER

    # We should not return any INTERNAL_UNKNOWN since we are assigning all segregation stays to TRANSFER
    return StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN


def parse_segregation_release_reason(
    raw_text: str,
) -> StateIncarcerationPeriodReleaseReason:
    """
    Returns the StateIncarcerationPeriodReleaseReason of TRANSFER for all Segregation periods
    """
    if raw_text:
        return StateIncarcerationPeriodReleaseReason.TRANSFER

    # We should not return any INTERNAL_UNKNOWN since we are assigning all segregation stays to TRANSFER
    return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN


def get_punishment_days(
    months: str, weeks: str, days: str, hours: str, effective_date: str
) -> Optional[str]:
    if months or days or weeks or hours:
        # TN sometimes uses 9999 as the year for dates, we cannot use these because they will not parse
        start_dt_str = None if "9999" in effective_date else effective_date
        return str(
            parse_days_from_duration_pieces(
                years_str=None,
                months_str=months,
                weeks_str=weeks,
                days_str=days,
                hours_str=hours,
                start_dt_str=start_dt_str,
            )
        )
    return None
