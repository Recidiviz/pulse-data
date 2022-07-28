# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Constants related to a StateCharge entity in the state schema."""
from enum import unique
from typing import Dict, Optional

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateChargeClassificationType(StateEntityEnum):
    CIVIL = state_enum_strings.state_charge_classification_type_civil
    FELONY = state_enum_strings.state_charge_classification_type_felony
    # TODO(#12648): Delete this in favor of CIVIL
    INFRACTION = state_enum_strings.state_charge_classification_type_infraction
    MISDEMEANOR = state_enum_strings.state_charge_classification_type_misdemeanor
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, Optional["StateChargeClassificationType"]]:
        return _STATE_CHARGE_CLASSIFICATION_TYPE_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "The classification of the offense a person has been charged with."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_CHARGE_CLASSIFICATION_TYPE_VALUE_DESCRIPTIONS


_STATE_CHARGE_CLASSIFICATION_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateChargeClassificationType.CIVIL: "Used when a person has been charged with "
    "having committed a civil offense (also known as a municipal infraction), which "
    "is defined as a violation of a local law enacted by a city or town. Also "
    "describes a charge resulting from a case involving a private dispute between "
    "persons or organizations (as opposed to a criminal case due to the violation of "
    "a law).",
    StateChargeClassificationType.FELONY: "Used when a person has been charged with "
    "having committed a felony offense, which is defined as a crime of high "
    "seriousness.",
    StateChargeClassificationType.INFRACTION: "Used when a person has been charged "
    "with having committed a civil offense (also known as a municipal infraction), "
    "which is defined as a violation of a local law enacted by a city or town. Also "
    "describes a charge resulting from a case involving a private dispute between "
    "persons or organizations (as opposed to a criminal case due to the violation of "
    "a law). TODO(#12648): THIS WILL SOON BE MERGED WITH `CIVIL`. IF YOU ARE ADDING "
    "NEW ENUM MAPPINGS, USE `CIVIL` INSTEAD.",
    StateChargeClassificationType.MISDEMEANOR: "Used when a person has been charged "
    "with having committed a misdemeanor offense, which is defined as a crime of "
    "moderate seriousness.",
}


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
class StateChargeStatus(StateEntityEnum):
    ACQUITTED = state_enum_strings.state_charge_status_acquitted
    ADJUDICATED = state_enum_strings.state_charge_status_adjudicated
    CONVICTED = state_enum_strings.state_charge_status_convicted
    DROPPED = state_enum_strings.state_charge_status_dropped
    PENDING = state_enum_strings.state_charge_status_pending
    TRANSFERRED_AWAY = state_enum_strings.state_charge_status_transferred_away
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, Optional["StateChargeStatus"]]:
        return _STATE_CHARGE_STATUS_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "The status of the charge in court."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_CHARGE_STATUS_VALUE_DESCRIPTIONS


_STATE_CHARGE_STATUS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateChargeStatus.ACQUITTED: "Used when the person has been freed completely from "
    "the charge due to a case reaching a verdict of not guilty.",
    StateChargeStatus.ADJUDICATED: "Used when there has been a final judgement on a "
    "charge by a judge, but the result is not legally classified as a conviction. For "
    "example, this is used in Maine for all charges in juvenile court that result in "
    "a guilty plea or if the court has found the juvenile guilty of the charge.",
    StateChargeStatus.CONVICTED: "Used when the person has been declared guilty of the "
    "offense by a judge or a jury.",
    StateChargeStatus.DROPPED: "Describes a charge that was dropped by the prosecution, "
    "which means the person has been freed completely from the charge.",
    StateChargeStatus.PENDING: "Used when the charges have been filed with the court, "
    "but there has not yet been a result of the case.",
    StateChargeStatus.TRANSFERRED_AWAY: "Used when the case associated with the charge "
    "has been transferred to another jurisdiction outside of the state.",
}


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_STATE_CHARGE_CLASSIFICATION_TYPE_MAP = {
    "*": None,
    "-": None,
    ".": None,
    "0": None,
    "ADMINISTRATIVE": StateChargeClassificationType.CIVIL,
    "C": StateChargeClassificationType.CIVIL,
    "CITED CITATION": StateChargeClassificationType.INFRACTION,
    "CIVIL": StateChargeClassificationType.CIVIL,
    "COMM": StateChargeClassificationType.CIVIL,
    "CRIMINAL TRAFFIC VIOLATION": StateChargeClassificationType.INFRACTION,
    "ENHANCEMENT": None,
    "EXTERNAL UNKNOWN": StateChargeClassificationType.EXTERNAL_UNKNOWN,
    "F": StateChargeClassificationType.FELONY,
    "FEL": StateChargeClassificationType.FELONY,
    "FELONY": StateChargeClassificationType.FELONY,
    "I": StateChargeClassificationType.INFRACTION,
    "INFRACTION": StateChargeClassificationType.INFRACTION,
    "M": StateChargeClassificationType.MISDEMEANOR,
    "MIS": StateChargeClassificationType.MISDEMEANOR,
    "MISD": StateChargeClassificationType.MISDEMEANOR,
    "MISDEMEANOR": StateChargeClassificationType.MISDEMEANOR,
    "MUNICIPAL ORDINANCE VIOLATION": StateChargeClassificationType.INFRACTION,
    "N A": None,
    "NA": None,
    "NON ARREST TRAFFIC VIOLATION": StateChargeClassificationType.INFRACTION,
    "NON MOVING TRAFFIC VIOLATION": StateChargeClassificationType.INFRACTION,
    "SUMMONS": StateChargeClassificationType.INFRACTION,
    "U": StateChargeClassificationType.EXTERNAL_UNKNOWN,
    "UNKNOWN": StateChargeClassificationType.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateChargeClassificationType.INTERNAL_UNKNOWN,
}


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_STATE_CHARGE_STATUS_MAP = {
    "24HR HOLD": StateChargeStatus.PENDING,
    "ACQUITTED": StateChargeStatus.ACQUITTED,
    "ADJUDICATED": StateChargeStatus.ADJUDICATED,
    "ALT SENT": StateChargeStatus.CONVICTED,
    "AMENDED": None,
    "APPEALED": StateChargeStatus.CONVICTED,
    "BONDED": StateChargeStatus.PENDING,
    "CASE DISMISSED": StateChargeStatus.DROPPED,
    "CASE DISPOSED": StateChargeStatus.DROPPED,
    "CASE RESOLVED": StateChargeStatus.DROPPED,
    "CONFINEMENT IN RESPO VIOL": StateChargeStatus.CONVICTED,
    "CHARGE NOLLE PROSEQUI": StateChargeStatus.DROPPED,
    "CHARGE NOT FILED BY PROSECUTOR": StateChargeStatus.DROPPED,
    "CHARGES DISMISSED": StateChargeStatus.DROPPED,
    "CHARGES DROPPED": StateChargeStatus.DROPPED,
    "CHARGES NOT FILED": StateChargeStatus.DROPPED,
    "CHG DISMISSED": StateChargeStatus.DROPPED,
    "COSTS FINES": StateChargeStatus.CONVICTED,
    "COURT ORDER OF RELEASE": StateChargeStatus.DROPPED,
    "COURT ORDER RELEASED": StateChargeStatus.DROPPED,
    "COURT ORDERED RELEASED": StateChargeStatus.DROPPED,
    "COURT RELEASE": StateChargeStatus.DROPPED,
    "COURT RELEASED": StateChargeStatus.DROPPED,
    "CONVICTED": StateChargeStatus.CONVICTED,
    "COUNTY JAIL TIME": StateChargeStatus.CONVICTED,
    "DECLINED TO PROSECUTE": StateChargeStatus.DROPPED,
    "DISMISS": StateChargeStatus.DROPPED,
    "DISMISSAL": StateChargeStatus.DROPPED,
    "DISMISSED": StateChargeStatus.DROPPED,
    "DISMISSED AT COURT": StateChargeStatus.DROPPED,
    "DISMISSED BY DISTRICT ATTORNEY": StateChargeStatus.DROPPED,
    "DISMISSED BY THE COURT": StateChargeStatus.DROPPED,
    "DROPPED": StateChargeStatus.DROPPED,
    "DROPPED ABANDONED": StateChargeStatus.DROPPED,
    "DROPPED CHARGES": StateChargeStatus.DROPPED,
    "DRUG COURT SANCTION": StateChargeStatus.CONVICTED,
    "ENTERED IN ERROR": None,
    "ENHANCEMENT": None,
    # End of Sentence
    "EXTERNAL UNKNOWN": StateChargeStatus.EXTERNAL_UNKNOWN,
    "FINAL SENTENCED": StateChargeStatus.CONVICTED,
    "FINED": StateChargeStatus.CONVICTED,
    "FOUND NOT GUILTY AT TRIAL": StateChargeStatus.ACQUITTED,
    "GENERAL": None,
    "GUILTY": StateChargeStatus.CONVICTED,
    "GUILTY PEND SENTENCING": StateChargeStatus.CONVICTED,
    "HELD TO GRAND JURY": StateChargeStatus.PENDING,
    "INTAKE": StateChargeStatus.PENDING,
    "LIFTED": StateChargeStatus.DROPPED,
    "NO GRAND JURY ACTION TAKEN": StateChargeStatus.DROPPED,
    "M R S": None,
    "MOOT": StateChargeStatus.DROPPED,
    # Defendant Not In Court
    "NIC": StateChargeStatus.PENDING,
    "NOELLEPR": StateChargeStatus.DROPPED,
    "NOTFILED": StateChargeStatus.DROPPED,
    "NOLLE PROS": StateChargeStatus.DROPPED,
    "NOLLE PROSE": StateChargeStatus.DROPPED,
    "NOLLE PROSEQUI": StateChargeStatus.DROPPED,
    "NOLLED PROSSED": StateChargeStatus.DROPPED,
    "NOLPROSSED": StateChargeStatus.DROPPED,
    "NG NOT GUILTY": StateChargeStatus.ACQUITTED,
    "NO INFO": None,
    "NO PROBABLE CAUSE": StateChargeStatus.DROPPED,
    "NOT FILED": StateChargeStatus.DROPPED,
    "NOT GUILTY": StateChargeStatus.ACQUITTED,
    "NOTICE OF APPEAL": StateChargeStatus.CONVICTED,
    "NOTICE OF DISCHARGE": StateChargeStatus.CONVICTED,
    "NOTICE OF DISCHARGE DOC": StateChargeStatus.CONVICTED,
    "ORDER TO FOLLOW": StateChargeStatus.CONVICTED,
    "ORDER OF RELEASE": StateChargeStatus.DROPPED,
    "OTHER": None,
    "OTHER SEE NOTES": None,
    "OTHER W EXPLANATION": None,
    "PAROLE": StateChargeStatus.CONVICTED,
    "PAROLED": StateChargeStatus.CONVICTED,
    "PAROLED BY COURT OF RECORD": StateChargeStatus.CONVICTED,
    "PAROLE PROBATION REINSTATED": StateChargeStatus.CONVICTED,
    "PAROLE PROBATION REVOKED": StateChargeStatus.CONVICTED,
    "PELIMARY HEARING": StateChargeStatus.PENDING,
    "PEND SANCTION": StateChargeStatus.CONVICTED,
    "PENDIGN ARRIGNMENT": StateChargeStatus.PENDING,
    "PENDING": StateChargeStatus.PENDING,
    "PENDING ARRIGNMENT": StateChargeStatus.PENDING,
    "PENDING CASE": StateChargeStatus.PENDING,
    "PENDING SANCTION": StateChargeStatus.CONVICTED,
    "PENDING SEE COMMENTS BELOW": StateChargeStatus.PENDING,
    "PENDING TRANSPORT": StateChargeStatus.PENDING,
    "PRESENT WITHOUT INFO": StateChargeStatus.PRESENT_WITHOUT_INFO,
    "PROB REVOKED": StateChargeStatus.CONVICTED,
    "PROBATED": StateChargeStatus.CONVICTED,
    "PROBATION": StateChargeStatus.CONVICTED,
    "PROBATION AND PAROLE": StateChargeStatus.CONVICTED,
    "PROBATION HOLD": StateChargeStatus.CONVICTED,
    "PROBATION REVOCATION": StateChargeStatus.CONVICTED,
    "PROBATION REVOKED": StateChargeStatus.CONVICTED,
    "PLEAD": StateChargeStatus.CONVICTED,
    "RELEASE": StateChargeStatus.DROPPED,
    "RELEASE PER JUDGE": StateChargeStatus.DROPPED,
    "RELEASED": StateChargeStatus.DROPPED,
    "RELEASED BY COURT": StateChargeStatus.DROPPED,
    "RELEASED BY JUDGE": StateChargeStatus.DROPPED,
    "RELEASED PER JUDGE": StateChargeStatus.DROPPED,
    "RELEASED FROM CUSTODY": StateChargeStatus.DROPPED,
    "RELEASED THIS CASE ONLY": StateChargeStatus.DROPPED,
    "RELEASE TO WORK RELEASE": StateChargeStatus.CONVICTED,
    "REPORT IN": StateChargeStatus.CONVICTED,
    "RESCINDED": StateChargeStatus.DROPPED,
    "REVOKED": StateChargeStatus.CONVICTED,
    # Release on your Own Recognizance
    "ROR": StateChargeStatus.PENDING,
    # Probably: Sentenced / Community Control
    "S COMM": StateChargeStatus.CONVICTED,
    # Probably: Sentenced / Department of Corrections
    "S DOC": StateChargeStatus.CONVICTED,
    "S JAIL": StateChargeStatus.CONVICTED,
    "S PROB": StateChargeStatus.CONVICTED,
    # Probably: Sentenced / Probation
    "S PROG": StateChargeStatus.CONVICTED,
    "SAFE KEEPING": StateChargeStatus.PENDING,
    "SANCTION": StateChargeStatus.CONVICTED,
    "SENTENCED": StateChargeStatus.CONVICTED,
    "SENTENCED ON CHARGES": StateChargeStatus.CONVICTED,
    "SENTENCED STATE YEARS": StateChargeStatus.CONVICTED,
    "SENTENCED TO PROBATION": StateChargeStatus.CONVICTED,
    "SERVE OUT": StateChargeStatus.CONVICTED,
    "SERVING MISD TIME": StateChargeStatus.CONVICTED,
    "SERVING SENTENCE": StateChargeStatus.CONVICTED,
    "SERVING TIME": StateChargeStatus.CONVICTED,
    "SERVIING TIME": StateChargeStatus.CONVICTED,
    "SHOCK PROBATED": StateChargeStatus.CONVICTED,
    "SPECIFY AT NOTES": None,
    "SUPERVISED PROBATION": StateChargeStatus.CONVICTED,
    "TEMPORARY CUSTODY ORDER": StateChargeStatus.PENDING,
    # Turned In By Bondsman
    "TIBB": StateChargeStatus.PENDING,
    "TIME SUSPENDED": StateChargeStatus.CONVICTED,
    "TRANSFERRED AWAY": StateChargeStatus.TRANSFERRED_AWAY,
    "TRANSFERRED_AWAY": StateChargeStatus.TRANSFERRED_AWAY,
    "UNDER SENTENCE": StateChargeStatus.CONVICTED,
    "UNKNOWN": StateChargeStatus.EXTERNAL_UNKNOWN,
    "WAVIER SIGNED": StateChargeStatus.DROPPED,
    "WEEKENDER": StateChargeStatus.CONVICTED,
    "WRIT OF HABEAS CORPUS": None,
    "WRONG PERSON BOOKED": StateChargeStatus.DROPPED,
    "UNSPECIFIED": StateChargeStatus.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateChargeStatus.INTERNAL_UNKNOWN,
}
