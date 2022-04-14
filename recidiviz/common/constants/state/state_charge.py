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

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


# TODO(#8905): Change superclass to Enum, remove EntityEnumMeta metaclass, and delete
#  _get_default_map() once all state ingest views have been migrated to v2 mappings.
@unique
class StateChargeClassificationType(EntityEnum, metaclass=EntityEnumMeta):
    CIVIL = state_enum_strings.state_charge_classification_type_civil
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    FELONY = state_enum_strings.state_charge_classification_type_felony
    INFRACTION = state_enum_strings.state_charge_classification_type_infraction
    MISDEMEANOR = state_enum_strings.state_charge_classification_type_misdemeanor
    OTHER = state_enum_strings.state_charge_classification_type_other

    @staticmethod
    def _get_default_map() -> Dict[str, Optional["StateChargeClassificationType"]]:
        return _STATE_CHARGE_CLASSIFICATION_TYPE_MAP


# TODO(#8905): Change superclass to Enum, remove EntityEnumMeta metaclass, and delete
#  _get_default_map() once all state ingest views have been migrated to v2 mappings.
class StateChargeStatus(EntityEnum, metaclass=EntityEnumMeta):
    ACQUITTED = state_enum_strings.state_charge_status_acquitted
    ADJUDICATED = state_enum_strings.state_charge_status_adjudicated
    COMPLETED_SENTENCE = state_enum_strings.state_charge_status_completed
    CONVICTED = state_enum_strings.state_charge_status_convicted
    DROPPED = state_enum_strings.state_charge_status_dropped
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    INFERRED_DROPPED = state_enum_strings.state_charge_status_inferred_dropped
    PENDING = state_enum_strings.state_charge_status_pending
    PRETRIAL = state_enum_strings.state_charge_status_pretrial
    SENTENCED = state_enum_strings.state_charge_status_sentenced
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info
    REMOVED_WITHOUT_INFO = enum_strings.removed_without_info
    TRANSFERRED_AWAY = state_enum_strings.state_charge_status_transferred_away

    @staticmethod
    def _get_default_map() -> Dict[str, Optional["StateChargeStatus"]]:
        return _STATE_CHARGE_STATUS_MAP


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
    "O": StateChargeClassificationType.OTHER,
    "OTH": StateChargeClassificationType.OTHER,
    "OTHER": StateChargeClassificationType.OTHER,
    "SUMMONS": StateChargeClassificationType.INFRACTION,
    "U": StateChargeClassificationType.EXTERNAL_UNKNOWN,
    "UNKNOWN": StateChargeClassificationType.EXTERNAL_UNKNOWN,
}


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.

# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_STATE_CHARGE_STATUS_MAP = {
    "24HR HOLD": StateChargeStatus.PENDING,
    "ACCEPTED": StateChargeStatus.PRETRIAL,
    "ACQUITTED": StateChargeStatus.ACQUITTED,
    "ADJUDICATED": StateChargeStatus.ADJUDICATED,
    "ADMINISTRATIVE RELEASE": StateChargeStatus.PRETRIAL,
    "ALT SENT": StateChargeStatus.SENTENCED,
    "AMENDED": None,
    "APPEALED": StateChargeStatus.SENTENCED,
    "AWAITING": StateChargeStatus.PRETRIAL,
    "AWAITING COURT": StateChargeStatus.PRETRIAL,
    "AWAITING PRETRIAL": StateChargeStatus.PRETRIAL,
    "AWAITING TRIAL": StateChargeStatus.PRETRIAL,
    "BAIL SET": StateChargeStatus.PRETRIAL,
    "BOND OUT": StateChargeStatus.PRETRIAL,
    "BOND SURRENDER": StateChargeStatus.PRETRIAL,
    "BONDED": StateChargeStatus.PENDING,
    "BOUND OVER": StateChargeStatus.PRETRIAL,
    "BOUND OVER TO GRAND JURY": StateChargeStatus.PRETRIAL,
    "BOUND TO GRAND JURY": StateChargeStatus.PRETRIAL,
    "POST CASH BAIL": StateChargeStatus.PRETRIAL,
    "CASE DISMISSED": StateChargeStatus.DROPPED,
    "CASE DISPOSED": StateChargeStatus.DROPPED,
    "CASE RESOLVED": StateChargeStatus.DROPPED,
    "CONFINED NOT CONVICTED": StateChargeStatus.PRETRIAL,
    "CONFINEMENT IN RESPO VIOL": StateChargeStatus.SENTENCED,
    "CHARGE NOLLE PROSEQUI": StateChargeStatus.DROPPED,
    "CHARGE NOT FILED BY PROSECUTOR": StateChargeStatus.DROPPED,
    "CHARGES DISMISSED": StateChargeStatus.DROPPED,
    "CHARGES DROPPED": StateChargeStatus.DROPPED,
    "CHARGES NOT FILED": StateChargeStatus.DROPPED,
    "CHARGES SATISFIED": StateChargeStatus.COMPLETED_SENTENCE,
    "CHG DISMISSED": StateChargeStatus.DROPPED,
    "COMMITMENT ORDER": StateChargeStatus.PRETRIAL,
    "COMPLETED": StateChargeStatus.COMPLETED_SENTENCE,
    "COMPLETED SENTENCE": StateChargeStatus.COMPLETED_SENTENCE,
    "COSTS FINES": StateChargeStatus.SENTENCED,
    "COURT ORDER OF RELEASE": StateChargeStatus.DROPPED,
    "COURT ORDER RELEASED": StateChargeStatus.DROPPED,
    "COURT ORDERED RELEASED": StateChargeStatus.DROPPED,
    "COURT RELEASE": StateChargeStatus.DROPPED,
    "COURT RELEASED": StateChargeStatus.DROPPED,
    "CONVICTED": StateChargeStatus.CONVICTED,
    "COUNTY JAIL TIME": StateChargeStatus.SENTENCED,
    "DECLINED TO PROSECUTE": StateChargeStatus.DROPPED,
    "DEFENDANT INDICTED": StateChargeStatus.PRETRIAL,
    "DETAINED": StateChargeStatus.PRETRIAL,
    "DISMISS": StateChargeStatus.DROPPED,
    "DISMISSAL": StateChargeStatus.DROPPED,
    "DISMISSED": StateChargeStatus.DROPPED,
    "DISMISSED AT COURT": StateChargeStatus.DROPPED,
    "DISMISSED BY DISTRICT ATTORNEY": StateChargeStatus.DROPPED,
    "DISMISSED BY THE COURT": StateChargeStatus.DROPPED,
    "DROPPED": StateChargeStatus.DROPPED,
    "DROPPED ABANDONED": StateChargeStatus.DROPPED,
    "DROPPED CHARGES": StateChargeStatus.DROPPED,
    "DRUG COURT SANCTION": StateChargeStatus.SENTENCED,
    "ENTERED IN ERROR": None,
    "ENHANCEMENT": None,
    # End of Sentence
    "EOS": StateChargeStatus.COMPLETED_SENTENCE,
    "EXTERNAL UNKNOWN": StateChargeStatus.EXTERNAL_UNKNOWN,
    "FILED PENDING TRIAL": StateChargeStatus.PRETRIAL,
    "FINAL SENTENCED": StateChargeStatus.SENTENCED,
    "FINE CREDIT": StateChargeStatus.COMPLETED_SENTENCE,
    "FINES CREDIT": StateChargeStatus.COMPLETED_SENTENCE,
    "FINES COST CLOSED": StateChargeStatus.COMPLETED_SENTENCE,
    "FINED": StateChargeStatus.SENTENCED,
    "FOUND NOT GUILTY AT TRIAL": StateChargeStatus.ACQUITTED,
    "FTA": StateChargeStatus.PRETRIAL,
    "GENERAL": None,
    "GUILTY": StateChargeStatus.SENTENCED,
    "GUILTY PEND SENTENCING": StateChargeStatus.SENTENCED,
    "HELD TO GRAND JURY": StateChargeStatus.PENDING,
    "INDICTED": StateChargeStatus.PRETRIAL,
    "INDICTMENT BY GRAND JURY": StateChargeStatus.PRETRIAL,
    "INFERRED DROPPED": StateChargeStatus.INFERRED_DROPPED,
    "INTAKE": StateChargeStatus.PENDING,
    "INVESTIGATION": StateChargeStatus.PRETRIAL,
    "INVESTIGATIVE": StateChargeStatus.PRETRIAL,
    "INVESTIGATIVE HOLD": StateChargeStatus.PRETRIAL,
    "INDICTED BY GJ": StateChargeStatus.PRETRIAL,
    "LIFTED": StateChargeStatus.DROPPED,
    "NO GRAND JURY ACTION TAKEN": StateChargeStatus.DROPPED,
    "MINIMUM EXPIRATION": StateChargeStatus.COMPLETED_SENTENCE,
    "M R S": None,
    "MOOT": StateChargeStatus.DROPPED,
    "MUNICIPAL COURT": StateChargeStatus.PRETRIAL,
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
    "NO TRUE BILL": StateChargeStatus.PRETRIAL,
    "NOT FILED": StateChargeStatus.DROPPED,
    "NOT GUILTY": StateChargeStatus.ACQUITTED,
    "NOTICE OF APPEAL": StateChargeStatus.SENTENCED,
    "NOTICE OF DISCHARGE": StateChargeStatus.SENTENCED,
    "NOTICE OF DISCHARGE DOC": StateChargeStatus.SENTENCED,
    "OPEN": StateChargeStatus.PRETRIAL,
    "OPEN CASE PENDING": StateChargeStatus.PRETRIAL,
    "ORDER TO FOLLOW": StateChargeStatus.SENTENCED,
    "ORDER OF RELEASE": StateChargeStatus.DROPPED,
    "OTHER": None,
    "OTHER SEE NOTES": None,
    "OTHER W EXPLANATION": None,
    "PAROLE": StateChargeStatus.SENTENCED,
    "PAROLED": StateChargeStatus.SENTENCED,
    "PAROLED BY COURT OF RECORD": StateChargeStatus.SENTENCED,
    "PAROLE PROBATION REINSTATED": StateChargeStatus.SENTENCED,
    "PAROLE PROBATION REVOKED": StateChargeStatus.SENTENCED,
    "PAID FULL CASH BOND": StateChargeStatus.PRETRIAL,
    "PELIMARY HEARING": StateChargeStatus.PENDING,
    "PEND SANCTION": StateChargeStatus.SENTENCED,
    "PENDIGN ARRIGNMENT": StateChargeStatus.PENDING,
    "PENDING": StateChargeStatus.PENDING,
    "PENDING ARRIGNMENT": StateChargeStatus.PENDING,
    "PENDING CASE": StateChargeStatus.PENDING,
    "PENDING GRANDY JURY": StateChargeStatus.PRETRIAL,
    "PENDING SANCTION": StateChargeStatus.SENTENCED,
    "PENDING SEE COMMENTS BELOW": StateChargeStatus.PENDING,
    "PENDING TRANSPORT": StateChargeStatus.PENDING,
    "PRE SENTENCED": StateChargeStatus.PRETRIAL,
    "PRESENT WITHOUT INFO": StateChargeStatus.PRESENT_WITHOUT_INFO,
    "PRESENTENCED": StateChargeStatus.PRETRIAL,
    "PRE TRIAL": StateChargeStatus.PRETRIAL,
    "PRETRIAL RELEASE": StateChargeStatus.PRETRIAL,
    "PRETRIAL": StateChargeStatus.PRETRIAL,
    "PROB REVOKED": StateChargeStatus.SENTENCED,
    "PROBATED": StateChargeStatus.SENTENCED,
    "PROBATION": StateChargeStatus.SENTENCED,
    "PROBATION AND PAROLE": StateChargeStatus.SENTENCED,
    "PROBATION HOLD": StateChargeStatus.SENTENCED,
    "PROBATION REVOCATION": StateChargeStatus.SENTENCED,
    "PROBATION REVOKED": StateChargeStatus.SENTENCED,
    "PLEAD": StateChargeStatus.SENTENCED,
    "RELEASE": StateChargeStatus.DROPPED,
    "RELEASE PER JUDGE": StateChargeStatus.DROPPED,
    "RELEASED": StateChargeStatus.DROPPED,
    "RELEASED BY COURT": StateChargeStatus.DROPPED,
    "RELEASED BY JUDGE": StateChargeStatus.DROPPED,
    "RELEASED ON BOND": StateChargeStatus.PRETRIAL,
    "RELEASED PER JUDGE": StateChargeStatus.DROPPED,
    "RELEASED FROM CUSTODY": StateChargeStatus.DROPPED,
    "RELEASED TIME SERVED": StateChargeStatus.COMPLETED_SENTENCE,
    "RELEASED THIS CASE ONLY": StateChargeStatus.DROPPED,
    "RELEASE TO WORK RELEASE": StateChargeStatus.SENTENCED,
    "REMOVED WITHOUT INFO": StateChargeStatus.REMOVED_WITHOUT_INFO,
    "REPORT IN": StateChargeStatus.SENTENCED,
    "RESCINDED": StateChargeStatus.DROPPED,
    "REVOKED": StateChargeStatus.SENTENCED,
    # Release on your Own Recognizance
    "ROR": StateChargeStatus.PENDING,
    # Probably: Sentenced / Community Control
    "S COMM": StateChargeStatus.SENTENCED,
    # Probably: Sentenced / Department of Corrections
    "S DOC": StateChargeStatus.SENTENCED,
    "S JAIL": StateChargeStatus.SENTENCED,
    "S PROB": StateChargeStatus.SENTENCED,
    # Probably: Sentenced / Probation
    "S PROG": StateChargeStatus.SENTENCED,
    "SAFE KEEPING": StateChargeStatus.PENDING,
    "SANCTION": StateChargeStatus.SENTENCED,
    "SENTENCE SERVED": StateChargeStatus.COMPLETED_SENTENCE,
    "SENTENCED": StateChargeStatus.SENTENCED,
    "SENTENCED ON CHARGES": StateChargeStatus.SENTENCED,
    "SENTENCED STATE YEARS": StateChargeStatus.SENTENCED,
    "SENTENCED TO PROBATION": StateChargeStatus.SENTENCED,
    "SERVE OUT": StateChargeStatus.SENTENCED,
    "SERVED": StateChargeStatus.COMPLETED_SENTENCE,
    "SERVING MISD TIME": StateChargeStatus.SENTENCED,
    "SERVING SENTENCE": StateChargeStatus.SENTENCED,
    "SERVING TIME": StateChargeStatus.SENTENCED,
    "SERVIING TIME": StateChargeStatus.SENTENCED,
    "SHOCK PROBATED": StateChargeStatus.SENTENCED,
    "SPECIFY AT NOTES": None,
    "SUMMONS": StateChargeStatus.PRETRIAL,
    "SUPERVISED PROBATION": StateChargeStatus.SENTENCED,
    "T SERVD": StateChargeStatus.COMPLETED_SENTENCE,
    "TEMPORARY CUSTODY ORDER": StateChargeStatus.PENDING,
    # Turned In By Bondsman
    "TIBB": StateChargeStatus.PENDING,
    "TIME EXPIRED": StateChargeStatus.COMPLETED_SENTENCE,
    "TIME SERVED": StateChargeStatus.COMPLETED_SENTENCE,
    "TIME SUSPENDED": StateChargeStatus.SENTENCED,
    "TRANSFERRED AWAY": StateChargeStatus.TRANSFERRED_AWAY,
    "TRANSFERRED_AWAY": StateChargeStatus.TRANSFERRED_AWAY,
    "TRUE BILL OF INDICTMENT": StateChargeStatus.PRETRIAL,
    "UNDER SENTENCE": StateChargeStatus.SENTENCED,
    "UNKNOWN": StateChargeStatus.EXTERNAL_UNKNOWN,
    "UNSENTENCED": StateChargeStatus.PRETRIAL,
    "WAITING FOR TRIAL": StateChargeStatus.PRETRIAL,
    "WARRANT": StateChargeStatus.PRETRIAL,
    "WARRANT ISSUED": StateChargeStatus.PRETRIAL,
    "WARRANT SERVED": StateChargeStatus.PRETRIAL,
    "WAVIER SIGNED": StateChargeStatus.DROPPED,
    "WEEKENDER": StateChargeStatus.SENTENCED,
    "WRIT OF HABEAS CORPUS": None,
    "WRONG PERSON BOOKED": StateChargeStatus.DROPPED,
    "UNSPECIFIED": StateChargeStatus.EXTERNAL_UNKNOWN,
}
