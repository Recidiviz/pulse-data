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
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StateChargeClassificationType(StateEntityEnum):
    CIVIL = state_enum_strings.state_charge_classification_type_civil
    FELONY = state_enum_strings.state_charge_classification_type_felony
    MISDEMEANOR = state_enum_strings.state_charge_classification_type_misdemeanor
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

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
    StateChargeClassificationType.MISDEMEANOR: "Used when a person has been charged "
    "with having committed a misdemeanor offense, which is defined as a crime of "
    "moderate seriousness.",
}


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


@unique
class StateChargeV2ClassificationType(StateEntityEnum):
    """A copy of state charge classification type to use in sentencing v2"""

    CIVIL = state_enum_strings.state_charge_v2_classification_type_civil
    FELONY = state_enum_strings.state_charge_v2_classification_type_felony
    MISDEMEANOR = state_enum_strings.state_charge_v2_classification_type_misdemeanor
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The classification of the offense a person has been charged with."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_CHARGE_V2_CLASSIFICATION_TYPE_VALUE_DESCRIPTIONS


_STATE_CHARGE_V2_CLASSIFICATION_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateChargeV2ClassificationType.CIVIL: "Used when a person has been charged with "
    "having committed a civil offense (also known as a municipal infraction), which "
    "is defined as a violation of a local law enacted by a city or town. Also "
    "describes a charge resulting from a case involving a private dispute between "
    "persons or organizations (as opposed to a criminal case due to the violation of "
    "a law).",
    StateChargeV2ClassificationType.FELONY: "Used when a person has been charged with "
    "having committed a felony offense, which is defined as a crime of high "
    "seriousness.",
    StateChargeV2ClassificationType.MISDEMEANOR: "Used when a person has been charged "
    "with having committed a misdemeanor offense, which is defined as a crime of "
    "moderate seriousness.",
}


class StateChargeV2Status(StateEntityEnum):
    """A copy of state charge status to use in sentencing v2"""

    ACQUITTED = state_enum_strings.state_charge_v2_status_acquitted
    ADJUDICATED = state_enum_strings.state_charge_v2_status_adjudicated
    CONVICTED = state_enum_strings.state_charge_v2_status_convicted
    DROPPED = state_enum_strings.state_charge_v2_status_dropped
    PENDING = state_enum_strings.state_charge_v2_status_pending
    TRANSFERRED_AWAY = state_enum_strings.state_charge_v2_status_transferred_away
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The status of the charge in court."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_CHARGE_V2_STATUS_VALUE_DESCRIPTIONS


_STATE_CHARGE_V2_STATUS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateChargeV2Status.ACQUITTED: "Used when the person has been freed completely from "
    "the charge due to a case reaching a verdict of not guilty.",
    StateChargeV2Status.ADJUDICATED: "Used when there has been a final judgement or decision on a "
    "charge by a judge or authority, but the result is not legally classified as a conviction. For "
    "example: alternative/suspended sentencing, types of diversion programs without a conviction, "
    "and types of juvenile/youth courts.",
    StateChargeV2Status.CONVICTED: "Used when the person has been declared guilty of the "
    "offense by a judge or a jury.",
    StateChargeV2Status.DROPPED: "Describes a charge that was dropped by the prosecution, "
    "which means the person has been freed completely from the charge.",
    StateChargeV2Status.PENDING: "Used when the charges have been filed with the court, "
    "but there has not yet been a result of the case.",
    StateChargeV2Status.TRANSFERRED_AWAY: "Used when the case associated with the charge "
    "has been transferred to another jurisdiction outside of the state.",
}
