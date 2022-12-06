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

"""Constants related to a StateSupervisionViolatedConditionEntry."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSupervisionViolatedConditionType(StateEntityEnum):
    """The type of violation of a condition of supervision."""

    # Failure to maintain employment as required by supervision conditions
    EMPLOYMENT = state_enum_strings.state_supervision_violated_condition_type_employment

    # Failure to notify agent of travel or residence changes, employment changes,
    # arrests or citations, or other relevant conditions
    FAILURE_TO_NOTIFY = (
        state_enum_strings.state_supervision_violated_condition_type_failure_to_notify
    )

    # Failure to report as required to supervision agent
    FAILURE_TO_REPORT = (
        state_enum_strings.state_supervision_violated_condition_type_failure_to_report
    )

    # Failure to pay supervision-related fees, or inability to support dependents
    FINANCIAL = state_enum_strings.state_supervision_violated_condition_type_financial

    # Conviction for offenses or pending criminal charges, and weapons possession
    LAW = state_enum_strings.state_supervision_violated_condition_type_law

    # Includes violation of any other condition of supervision not covered by financial, employment,
    # or treatment requirements — this may include violations of curfew, association with victims or
    # persons convicted of crime, violation of electronic monitoring, and failure to abide by imposed
    # board or field imposed special conditions
    SPECIAL_CONDITIONS = (
        state_enum_strings.state_supervision_violated_condition_type_special_conditions
    )

    # Possession or use of controlled substances and/or alcohol
    SUBSTANCE = state_enum_strings.state_supervision_violated_condition_type_substance

    # Failure to complete treatment or comply with conditions of treatment
    TREATMENT_COMPLIANCE = (
        state_enum_strings.state_supervision_violated_condition_type_treatment_compliance
    )

    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionViolatedConditionType"]:
        return _STATE_SUPERVISION_VIOLATED_CONDITION_TYPE_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The category of a person’s behavior that violated a condition of "
            "their supervision."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_VIOLATED_CONDITION_TYPE_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_VIOLATED_CONDITION_TYPE_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateSupervisionViolatedConditionType.EMPLOYMENT: "Failure to maintain employment as required "
    "by supervision conditions",
    StateSupervisionViolatedConditionType.FAILURE_TO_NOTIFY: "Failure to notify agent of travel or residence "
    "changes, employment changes, arrests or citations, or other relevant conditions",
    StateSupervisionViolatedConditionType.FAILURE_TO_REPORT: "Failure to report as required to supervision agent",
    StateSupervisionViolatedConditionType.FINANCIAL: "Failure to pay supervision-related fees, or inability to "
    "support dependents",
    StateSupervisionViolatedConditionType.LAW: "Conviction for offenses or pending criminal charges, "
    "and weapons possession",
    StateSupervisionViolatedConditionType.SPECIAL_CONDITIONS: "Includes violation of any other condition of "
    "supervision not covered by financial, employment, or treatment requirements — this may include violations "
    "of curfew, association with victims or persons convicted of crime, violation of electronic monitoring, and "
    "failure to abide by imposed board or field imposed special conditions",
    StateSupervisionViolatedConditionType.SUBSTANCE: "Possession or use of controlled substances and/or alcohol",
    StateSupervisionViolatedConditionType.TREATMENT_COMPLIANCE: "Failure to complete treatment or comply with "
    "conditions of treatment",
}

_STATE_SUPERVISION_VIOLATED_CONDITION_TYPE_MAP = {
    "EMPLOYMENT": StateSupervisionViolatedConditionType.EMPLOYMENT,
    "FAILURE TO NOTIFY": StateSupervisionViolatedConditionType.FAILURE_TO_NOTIFY,
    "FAILURE TO REPORT": StateSupervisionViolatedConditionType.FAILURE_TO_REPORT,
    "FINANCIAL": StateSupervisionViolatedConditionType.FINANCIAL,
    "LAW": StateSupervisionViolatedConditionType.LAW,
    "SPECIAL CONDITIONS": StateSupervisionViolatedConditionType.SPECIAL_CONDITIONS,
    "SUBSTANCE": StateSupervisionViolatedConditionType.SUBSTANCE,
    "TREATMENT COMPLIANCE": StateSupervisionViolatedConditionType.TREATMENT_COMPLIANCE,
    "EXTERNAL UNKNOWN": StateSupervisionViolatedConditionType.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateSupervisionViolatedConditionType.INTERNAL_UNKNOWN,
}
