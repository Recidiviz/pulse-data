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
"""Constants related to a StateTaskDeadline."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateTaskType(StateEntityEnum):
    """Enum listing all actionable tasks relating to a person's incarceration /
    supervision that someone with administrative power in the DOC might take."""

    APPEAL_FOR_TRANSFER_TO_SUPERVISION_FROM_INCARCERATION = (
        state_enum_strings.state_task_type_appeal_for_transfer_to_supervision_from_incarceration
    )
    ARREST_CHECK = state_enum_strings.state_task_type_arrest_check
    SUPERVISION_CASE_PLAN_UPDATE = (
        state_enum_strings.state_task_type_supervision_case_plan_update
    )
    DISCHARGE_EARLY_FROM_SUPERVISION = (
        state_enum_strings.state_task_type_discharge_early_from_supervision
    )
    DISCHARGE_FROM_INCARCERATION = (
        state_enum_strings.state_task_type_discharge_from_incarceration
    )
    DISCHARGE_FROM_SUPERVISION = (
        state_enum_strings.state_task_type_discharge_from_supervision
    )
    DRUG_SCREEN = state_enum_strings.state_task_type_drug_screen
    EMPLOYMENT_VERIFICATION = state_enum_strings.state_task_type_employment_verification
    FACE_TO_FACE_CONTACT = state_enum_strings.state_task_type_face_to_face_contact
    HOME_VISIT = state_enum_strings.state_task_type_home_visit
    NEW_ASSESSMENT = state_enum_strings.state_task_type_new_assessment
    PAYMENT_VERIFICATION = state_enum_strings.state_task_type_payment_verification
    SPECIAL_CONDITION_VERIFICATION = (
        state_enum_strings.state_task_type_special_condition_verification
    )
    TRANSFER_TO_ADMINISTRATIVE_SUPERVISION = (
        state_enum_strings.state_task_type_transfer_to_administrative_supervision
    )
    TRANSFER_TO_SUPERVISION_FROM_INCARCERATION = (
        state_enum_strings.state_task_type_transfer_to_supervision_from_incarceration
    )
    TREATMENT_REFERRAL = state_enum_strings.state_task_type_treatment_referral
    TREATMENT_VERIFICATION = state_enum_strings.state_task_type_treatment_verification
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateTaskType"]:
        return _STATE_TASK_TYPE_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "Describes a type of task that may be performed as part of someone’s "
            "supervision or incarceration term."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_TASK_TYPES_VALUE_DESCRIPTIONS


_STATE_TASK_TYPES_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateTaskType.APPEAL_FOR_TRANSFER_TO_SUPERVISION_FROM_INCARCERATION: (
        "Appeal for this person to serve out the remainder of their sentence on some "
        "form of supervision (e.g. transfer to parole or ME’s SCCP program)."
    ),
    StateTaskType.ARREST_CHECK: (
        "Check (usually in some central DB) that this person has no new arrest history."
    ),
    StateTaskType.SUPERVISION_CASE_PLAN_UPDATE: (
        "Update this person’s case plan, which is an artifact that outlines goals and "
        "an implementation plan for this person’s supervision term."
    ),
    StateTaskType.DISCHARGE_EARLY_FROM_SUPERVISION: (
        "Discharge this person from supervision before their official termination date "
        "(i.e. as part of some sort of early discharge program), or update appropriate"
        "data systems if it is determined they are not yet eligible."
    ),
    StateTaskType.DISCHARGE_FROM_INCARCERATION: (
        "Discharge this person from incarceration because they have passed their "
        "official sentence termination date (i.e. this person is overdue for "
        "discharge). This person may transition to supervision if they are sentenced "
        "to serve a stacked supervision sentence."
    ),
    StateTaskType.DISCHARGE_FROM_SUPERVISION: (
        "Discharge this person from supervision because they have passed their "
        "official termination date (i.e. this person is overdue for discharge)."
    ),
    StateTaskType.DRUG_SCREEN: "Screen this person for drug use.",
    StateTaskType.EMPLOYMENT_VERIFICATION: "Verify this person’s employment status.",
    StateTaskType.FACE_TO_FACE_CONTACT: (
        "Meet with this person. This could be at home, in office, or virtual. If a "
        "particular type of contact is required, it should be specified in the "
        "`StateTaskDeadline` `task_subtype` field."
    ),
    StateTaskType.HOME_VISIT: (
        "Visit this person’s home either physically or virtually."
    ),
    StateTaskType.NEW_ASSESSMENT: (
        "Perform a risk (or other) assessment on this person. More information about "
        "the type of assessment may be specified in the `StateTaskDeadline` "
        "`task_subtype` field."
    ),
    StateTaskType.PAYMENT_VERIFICATION: (
        "Verify whether this person has paid any requisite fines and fees."
    ),
    StateTaskType.SPECIAL_CONDITION_VERIFICATION: (
        "Verify that the person is complying with all special conditions of their "
        "supervision and that those conditions are up-to-date, if there are any. May "
        "require contacting said person, or could involve gathering information from "
        "other sources."
    ),
    StateTaskType.TRANSFER_TO_ADMINISTRATIVE_SUPERVISION: (
        "Transfer this person from their current supervision level to an "
        "administrative supervision level (e.g. LSU, Compliant Reporting, etc.)."
    ),
    StateTaskType.TRANSFER_TO_SUPERVISION_FROM_INCARCERATION: (
        "Transfer this incarcerated person to serve out the remainder of their "
        "sentence on some form of supervision (e.g. transfer to parole or "
        "ME’s SCCP program)."
    ),
    StateTaskType.TREATMENT_REFERRAL: (
        "Make a referral for this person to participate in a treatment program now "
        "that this person has been assessed to need treatment."
    ),
    StateTaskType.TREATMENT_VERIFICATION: (
        "Verify this person’s participation in a particular treatment program by "
        "contacting the program itself, not the person participating in the program. "
        "May often be described as a “treatment collateral contact”."
    ),
}


_STATE_TASK_TYPE_MAP = {
    "APPEAL FOR TRANSFER TO SUPERVISION FROM INCARCERATION": StateTaskType.APPEAL_FOR_TRANSFER_TO_SUPERVISION_FROM_INCARCERATION,
    "ARREST CHECK": StateTaskType.ARREST_CHECK,
    "SUPERVISION CASE PLAN UPDATE": StateTaskType.SUPERVISION_CASE_PLAN_UPDATE,
    "DISCHARGE EARLY FROM SUPERVISION": StateTaskType.DISCHARGE_EARLY_FROM_SUPERVISION,
    "DISCHARGE FROM INCARCERATION": StateTaskType.DISCHARGE_FROM_INCARCERATION,
    "DISCHARGE FROM SUPERVISION": StateTaskType.DISCHARGE_FROM_SUPERVISION,
    "DRUG SCREEN": StateTaskType.DRUG_SCREEN,
    "EMPLOYMENT VERIFICATION": StateTaskType.EMPLOYMENT_VERIFICATION,
    "FACE TO FACE CONTACT": StateTaskType.FACE_TO_FACE_CONTACT,
    "HOME VISIT": StateTaskType.HOME_VISIT,
    "NEW ASSESSMENT": StateTaskType.NEW_ASSESSMENT,
    "PAYMENT VERIFICATION": StateTaskType.PAYMENT_VERIFICATION,
    "SPECIAL CONDITION VERIFICATION": StateTaskType.SPECIAL_CONDITION_VERIFICATION,
    "TRANSFER TO ADMINISTRATIVE SUPERVISION": StateTaskType.TRANSFER_TO_ADMINISTRATIVE_SUPERVISION,
    "TRANSFER TO SUPERVISION FROM INCARCERATION": StateTaskType.TRANSFER_TO_SUPERVISION_FROM_INCARCERATION,
    "TREATMENT REFERRAL": StateTaskType.TREATMENT_REFERRAL,
    "TREATMENT VERIFICATION": StateTaskType.TREATMENT_VERIFICATION,
    "EXTERNAL UNKNOWN": StateTaskType.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateTaskType.INTERNAL_UNKNOWN,
}
