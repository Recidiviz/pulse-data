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
"""Constants related to a StateEmploymentPeriod."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateEmploymentPeriodEmploymentStatus(StateEntityEnum):
    ALTERNATE_INCOME_SOURCE = (
        state_enum_strings.state_employment_period_employment_status_alternate_income_source
    )
    EMPLOYED = state_enum_strings.state_employment_period_employment_status_employed
    EMPLOYED_FULL_TIME = (
        state_enum_strings.state_employment_period_employment_status_employed_full_time
    )
    EMPLOYED_PART_TIME = (
        state_enum_strings.state_employment_period_employment_status_employed_part_time
    )
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    STUDENT = state_enum_strings.state_employment_period_employment_status_student
    UNABLE_TO_WORK = (
        state_enum_strings.state_employment_period_employment_status_unable_to_work
    )
    UNEMPLOYED = state_enum_strings.state_employment_period_employment_status_unemployed

    @staticmethod
    def _get_default_map() -> Dict[str, "StateEmploymentPeriodEmploymentStatus"]:
        return _STATE_EMPLOYMENT_PERIOD_EMPLOYMENT_STATUS_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "Describes a person's employment status during a given period of time."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_EMPLOYMENT_PERIOD_EMPLOYMENT_STATUS_VALUE_DESCRIPTIONS


_STATE_EMPLOYMENT_PERIOD_EMPLOYMENT_STATUS_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateEmploymentPeriodEmploymentStatus.ALTERNATE_INCOME_SOURCE: (
        "This person is unemployed, but has an alternate source of income (such as "
        "Social Security benefits) that allows them to remain unemployed."
    ),
    StateEmploymentPeriodEmploymentStatus.EMPLOYED: (
        "This person is employed by the specified employer, but we are unable to "
        "determine whether they are employed full-time or part-time."
    ),
    StateEmploymentPeriodEmploymentStatus.EMPLOYED_FULL_TIME: (
        "This person is employed by the specified employer full-time."
    ),
    StateEmploymentPeriodEmploymentStatus.EMPLOYED_PART_TIME: (
        "This person is employed by the specified employer part-time."
    ),
    StateEmploymentPeriodEmploymentStatus.STUDENT: (
        "This person is a student so they are not working. If this value is set, the "
        "`employer_name` and `job_title` fields should be null."
    ),
    StateEmploymentPeriodEmploymentStatus.UNABLE_TO_WORK: (
        "This person is unemployed and has no alternate source of income, but also has "
        "extenuating circumstances such that they are unable to maintain employment. "
        "If this value is set, the `employer_name` and `job_title` fields should be "
        "null."
    ),
    StateEmploymentPeriodEmploymentStatus.UNEMPLOYED: (
        "This person is unemployed has no alternate source of income, but does not "
        "have circumstances inhibiting their ability to work. If this value is set, "
        "the `employer_name` and `job_title` fields should be null."
    ),
}

_STATE_EMPLOYMENT_PERIOD_EMPLOYMENT_STATUS_MAP = {
    "ALTERNATE INCOME SOURCE": StateEmploymentPeriodEmploymentStatus.ALTERNATE_INCOME_SOURCE,
    "EMPLOYED FULL TIME": StateEmploymentPeriodEmploymentStatus.EMPLOYED_FULL_TIME,
    "EMPLOYED PART TIME": StateEmploymentPeriodEmploymentStatus.EMPLOYED_PART_TIME,
    "EMPLOYED": StateEmploymentPeriodEmploymentStatus.EMPLOYED,
    "EXTERNAL UNKNOWN": StateEmploymentPeriodEmploymentStatus.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateEmploymentPeriodEmploymentStatus.INTERNAL_UNKNOWN,
    "STUDENT": StateEmploymentPeriodEmploymentStatus.STUDENT,
    "UNABLE TO WORK": StateEmploymentPeriodEmploymentStatus.UNABLE_TO_WORK,
    "UNEMPLOYED": StateEmploymentPeriodEmploymentStatus.UNEMPLOYED,
}


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateEmploymentPeriodEndReason(StateEntityEnum):
    EMPLOYMENT_STATUS_CHANGE = (
        state_enum_strings.state_employment_period_end_reason_employment_status_change
    )
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown
    FIRED = state_enum_strings.state_employment_period_end_reason_fired
    INCARCERATED = state_enum_strings.state_employment_period_end_reason_incarcerated
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    LAID_OFF = state_enum_strings.state_employment_period_end_reason_laid_off
    MEDICAL = state_enum_strings.state_employment_period_end_reason_medical
    MOVED = state_enum_strings.state_employment_period_end_reason_moved
    NEW_JOB = state_enum_strings.state_employment_period_end_reason_new_job
    QUIT = state_enum_strings.state_employment_period_end_reason_quit
    RETIRED = state_enum_strings.state_employment_period_end_reason_retired

    @staticmethod
    def _get_default_map() -> Dict[str, "StateEmploymentPeriodEndReason"]:
        return _STATE_EMPLOYMENT_PERIOD_END_REASON_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "Denotes why a period of employment (or unemployment) ended."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_EMPLOYMENT_PERIOD_END_REASON_VALUE_DESCRIPTIONS


_STATE_EMPLOYMENT_PERIOD_END_REASON_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateEmploymentPeriodEndReason.EMPLOYMENT_STATUS_CHANGE: (
        "This person's employment status changed during a period where they don't have "
        "a job (e.g. transitions from `UNEMPLOYED` -> `STUDENT`)."
    ),
    StateEmploymentPeriodEndReason.FIRED: (
        "This person's employment was terminated due to unsatisfactory employee performance."
    ),
    StateEmploymentPeriodEndReason.INCARCERATED: (
        "This person's employment was terminated because they were incarcerated."
    ),
    StateEmploymentPeriodEndReason.LAID_OFF: (
        "This person’s job was terminated or suspended due to employer/business reasons."
    ),
    StateEmploymentPeriodEndReason.MEDICAL: (
        "This person's employment was terminated due to medical reasons."
    ),
    StateEmploymentPeriodEndReason.MOVED: (
        "This person left their job because they moved and could no longer perform job "
        "responsibilities."
    ),
    StateEmploymentPeriodEndReason.NEW_JOB: (
        "This person's period without a job has ended because they are going to start "
        "a new job."
    ),
    StateEmploymentPeriodEndReason.QUIT: (
        "This person left their job voluntarily, without the intention of retiring. "
        "This may happen because this person will be starting a new job or any other "
        "personal reason not covered by other `StateEmploymentPeriodEndReason` values."
    ),
    StateEmploymentPeriodEndReason.RETIRED: (
        "This person left their job due to retirement."
    ),
}

_STATE_EMPLOYMENT_PERIOD_END_REASON_MAP = {
    "EMPLOYMENT STATUS CHANGE": StateEmploymentPeriodEndReason.EMPLOYMENT_STATUS_CHANGE,
    "EXTERNAL UNKNOWN": StateEmploymentPeriodEndReason.EXTERNAL_UNKNOWN,
    "FIRED": StateEmploymentPeriodEndReason.FIRED,
    "INCARCERATED": StateEmploymentPeriodEndReason.INCARCERATED,
    "INTERNAL UNKNOWN": StateEmploymentPeriodEndReason.INTERNAL_UNKNOWN,
    "LAID OFF": StateEmploymentPeriodEndReason.LAID_OFF,
    "MEDICAL": StateEmploymentPeriodEndReason.MEDICAL,
    "MOVED": StateEmploymentPeriodEndReason.MOVED,
    "NEW JOB": StateEmploymentPeriodEndReason.NEW_JOB,
    "QUIT": StateEmploymentPeriodEndReason.QUIT,
    "RETIRED": StateEmploymentPeriodEndReason.RETIRED,
}
