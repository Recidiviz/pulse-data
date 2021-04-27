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
"""Provides framework for evaluating whether a CaseUpdateActionType is still in-progress for a client."""
from typing import Callable, Dict

import attr

from recidiviz.case_triage.case_updates.types import (
    CaseUpdateActionType,
    CaseActionVersionData,
)


def _in_progress_until_changed(
    current_version: CaseActionVersionData, last_version: CaseActionVersionData
) -> bool:
    return attr.asdict(current_version) == attr.asdict(last_version)


def _always_in_progress(
    _current_version: CaseActionVersionData, _last_version: CaseActionVersionData
) -> bool:
    return True


def _in_progress_until_more_recent_date(
    current_version: CaseActionVersionData, last_version: CaseActionVersionData
) -> bool:
    # In progress if there is no current recorded date
    # or the last recorded date is as if not more recent than the current date
    return current_version.last_recorded_date is None or (
        last_version.last_recorded_date is not None
        and last_version.last_recorded_date >= current_version.last_recorded_date
    )


def _found_employment_progress_checker(
    current_version: CaseActionVersionData, _last_version: CaseActionVersionData
) -> bool:
    return (
        not current_version.last_employer
        or current_version.last_employer.upper() == "UNEMPLOYED"
    )


_CASE_UPDATE_ACTION_TYPE_TO_PROGRESS_CHECKER: Dict[
    CaseUpdateActionType, Callable[[CaseActionVersionData, CaseActionVersionData], bool]
] = {
    # Assessment progress checkers
    CaseUpdateActionType.COMPLETED_ASSESSMENT: _in_progress_until_more_recent_date,
    CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA: _in_progress_until_changed,
    # Employment progress checkers
    CaseUpdateActionType.FOUND_EMPLOYMENT: _found_employment_progress_checker,
    CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA: _in_progress_until_changed,
    # Face to face contact progress checkers
    CaseUpdateActionType.SCHEDULED_FACE_TO_FACE: _in_progress_until_more_recent_date,
    CaseUpdateActionType.INCORRECT_CONTACT_DATA: _in_progress_until_changed,
    # Supervision level progress checkers
    CaseUpdateActionType.DOWNGRADE_INITIATED: _in_progress_until_changed,
    # TODO(#5721): Need to better understand how to detect when DISCHARGE_INITIATED is no longer in-progress.
    CaseUpdateActionType.DISCHARGE_INITIATED: _always_in_progress,
    CaseUpdateActionType.NOT_ON_CASELOAD: _always_in_progress,
    CaseUpdateActionType.CURRENTLY_IN_CUSTODY: _always_in_progress,
    CaseUpdateActionType.DEPRECATED__INFORMATION_DOESNT_MATCH_OMS: _always_in_progress,
    CaseUpdateActionType.DEPRECATED__FILED_REVOCATION_OR_VIOLATION: _always_in_progress,
    CaseUpdateActionType.DEPRECATED__OTHER_DISMISSAL: _always_in_progress,
}


def check_case_update_action_progress(
    action_type: CaseUpdateActionType,
    *,
    last_version: CaseActionVersionData,
    current_version: CaseActionVersionData,
) -> bool:
    progress_checker = _CASE_UPDATE_ACTION_TYPE_TO_PROGRESS_CHECKER[action_type]
    return progress_checker(current_version, last_version)
