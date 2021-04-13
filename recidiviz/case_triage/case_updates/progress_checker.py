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

from recidiviz.case_triage.case_updates.types import (
    CaseUpdateActionType,
    LastVersionData,
)
from recidiviz.persistence.database.schema.case_triage.schema import ETLClient


def _always_in_progress(_client: ETLClient, _last_version: LastVersionData) -> bool:
    return True


def _completed_assessment_progress_checker(
    client: ETLClient, last_version: LastVersionData
) -> bool:
    if not client.most_recent_assessment_date:
        return True
    if not last_version.last_recorded_date:
        return False
    return last_version.last_recorded_date >= client.most_recent_assessment_date


def _discharge_initiated_progress_checker(
    _client: ETLClient, _last_version: LastVersionData
) -> bool:
    # TODO(#5721): Need to better understand how to detect when DISCHARGE_INITIATED is no longer in-progress.
    return True


def _downgrade_initiated_progress_checker(
    client: ETLClient, last_version: LastVersionData
) -> bool:
    return client.supervision_level == last_version.last_supervision_level


def _found_employment_progress_checker(
    client: ETLClient, _last_version: LastVersionData
) -> bool:
    return not client.employer or client.employer.upper() == "UNEMPLOYED"


def _scheduled_face_to_face_progress_checker(
    client: ETLClient, last_version: LastVersionData
) -> bool:
    if not client.most_recent_face_to_face_date:
        return True
    if last_version.last_recorded_date is None:
        return False
    return last_version.last_recorded_date >= client.most_recent_face_to_face_date


_CASE_UPDATE_ACTION_TYPE_TO_PROGRESS_CHECKER: Dict[
    CaseUpdateActionType, Callable[[ETLClient, LastVersionData], bool]
] = {
    CaseUpdateActionType.COMPLETED_ASSESSMENT: _completed_assessment_progress_checker,
    CaseUpdateActionType.DISCHARGE_INITIATED: _discharge_initiated_progress_checker,
    CaseUpdateActionType.DOWNGRADE_INITIATED: _downgrade_initiated_progress_checker,
    CaseUpdateActionType.FOUND_EMPLOYMENT: _found_employment_progress_checker,
    CaseUpdateActionType.SCHEDULED_FACE_TO_FACE: _scheduled_face_to_face_progress_checker,
    CaseUpdateActionType.INFORMATION_DOESNT_MATCH_OMS: _always_in_progress,
    CaseUpdateActionType.NOT_ON_CASELOAD: _always_in_progress,
    CaseUpdateActionType.FILED_REVOCATION_OR_VIOLATION: _always_in_progress,
    CaseUpdateActionType.OTHER_DISMISSAL: _always_in_progress,
}


def check_case_update_action_progress(
    action_type: CaseUpdateActionType, client: ETLClient, last_version: LastVersionData
) -> bool:
    progress_checker = _CASE_UPDATE_ACTION_TYPE_TO_PROGRESS_CHECKER[action_type]
    return progress_checker(client, last_version)
