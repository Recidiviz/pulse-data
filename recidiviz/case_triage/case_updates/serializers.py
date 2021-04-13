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
"""Implements common serializers for different CaseUpdate subtypes."""
from typing import Callable, Dict

from recidiviz.case_triage.case_updates.types import (
    CaseUpdateActionType,
    LastVersionData,
)
from recidiviz.persistence.database.schema.case_triage.schema import ETLClient


def _default_user_initiated_action_serializer(_client: ETLClient) -> LastVersionData:
    return LastVersionData()


def _completed_assessment_serializer(client: ETLClient) -> LastVersionData:
    return LastVersionData(last_recorded_date=client.most_recent_assessment_date)


def _discharge_initiated_serializer(_client: ETLClient) -> LastVersionData:
    # TODO(#5721): Figure out what additional metadata is needed
    return LastVersionData()


def _downgrade_initiated_serializer(client: ETLClient) -> LastVersionData:
    return LastVersionData(last_supervision_level=client.supervision_level)


def _scheduled_face_to_face_serializer(client: ETLClient) -> LastVersionData:
    return LastVersionData(last_recorded_date=client.most_recent_face_to_face_date)


_USER_INITIATED_ACTION_TO_SERIALIZER: Dict[
    CaseUpdateActionType,
    Callable[[ETLClient], LastVersionData],
] = {
    CaseUpdateActionType.COMPLETED_ASSESSMENT: _completed_assessment_serializer,
    CaseUpdateActionType.DISCHARGE_INITIATED: _discharge_initiated_serializer,
    CaseUpdateActionType.DOWNGRADE_INITIATED: _downgrade_initiated_serializer,
    CaseUpdateActionType.FOUND_EMPLOYMENT: _default_user_initiated_action_serializer,
    CaseUpdateActionType.SCHEDULED_FACE_TO_FACE: _scheduled_face_to_face_serializer,
    CaseUpdateActionType.INFORMATION_DOESNT_MATCH_OMS: _default_user_initiated_action_serializer,
    CaseUpdateActionType.NOT_ON_CASELOAD: _default_user_initiated_action_serializer,
    CaseUpdateActionType.FILED_REVOCATION_OR_VIOLATION: _default_user_initiated_action_serializer,
    CaseUpdateActionType.OTHER_DISMISSAL: _default_user_initiated_action_serializer,
}


def serialize_last_version_info(
    action: CaseUpdateActionType, client: ETLClient
) -> LastVersionData:
    serializer = _USER_INITIATED_ACTION_TO_SERIALIZER[action]
    return serializer(client)
