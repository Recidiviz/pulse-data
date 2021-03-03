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
"""Defines the types needed for modeling CaseUpdates."""
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, Optional

import attr
import dateutil.parser


class CaseUpdateActionType(Enum):
    COMPLETED_ASSESSMENT = "COMPLETED_ASSESSMENT"
    DISCHARGE_INITIATED = "DISCHARGE_INITIATED"
    DOWNGRADE_INITIATED = "DOWNGRADE_INITIATED"
    FOUND_EMPLOYMENT = "FOUND_EMPLOYMENT"
    SCHEDULED_FACE_TO_FACE = "SCHEDULED_FACE_TO_FACE"

    INFORMATION_DOESNT_MATCH_OMS = "INFORMATION_DOESNT_MATCH_OMS"
    NOT_ON_CASELOAD = "NOT_ON_CASELOAD"
    FILED_REVOCATION_OR_VIOLATION = "FILED_REVOCATION_OR_VIOLATION"
    OTHER_DISMISSAL = "OTHER_DISMISSAL"


class CaseUpdateMetadataKeys:
    ACTION = "action"
    ACTION_TIMESTAMP = "action_ts"
    LAST_RECORDED_DATE = "last_recorded_date"
    LAST_SUPERVISION_LEVEL = "last_supervision_level"


@attr.s(auto_attribs=True)
class CaseUpdateAction:
    """Implements wrapper for actions stored in a CaseUpdate."""

    action_type: CaseUpdateActionType
    action_ts: datetime

    last_recorded_date: Optional[date]
    last_supervision_level: Optional[str]

    @staticmethod
    def from_json(action_dict: Dict[str, Any]) -> "CaseUpdateAction":
        action_type = CaseUpdateActionType(action_dict[CaseUpdateMetadataKeys.ACTION])
        action_ts = dateutil.parser.parse(
            action_dict[CaseUpdateMetadataKeys.ACTION_TIMESTAMP]
        )

        last_recorded_date = None
        if CaseUpdateMetadataKeys.LAST_RECORDED_DATE in action_dict:
            last_recorded_date = dateutil.parser.parse(
                action_dict[CaseUpdateMetadataKeys.LAST_RECORDED_DATE]
            ).date()

        last_supervision_level = None
        if CaseUpdateMetadataKeys.LAST_SUPERVISION_LEVEL in action_dict:
            last_supervision_level = action_dict[
                CaseUpdateMetadataKeys.LAST_SUPERVISION_LEVEL
            ]

        return CaseUpdateAction(
            action_type=action_type,
            action_ts=action_ts,
            last_recorded_date=last_recorded_date,
            last_supervision_level=last_supervision_level,
        )

    def to_json(self) -> Dict[str, str]:
        base_json = {
            CaseUpdateMetadataKeys.ACTION: self.action_type.value,
            CaseUpdateMetadataKeys.ACTION_TIMESTAMP: str(self.action_ts),
        }

        if self.last_recorded_date is not None:
            base_json[CaseUpdateMetadataKeys.LAST_RECORDED_DATE] = str(
                self.last_recorded_date
            )
        if self.last_supervision_level is not None:
            base_json[
                CaseUpdateMetadataKeys.LAST_SUPERVISION_LEVEL
            ] = self.last_supervision_level

        return base_json
