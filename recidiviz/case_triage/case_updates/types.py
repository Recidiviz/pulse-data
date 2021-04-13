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
from datetime import date
from enum import Enum
from typing import Dict, Optional

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
    LAST_RECORDED_DATE = "last_recorded_date"
    LAST_SUPERVISION_LEVEL = "last_supervision_level"


@attr.s
class LastVersionData:
    last_recorded_date: Optional[date] = attr.ib(default=None)
    last_supervision_level: Optional[str] = attr.ib(default=None)

    def to_json(self) -> Dict[str, str]:
        base = {}
        if self.last_recorded_date is not None:
            base[
                CaseUpdateMetadataKeys.LAST_RECORDED_DATE
            ] = self.last_recorded_date.isoformat()
        if self.last_supervision_level is not None:
            base[
                CaseUpdateMetadataKeys.LAST_SUPERVISION_LEVEL
            ] = self.last_supervision_level
        return base

    @staticmethod
    def from_json(json_dict: Dict[str, str]) -> "LastVersionData":
        last_recorded_date = None
        if CaseUpdateMetadataKeys.LAST_RECORDED_DATE in json_dict:
            last_recorded_date = dateutil.parser.parse(
                json_dict[CaseUpdateMetadataKeys.LAST_RECORDED_DATE]
            ).date()
        last_supervision_level = json_dict.get(
            CaseUpdateMetadataKeys.LAST_SUPERVISION_LEVEL
        )

        return LastVersionData(
            last_recorded_date=last_recorded_date,
            last_supervision_level=last_supervision_level,
        )
