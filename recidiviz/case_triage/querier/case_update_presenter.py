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
""" Contains presentation logic for case updates """
import enum
from typing import Dict

from recidiviz.case_triage.case_updates.progress_checker import (
    check_case_update_action_progress,
)
from recidiviz.case_triage.case_updates.types import (
    LastVersionData,
    CaseUpdateActionType,
)
from recidiviz.case_triage.querier.utils import _json_map_dates_to_strings
from recidiviz.persistence.database.schema.case_triage.schema import (
    CaseUpdate,
    ETLClient,
)


class CaseUpdateStatus(enum.Enum):
    """ CaseUpdate status """

    IN_PROGRESS = "IN_PROGRESS"
    UPDATED_IN_CIS = "UPDATED_IN_CIS"


class CaseUpdatePresenter:
    """ Presenter for a CaseUpdate """

    def __init__(self, etl_client: ETLClient, case_update: CaseUpdate):
        self.etl_client = etl_client
        self.case_update = case_update

    def to_json(self) -> Dict[str, str]:
        return _json_map_dates_to_strings(
            {
                "updateId": self.case_update.update_id,
                "actionType": self.case_update.action_type,
                "actionTs": self.case_update.action_ts,
                "status": CaseUpdateStatus.IN_PROGRESS.value
                if self.is_in_progress()
                else CaseUpdateStatus.UPDATED_IN_CIS.value,
                "comment": self.case_update.comment,
            }
        )

    def is_in_progress(self) -> bool:
        return check_case_update_action_progress(
            CaseUpdateActionType(self.case_update.action_type),
            self.etl_client,
            LastVersionData.from_json(self.case_update.last_version),
        )
