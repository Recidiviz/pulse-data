#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Delegate class to ETL Supervision Tasks records for Workflows into Firestore."""
import json
from typing import Any, Dict, List, Optional, Tuple

from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate

COLLECTION_BY_FILENAME = {
    "us_ix_supervision_tasks_record.json": "US_ID-supervisionTasks",
    "us_tx_supervision_tasks_record.json": "US_TX-supervisionTasks",
    "us_nd_supervision_tasks_record.json": "US_ND-supervisionTasks",
    "us_ne_supervision_tasks_record.json": "US_NE-supervisionTasks",
}


class WorkflowsTasksETLDelegate(WorkflowsFirestoreETLDelegate):
    """Generic delegate for loading Workflows' tasks records into Firestore."""

    def __init__(self, state_code: StateCode):
        super().__init__(state_code)

    @property
    def COLLECTION_BY_FILENAME(self) -> Dict[str, str]:
        return COLLECTION_BY_FILENAME

    def get_supported_files(self) -> List[str]:
        return list(COLLECTION_BY_FILENAME.keys())

    @property
    def timestamp_key(self) -> str:
        """Name of the key this delegate will insert into each document to record when it was loaded."""
        return "__loadedAt"

    def filepath_url(self, filename: str) -> str:
        return f"gs://{self.get_filepath(filename).abs_path()}"

    def format_state_code(self, state_code: str) -> str:
        if state_code == "US_IX":
            return "US_ID"
        return state_code

    def transform_row(self, row: str) -> Tuple[Optional[str], Optional[dict]]:
        data = json.loads(row)

        new_document: dict[str, Any] = {
            "externalId": data["person_external_id"],
            "officerId": data.get("officer_id"),
            "stateCode": self.format_state_code(data["state_code"]),
            "tasks": [
                convert_nested_dictionary_keys(task, snake_to_camel)
                for task in data.get("tasks")
            ],
        }

        return data["person_external_id"], new_document
