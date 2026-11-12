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
from typing import Any, Optional, Tuple

from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate

COLLECTION_BY_FILENAME = {
    "us_ix_supervision_tasks_record_v2.json": "US_ID-supervisionTasksV2",
    "us_mo_supervision_tasks_record.json": "US_MO-supervisionTasks",
    "us_nd_supervision_tasks_record.json": "US_ND-supervisionTasks",
    "us_ne_supervision_tasks_record.json": "US_NE-supervisionTasks",
    "us_tx_supervision_tasks_record.json": "US_TX-supervisionTasks",
    "us_co_supervision_tasks_record.json": "US_CO-supervisionTasks",
}


class WorkflowsTasksETLDelegate(WorkflowsFirestoreETLDelegate):
    """Generic delegate for loading Workflows' tasks records into Firestore."""

    @classmethod
    def supports_file(cls, state_code: StateCode, filename: str) -> bool:
        return filename in COLLECTION_BY_FILENAME

    def __init__(self, state_code: StateCode, filename: str):
        super().__init__(state_code, filename)
        self.collection_name = COLLECTION_BY_FILENAME[filename]

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

    def transform_row(
        self, row: dict[str, Any], filename: str
    ) -> Tuple[Optional[str], Optional[dict]]:
        new_document: dict[str, Any] = {
            "externalId": row["person_external_id"],
            "officerId": row.get("officer_id"),
            "stateCode": self.format_state_code(row["state_code"]),
            "tasks": [
                convert_nested_dictionary_keys(task, snake_to_camel)
                for task in row["tasks"]
            ],
        }

        return row["person_external_id"], new_document
