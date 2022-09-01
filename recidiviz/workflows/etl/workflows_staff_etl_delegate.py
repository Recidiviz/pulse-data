#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
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
"""Delegate class to ETL staff records for Workflows into Firestore."""
import json
from typing import Dict, List, Tuple

from recidiviz.common.str_field_utils import person_name_case
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


class WorkflowsStaffETLDelegate(WorkflowsFirestoreETLDelegate):
    """Delegate class to ETL the staff_record.json file into Firestore."""

    @property
    def COLLECTION_BY_FILENAME(self) -> Dict[str, str]:
        return {"staff_record.json": "staff"}

    def get_supported_files(self, state_code: str) -> List[str]:
        return ["staff_record.json"]

    def transform_row(self, row: str) -> Tuple[str, dict]:
        data = json.loads(row)

        new_document = {
            "id": data["id"],
            "stateCode": data["state_code"],
            "name": person_name_case(data["name"]),
            "email": data.get("email"),
            "hasCaseload": data["has_caseload"],
            "district": data.get("district"),
        }

        return data["id"], new_document
