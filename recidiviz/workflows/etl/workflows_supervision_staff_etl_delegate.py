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
"""Delegate class to ETL supervision staff records for Workflows into Firestore."""
import json
from typing import List, Tuple

from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.str_field_utils import person_name_case, snake_to_camel
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


class WorkflowsSupervisionStaffETLDelegate(WorkflowsFirestoreETLDelegate):
    """Delegate class to ETL the supervision_staff_record.json file into Firestore."""

    COLLECTION_BY_FILENAME = {"supervision_staff_record.json": "supervisionStaff"}

    def get_supported_files(self) -> List[str]:
        return ["supervision_staff_record.json"]

    def transform_row(self, row: str) -> Tuple[str, dict]:
        data = json.loads(row)

        # Convert all keys to camelcase
        new_document = convert_nested_dictionary_keys(data, snake_to_camel)

        new_document["givenNames"] = person_name_case(data.get("given_names", ""))
        new_document["surname"] = person_name_case(data.get("surname", ""))
        if new_document.get("email"):
            new_document["email"] = data.get("email").lower()

        return data["id"], new_document
