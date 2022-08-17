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
"""Delegate class to ETL staff records for workflows into Firestore."""
import json
import logging
from typing import Tuple

from recidiviz.common.str_field_utils import person_name_case
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_etl_delegate import (
    WorkflowsSingleStateETLDelegate,
)


class StaffRecordETLDelegate(WorkflowsSingleStateETLDelegate):
    """Delegate class to ETL the staff_record.json file into Firestore."""

    STATE_CODE = "US_ND"
    EXPORT_FILENAME = "staff_record.json"
    _COLLECTION_NAME_BASE = "staff"

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


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        StaffRecordETLDelegate().run_etl("US_ND", "staff_record.json")
