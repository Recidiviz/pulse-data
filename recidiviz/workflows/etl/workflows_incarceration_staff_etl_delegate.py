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
"""Delegate class to ETL incarceration staff records for Workflows into Firestore."""
from typing import Any, Tuple

from recidiviz.common.str_field_utils import person_name_case
from recidiviz.workflows.etl.workflows_etl_delegate import (
    WorkflowsSingleFileETLDelegate,
)


class WorkflowsIncarcerationStaffETLDelegate(WorkflowsSingleFileETLDelegate):
    """Delegate class to ETL the incarceration_staff_record.json file into Firestore."""

    EXPECTED_FILENAME = "incarceration_staff_record.json"
    COLLECTION_NAME = "incarcerationStaff"

    def transform_row(self, row: dict[str, Any], filename: str) -> Tuple[str, dict]:
        if email := row.get("email"):
            email = email.lower()

        new_document = {
            "id": row["id"],
            "staffExternalId": row["staff_external_id"],
            "stateCode": row["state_code"],
            "email": email,
            "district": row.get("district"),
            "givenNames": person_name_case(row.get("given_names", "")),
            "surname": person_name_case(row.get("surname", "")),
            "roleSubtype": row.get("role_subtype"),
            "pseudonymizedId": row.get("pseudonymized_id"),
        }

        return row["id"], new_document
