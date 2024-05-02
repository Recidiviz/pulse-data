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
"""Delegate class to ETL resident records for Workflows into Firestore."""
import json
from typing import List, Tuple

from recidiviz.common.str_field_utils import person_name_case, snake_to_camel
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


class WorkflowsResidentETLDelegate(WorkflowsFirestoreETLDelegate):
    """Delegate class to ETL the resident_record.json file into Firestore."""

    COLLECTION_BY_FILENAME = {"resident_record.json": "residents"}

    def get_supported_files(self) -> List[str]:
        return ["resident_record.json"]

    def transform_row(self, row: str) -> Tuple[str, dict]:
        data = json.loads(row)

        new_document = {
            "pseudonymizedId": data["pseudonymized_id"],
            "personExternalId": data["person_external_id"],
            "displayId": data["display_id"],
            "stateCode": data["state_code"],
            "gender": data["gender"],
            "personName": {
                snake_to_camel(k): person_name_case(v)
                for k, v in json.loads(data["person_name"]).items()
            },
            "officerId": data["officer_id"],
            "facilityId": data.get("facility_id"),
            "unitId": data.get("unit_id"),
            "facilityUnitId": data.get("facility_unit_id"),
            "custodyLevel": data.get("custody_level"),
            "admissionDate": data.get("admission_date"),
            "releaseDate": data.get("release_date"),
            "portionServedNeeded": data.get("portion_served_needed"),
            "sccpEligibilityDate": data.get("sccp_eligibility_date"),
            "usTnFacilityAdmissionDate": data.get("us_tn_facility_admission_date"),
        }

        if "all_eligible_opportunities" in data:
            new_document["allEligibleOpportunities"] = data[
                "all_eligible_opportunities"
            ]

        return data["person_external_id"], new_document
