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
from typing import Any, List, Tuple

from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.str_field_utils import person_name_case, snake_to_camel
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


class WorkflowsResidentETLDelegate(WorkflowsFirestoreETLDelegate):
    """Delegate class to ETL the resident_record.json file into Firestore."""

    COLLECTION_BY_FILENAME = {"resident_record.json": "residents"}

    PSEUDONYMIZED_FIELDS = {
        "person_name": {
            "given_names": "first_name",
            "middle_names": "first_name",
            "surname": "last_name",
        }
    }

    def get_supported_files(self) -> List[str]:
        return ["resident_record.json"]

    def transform_row(self, row: dict[str, Any], filename: str) -> Tuple[str, dict]:
        metadata = convert_nested_dictionary_keys(
            json.loads(row.get("metadata", "{}")), snake_to_camel
        )

        new_document = {
            "pseudonymizedId": row["pseudonymized_id"],
            "personExternalId": row["person_external_id"],
            "displayId": row["display_id"],
            "stateCode": row["state_code"],
            "gender": row.get("gender"),
            "personName": {
                snake_to_camel(k): person_name_case(v)
                for k, v in json.loads(row["person_name"]).items()
            },
            "officerId": row.get("officer_id"),
            "facilityId": row.get("facility_id"),
            "unitId": row.get("unit_id"),
            "facilityUnitId": row.get("facility_unit_id"),
            "custodyLevel": row.get("custody_level"),
            "admissionDate": row.get("admission_date"),
            "releaseDate": row.get("release_date"),
            "allEligibleOpportunities": row.get("all_eligible_opportunities"),
            "allIneligibleOpportunities": row.get("all_ineligible_opportunities"),
            "metadata": metadata,
            "portionServedNeeded": row.get("portion_served_needed"),
            "usMePortionNeededEligibleDate": row.get(
                "us_me_portion_needed_eligible_date"
            ),
            "sccpEligibilityDate": row.get("sccp_eligibility_date"),
            "usTnFacilityAdmissionDate": row.get("us_tn_facility_admission_date"),
        }

        return row["person_external_id"], new_document
