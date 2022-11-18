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
"""Delegate class to ETL client records for Workflows into Firestore."""
import json
from typing import List, Tuple

from recidiviz.common.str_field_utils import person_name_case, snake_to_camel
from recidiviz.workflows.etl.state_specific_etl_transformations import (
    state_specific_client_address_transformation,
    state_specific_supervision_type_transformation,
)
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


class WorkflowsClientETLDelegate(WorkflowsFirestoreETLDelegate):
    """Delegate class to ETL the client_record.json file into Firestore."""

    COLLECTION_BY_FILENAME = {"client_record.json": "clients"}

    def get_supported_files(self, state_code: str) -> List[str]:
        return ["client_record.json"]

    def transform_row(self, row: str) -> Tuple[str, dict]:
        data = json.loads(row)

        # First fill the non-nullable fields
        new_document = {
            "pseudonymizedId": data["pseudonymized_id"],
            "personExternalId": data["person_external_id"],
            "stateCode": data["state_code"],
            "personName": {
                snake_to_camel(k): person_name_case(v)
                for k, v in json.loads(data["person_name"]).items()
            },
            "officerId": data["officer_id"],
            "supervisionType": state_specific_supervision_type_transformation(
                data["state_code"], data["supervision_type"]
            ),
        }

        # add nullable fields
        if "supervision_level" in data:
            new_document["supervisionLevel"] = data["supervision_level"]

        # Note that date fields such as these are preserved as ISO strings (i.e., "YYYY-MM-DD")
        # rather than datetimes to avoid time-zone discrepancies
        if "supervision_level_start" in data:
            new_document["supervisionLevelStart"] = data["supervision_level_start"]

        if "address" in data:
            new_document["address"] = state_specific_client_address_transformation(
                data["state_code"], data["address"]
            )

        if "phone_number" in data:
            new_document["phoneNumber"] = data["phone_number"]

        if "supervision_start_date" in data:
            new_document["supervisionStartDate"] = data["supervision_start_date"]

        if "expiration_date" in data:
            new_document["expirationDate"] = data["expiration_date"]

        if "current_balance" in data:
            new_document["currentBalance"] = data["current_balance"]

        if "last_payment_amount" in data:
            new_document["lastPaymentAmount"] = data["last_payment_amount"]

        if "last_payment_date" in data:
            new_document["lastPaymentDate"] = data["last_payment_date"]

        if "special_conditions" in data:
            new_document["specialConditions"] = data["special_conditions"]

        if "board_conditions" in data and data["board_conditions"]:
            new_document["boardConditions"] = [
                {
                    "condition": condition["condition"],
                    "conditionDescription": condition["condition_description"],
                }
                for condition in data["board_conditions"]
            ]
        if "district" in data:
            new_document["district"] = data["district"]

        if "all_eligible_opportunities" in data:
            new_document["allEligibleOpportunities"] = data[
                "all_eligible_opportunities"
            ]

        return data["person_external_id"], new_document
