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
"""Delegate class to ETL client records for workflows into Firestore."""
import json
import logging
import re
from typing import Optional, Tuple

from recidiviz.common.str_field_utils import person_name_case, snake_to_camel
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_etl_delegate import (
    WorkflowsSingleStateETLDelegate,
)

SUPERVISION_TYPE_MAPPING = {
    "DIVERSION": "Diversion",
    "TN PROBATIONER": "Probation",
    "TN PAROLEE": "Parole",
    "ISC FROM OTHER JURISDICTION": "ISC",
    "DETERMINATE RLSE PROBATIONER": "Determinate Release Probation",
    "SPCL ALT INCARCERATION UNIT": "SAIU",
    "MISDEMEANOR PROBATIONER": "Misdemeanor Probation",
}


class ClientRecordETLDelegate(WorkflowsSingleStateETLDelegate):
    """Delegate class to ETL the client_record.json file into Firestore."""

    STATE_CODE = "US_TN"
    EXPORT_FILENAME = "client_record.json"
    _COLLECTION_NAME_BASE = "clients"

    def transform_row(self, row: str) -> Tuple[Optional[str], Optional[dict]]:
        data = json.loads(row)

        # First fill the non-nullable fields
        new_document = {
            "personExternalId": data["person_external_id"],
            "pseudonymizedId": data["pseudonymized_id"],
            "stateCode": data["state_code"],
            "personName": {
                snake_to_camel(k): person_name_case(v)
                for k, v in json.loads(data["person_name"]).items()
            },
            "officerId": data["officer_id"],
            "currentBalance": data["current_balance"],
            "specialConditions": data["special_conditions"],
            "district": data["district"],
        }
        supervision_type = data["supervision_type"]
        new_document["supervisionType"] = SUPERVISION_TYPE_MAPPING.get(
            supervision_type, supervision_type
        )

        # add nullable fields
        if "phone_number" in data:
            new_document["phoneNumber"] = data["phone_number"]

        if "address" in data:
            # incoming strings may have cased state abbreviation wrong
            new_document["address"] = re.sub(
                r"\bTn\b", lambda m: m[0].upper(), data["address"]
            )

        if "last_payment_amount" in data:
            new_document["lastPaymentAmount"] = data["last_payment_amount"]

        if "supervision_level" in data:
            new_document["supervisionLevel"] = data["supervision_level"]

        # Note that date fields such as these are preserved as ISO strings (i.e., "YYYY-MM-DD")
        # rather than datetimes to avoid time-zone discrepancies
        if "supervision_level_start" in data:
            new_document["supervisionLevelStart"] = data["supervision_level_start"]

        if "last_payment_date" in data:
            new_document["lastPaymentDate"] = data["last_payment_date"]

        if "expiration_date" in data:
            new_document["expirationDate"] = data["expiration_date"]

        if "supervision_start_date" in data:
            new_document["supervisionStartDate"] = data["supervision_start_date"]

        # add nullable objects
        if "compliant_reporting_eligible" in data:
            new_document["compliantReportingEligible"] = True

        if data["board_conditions"]:
            new_document["boardConditions"] = [
                {
                    "condition": condition["condition"],
                    "conditionDescription": condition["condition_description"],
                }
                for condition in data["board_conditions"]
            ]

        return data["person_external_id"], new_document


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        ClientRecordETLDelegate().run_etl("US_TN", "client_record.json")
