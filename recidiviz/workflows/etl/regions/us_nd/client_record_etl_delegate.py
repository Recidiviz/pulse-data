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
from typing import Optional, Tuple

from recidiviz.common.str_field_utils import person_name_case, snake_to_camel
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


class ClientRecordETLDelegate(WorkflowsFirestoreETLDelegate):
    """Delegate class to ETL the client_record.json file into Firestore."""

    STATE_CODE = "US_ND"
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
            "supervisionType": data["supervision_type"],
        }

        # add nullable fields
        if "phone_number" in data:
            new_document["phoneNumber"] = data["phone_number"]

        if "address" in data:
            # incoming strings may have cased state abbreviation wrong
            new_document["address"] = data["address"]

        if "supervision_level" in data:
            new_document["supervisionLevel"] = data["supervision_level"]

        if "early_termination_eligible" in data:
            new_document["earlyTerminationEligible"] = data[
                "early_termination_eligible"
            ]

        # Note that date fields such as these are preserved as ISO strings (i.e., "YYYY-MM-DD")
        # rather than datetimes to avoid time-zone discrepancies
        if "supervision_level_start" in data:
            new_document["supervisionLevelStart"] = data["supervision_level_start"]

        if "expiration_date" in data:
            new_document["expirationDate"] = data["expiration_date"]

        if "supervision_start_date" in data:
            new_document["supervisionStartDate"] = data["supervision_start_date"]

        return data["person_external_id"], new_document


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        ClientRecordETLDelegate().run_etl()
