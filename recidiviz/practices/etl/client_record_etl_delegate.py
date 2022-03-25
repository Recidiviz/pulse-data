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
"""Delegate class to ETL client records for practices into Firestore."""
import json
import logging
from datetime import datetime
from typing import Tuple

from recidiviz.practices.etl.practices_etl_delegate import PracticesFirestoreETLDelegate
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


class ClientRecordETLDelegate(PracticesFirestoreETLDelegate):
    """Delegate class to ETL the client_record.json file into Firestore."""

    EXPORT_FILENAME = "client_record.json"
    COLLECTION_NAME = "clients"

    def transform_row(self, row: str) -> Tuple[str, dict]:
        data = json.loads(row)

        new_document = {
            "personExternalId": data["person_external_id"],
            "stateCode": data["state_code"],
            "personName": json.loads(data["person_name"]),
            "officerId": data["officer_id"],
            "supervisionType": data["supervision_type"],
            "supervisionLevel": data.get("supervision_level"),
        }

        if "supervision_level_start" in data:
            new_document["supervisionLevelStart"] = datetime.fromisoformat(
                data["supervision_level_start"]
            )

        if data["all_eligible_and_discretion"]:
            new_document["compliantReportingEligible"] = {
                "offenseType": data.get("offense_type"),
                "judicialDistrict": data.get("judicial_district"),
                "lastDrugNegative": [
                    datetime.fromisoformat(date) for date in data["last_drug_negative"]
                ],
                "lastSanction": data.get("last_sanction"),
            }

        return data["person_external_id"], new_document


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        ClientRecordETLDelegate().run_etl()
