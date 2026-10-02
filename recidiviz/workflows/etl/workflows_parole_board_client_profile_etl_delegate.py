# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Delegate class to ETL Parole Board client profiles into Firestore."""
import json

from recidiviz.calculator.query.state.views.workflows.firestore.parole_board_client_profile_record import (
    PAROLE_BOARD_CLIENT_PROFILE_RECORD_VIEW_NAME,
    PERSON_EXTERNAL_ID_COLUMN_NAME,
)
from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate

PAROLE_BOARD_CLIENT_PROFILE_RECORD_FILENAME = (
    f"{PAROLE_BOARD_CLIENT_PROFILE_RECORD_VIEW_NAME}.json"
)

# The dashboards Firestore rules restrict reads of this collection to staff with
# Parole Board access, so renaming it means updating those rules in the same release.
PAROLE_BOARD_CLIENT_PROFILES_COLLECTION_NAME = "paroleBoardClientProfiles"


class WorkflowsParoleBoardClientProfileETLDelegate(WorkflowsFirestoreETLDelegate):
    """Delegate class to ETL the parole_board_client_profile_record.json file into
    Firestore, one document per resident, under the same document id as that
    resident's document in the residents collection.

    For example, the row
        {"state_code": "US_ID", "person_external_id": "1234567",
         "pseudonymized_id": "a1b2c3", "parole_hearings": [{"hearing_status": "SCHEDULED"}]}
    becomes the document us_id_1234567:
        {"stateCode": "US_ID", "pseudonymizedId": "a1b2c3",
         "paroleHearings": [{"hearingStatus": "SCHEDULED"}]}
    """

    COLLECTION_BY_FILENAME = {
        PAROLE_BOARD_CLIENT_PROFILE_RECORD_FILENAME: PAROLE_BOARD_CLIENT_PROFILES_COLLECTION_NAME
    }

    def get_supported_files(self) -> list[str]:
        return [PAROLE_BOARD_CLIENT_PROFILE_RECORD_FILENAME]

    def transform_row(self, row: str, filename: str) -> tuple[str, dict]:
        data = json.loads(row)
        person_external_id = data.pop(PERSON_EXTERNAL_ID_COLUMN_NAME)
        return person_external_id, convert_nested_dictionary_keys(data, snake_to_camel)
