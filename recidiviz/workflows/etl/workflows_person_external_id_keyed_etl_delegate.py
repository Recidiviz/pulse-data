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
"""Abstract delegate class to ETL records exported alongside the resident record
into Firestore, one document per resident, under the same document id as that
resident's document in the residents collection."""

import json

from recidiviz.calculator.query.state.views.workflows.firestore.resident_record import (
    PERSON_EXTERNAL_ID_COLUMN_NAME,
)
from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


class WorkflowsPersonExternalIdKeyedETLDelegate(WorkflowsFirestoreETLDelegate):
    """Abstract delegate class to ETL records exported alongside the resident record
    into Firestore, one document per resident, under the same document id as that
    resident's document in the residents collection.

    For example, transform_row turns the row
        {"state_code": "US_IX", "person_external_id": "1234567",
         "pseudonymized_id": "a1b2c3", "move_to_nici_is_eligible": true}
    into the row id 1234567, which doc_id_for_row_id prefixes with the state code,
    and the document
        {"stateCode": "US_IX", "pseudonymizedId": "a1b2c3",
         "moveToNiciIsEligible": true}
    """

    def transform_row(self, row: str, filename: str) -> tuple[str, dict]:
        data = json.loads(row)
        person_external_id = data.pop(PERSON_EXTERNAL_ID_COLUMN_NAME)
        return person_external_id, convert_nested_dictionary_keys(data, snake_to_camel)
