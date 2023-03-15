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
"""Delegate class to ETL location records for Workflows into Firestore."""
import json
import re
from typing import List, Tuple

from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


class WorkflowsLocationETLDelegate(WorkflowsFirestoreETLDelegate):
    """Delegate class to ETL the location_record.json file into Firestore."""

    COLLECTION_BY_FILENAME = {"location_record.json": "locations"}

    def get_supported_files(self) -> List[str]:
        return ["location_record.json"]

    def transform_row(self, row: str) -> Tuple[str, dict]:
        data = json.loads(row)

        new_document = convert_nested_dictionary_keys(data, snake_to_camel)

        doc_id = (
            data["id"]
            # https://firebase.google.com/docs/firestore/quotas#collections_documents_and_fields
            # Document IDs cannot contain a forward slash
            .replace("/", "-")
            # Document IDs can contain spaces, but replacing them with underscores will look nicer
            # when browsing
            .replace(" ", "_")
            # Lowercase them for the same reason
            .lower()
        )
        # also replace any other special characters to look nicer when browsing
        doc_id = re.sub(r"[^a-z0-9_-]", "", doc_id)

        return doc_id, new_document
