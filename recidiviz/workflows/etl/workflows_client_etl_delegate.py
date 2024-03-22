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
from typing import Any, List, Tuple

from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.str_field_utils import person_name_case, snake_to_camel
from recidiviz.workflows.etl.state_specific_etl_transformations import (
    state_specific_client_address_transformation,
    state_specific_supervision_type_transformation,
)
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


class WorkflowsClientETLDelegate(WorkflowsFirestoreETLDelegate):
    """Delegate class to ETL the client_record.json file into Firestore."""

    COLLECTION_BY_FILENAME = {"client_record.json": "clients"}

    def get_supported_files(self) -> List[str]:
        return ["client_record.json"]

    def transform_row(self, row: str) -> Tuple[str, dict]:
        data = json.loads(row)
        new_document: dict[str, Any] = {}

        state_code = data.get("state_code")

        for key, value in data.items():
            match key:
                case "person_name":
                    new_document[key] = {
                        snake_to_camel(k): person_name_case(v)
                        for k, v in json.loads(value).items()
                    }
                case "supervision_type":
                    new_document[key] = state_specific_supervision_type_transformation(
                        state_code, value
                    )
                case "address":
                    new_document[key] = state_specific_client_address_transformation(
                        state_code, value
                    )
                case _:
                    new_document[key] = value

        # Convert all keys to camelcase
        new_document = convert_nested_dictionary_keys(new_document, snake_to_camel)

        return (
            data.get("person_external_id"),
            new_document,
        )
