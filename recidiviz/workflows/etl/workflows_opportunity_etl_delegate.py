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
"""Delegate class to ETL opportunity referral records for workflows into Firestore."""
import json
import re
from typing import Any, List, Optional, Tuple

from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


class WorkflowsOpportunityETLDelegate(WorkflowsFirestoreETLDelegate):
    """Generic delegate for loading Workflows' opportunity records into Firestore."""

    EXPORT_BY_STATE = {
        "US_ND": ["us_nd_complete_discharge_early_from_supervision_record.json"]
    }
    COLLECTION_BY_FILENAME = {
        "us_nd_complete_discharge_early_from_supervision_record.json": "earlyTerminationReferrals"
    }

    def get_supported_files(self, state_code: str) -> List[str]:
        return self.EXPORT_BY_STATE.get(state_code, [])

    @property
    def timestamp_key(self) -> str:
        """Name of the key this delegate will insert into each document to record when it was loaded."""
        return "__loadedAt"

    def filepath_url(self, state_code: str, filename: str) -> str:
        return f"gs://{self.get_filepath(state_code, filename).abs_path()}"

    @staticmethod
    def build_document(row: dict[str, Any]) -> dict:
        new_document: dict[str, Any] = {"formInformation": {}, "metadata": {}}
        # Rename form_information and metadata fields
        for key, value in row.items():
            if key.startswith("form_information_"):
                formatted_key = re.sub(r"form_information_", "", key)
                new_document["formInformation"][formatted_key] = value
            elif key.startswith("metadata_"):
                formatted_key = re.sub(r"metadata_", "", key)
                new_document["metadata"][formatted_key] = value
            else:
                new_document[key] = value

        # Convert all keys to camelcase
        new_document = convert_nested_dictionary_keys(new_document, snake_to_camel)
        return new_document

    def transform_row(self, row: str) -> Tuple[Optional[str], Optional[dict]]:
        data = json.loads(row)
        return data["external_id"], self.build_document(data)
