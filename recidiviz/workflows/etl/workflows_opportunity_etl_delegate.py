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
from typing import Any, Dict, List, Optional, Tuple

import attr

from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


@attr.s
class OpportunityExportConfig:
    source_filename: str = attr.ib()
    export_collection_name: str = attr.ib()


CONFIG_BY_STATE: Dict[str, List[OpportunityExportConfig]] = {
    "US_ID": [
        OpportunityExportConfig(
            source_filename="us_id_complete_discharge_early_from_supervision_request_record.json",
            export_collection_name="US_ID-earnedDischargeReferrals",
        ),
        OpportunityExportConfig(
            source_filename="us_id_complete_transfer_to_limited_supervision_form_record.json",
            export_collection_name="US_ID-LSUReferrals",
        ),
        OpportunityExportConfig(
            source_filename="us_id_complete_full_term_discharge_from_supervision_request_record.json",
            export_collection_name="US_ID-pastFTRDReferrals",
        ),
    ],
    "US_ND": [
        OpportunityExportConfig(
            source_filename="us_nd_complete_discharge_early_from_supervision_record.json",
            export_collection_name="earlyTerminationReferrals",
        ),
    ],
    "US_TN": [
        OpportunityExportConfig(
            source_filename="us_tn_supervision_level_downgrade_record_materialized.json",
            export_collection_name="US_TN-supervisionLevelDowngrade",
        ),
    ],
}


class WorkflowsOpportunityETLDelegate(WorkflowsFirestoreETLDelegate):
    """Generic delegate for loading Workflows' opportunity records into Firestore."""

    @property
    def COLLECTION_BY_FILENAME(self) -> Dict[str, str]:
        return {
            config.source_filename: config.export_collection_name
            for config_list in CONFIG_BY_STATE.values()
            for config in config_list
        }

    def get_supported_files(self, state_code: str) -> List[str]:
        return [
            config.source_filename for config in CONFIG_BY_STATE.get(state_code, [])
        ]

    @property
    def timestamp_key(self) -> str:
        """Name of the key this delegate will insert into each document to record when it was loaded."""
        return "__loadedAt"

    def filepath_url(self, state_code: str, filename: str) -> str:
        return f"gs://{self.get_filepath(state_code, filename).abs_path()}"

    @staticmethod
    def build_document(row: dict[str, Any]) -> dict:
        """Transform the raw record from Big Query into a nested form for Firestore."""
        new_document: dict[str, Any] = {
            "formInformation": {},
            "metadata": {},
            "criteria": {},
            "caseNotes": {},
        }
        for key, value in row.items():
            # Rename form_information and metadata fields
            if key.startswith("form_information_"):
                formatted_key = re.sub(r"form_information_", "", key)
                new_document["formInformation"][formatted_key] = value
            elif key.startswith("metadata_"):
                formatted_key = re.sub(r"metadata_", "", key)
                new_document["metadata"][formatted_key] = value
            # transform reasons array to mapping
            elif key == "reasons":
                new_document["criteria"] = {
                    # conversion below is relatively naive and does not support CONSTANT_CASE,
                    # which is what we expect these names to be
                    reason["criteria_name"].lower(): reason["reason"]
                    for reason in value
                }
            elif key == "case_notes":
                for note in value:
                    criteria = note["criteria"]
                    if criteria not in new_document["caseNotes"]:
                        new_document["caseNotes"][criteria] = []

                    new_document["caseNotes"][criteria].append(
                        {
                            "noteTitle": note["note_title"],
                            "noteBody": note["note_body"],
                            "eventDate": note["event_date"],
                        }
                    )
            else:
                new_document[key] = value

        # Convert all keys to camelcase
        new_document = convert_nested_dictionary_keys(new_document, snake_to_camel)
        return new_document

    def transform_row(self, row: str) -> Tuple[Optional[str], Optional[dict]]:
        data = json.loads(row)
        return data["external_id"], self.build_document(data)
