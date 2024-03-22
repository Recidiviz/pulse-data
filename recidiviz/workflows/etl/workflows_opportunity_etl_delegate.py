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
from typing import Any, Dict, List, Optional, Set, Tuple

import attr

from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIGS,
)
from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate


@attr.s
class OpportunityExportConfig:
    source_filename: str = attr.ib()
    export_collection_name: str = attr.ib()


CRITERIA_TO_DEPREFIX: List[str] = [
    "SUPERVISION_LEVEL_HIGHER_THAN_ASSESSMENT_LEVEL",
]


class WorkflowsOpportunityETLDelegate(WorkflowsFirestoreETLDelegate):
    """Generic delegate for loading Workflows' opportunity records into Firestore."""

    def __init__(self, state_code: StateCode):
        super().__init__(state_code)

        self.criteria_to_deprefix = {
            f"{state_code.value.upper()}_{criterion}": criterion
            for criterion in CRITERIA_TO_DEPREFIX
        }

        if state_code == StateCode.US_ID:
            # After exporting, US_IX files will be in a US_ID bucket, but their
            # criteria may still be prefixed with US_IX_
            self.criteria_to_deprefix.update(
                {f"US_IX_{criterion}": criterion for criterion in CRITERIA_TO_DEPREFIX}
            )

    @property
    def COLLECTION_BY_FILENAME(self) -> Dict[str, str]:
        return {
            config.source_filename: config.export_collection_name
            for config in WORKFLOWS_OPPORTUNITY_CONFIGS
        }

    def get_supported_files(self) -> List[str]:
        return [
            config.source_filename
            for config in WORKFLOWS_OPPORTUNITY_CONFIGS
            if config.state_code == self.state_code
            or (
                # The bucket for US_IX files is still US_ID
                config.state_code == StateCode.US_IX
                and self.state_code == StateCode.US_ID
            )
        ]

    @property
    def timestamp_key(self) -> str:
        """Name of the key this delegate will insert into each document to record when it was loaded."""
        return "__loadedAt"

    def filepath_url(self, filename: str) -> str:
        return f"gs://{self.get_filepath(filename).abs_path()}"

    def _preprocess_criterion_name(self, criterion: str) -> str:
        # Rename US_IX criteria to US_ID so we don't have to make frontend changes
        if self.state_code == StateCode.US_ID and criterion.upper().startswith("US_IX"):
            criterion = criterion.replace("US_IX", "US_ID", 1)

        # checks against a list of criterion to remove state prefixes.
        # converts to lower case since the snake_case to camelCase conversion
        # below is relatively naive and does not support CONSTANT_CASE,
        # which is what we expect these names to be
        return self.criteria_to_deprefix.get(criterion, criterion).lower()

    def _is_reason_blob(self, value: Any) -> bool:
        if not isinstance(value, List):
            return False

        for item in value:
            if not isinstance(item, Dict):
                return False
            if sorted(item) != ["criteria_name", "reason"]:
                return False

        return True

    def _process_criteria(self, criteria: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            self._preprocess_criterion_name(reason["criteria_name"]): (
                self._process_criteria(reason["reason"])
                if self._is_reason_blob(reason["reason"])
                else reason["reason"]
            )
            for reason in criteria
        }

    def build_document(self, row: dict[str, Any]) -> dict:
        """Transform the raw record from Big Query into a nested form for Firestore."""
        new_document: dict[str, Any] = {
            "formInformation": {},
            "metadata": {},
            "eligibleCriteria": {},
            "ineligibleCriteria": {},
            "caseNotes": {},
        }
        processed_criteria: dict[str, Any] = {}
        ineligible_criteria: Optional[Set[str]] = None

        # Process form_information, metadata, reasons, and case_notes
        for key, value in row.items():
            if key.startswith("form_information_"):
                new_document["formInformation"][
                    key.removeprefix("form_information_")
                ] = value
            elif key.startswith("metadata_"):
                new_document["metadata"][key.removeprefix("metadata_")] = value
            elif key == "reasons":
                processed_criteria = self._process_criteria(value)
            elif key == "ineligible_criteria":
                ineligible_criteria = {
                    self._preprocess_criterion_name(v) for v in value
                }
            elif key == "case_notes":
                if value:
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

        # Split processed criteria if ineligibleCriteria is present,
        # otherwise set all processed criteria to eligibleCriteria
        if ineligible_criteria:
            new_document["eligibleCriteria"] = {
                k: v
                for k, v in processed_criteria.items()
                if k not in ineligible_criteria
            }
            new_document["ineligibleCriteria"] = {
                k: v for k, v in processed_criteria.items() if k in ineligible_criteria
            }
        else:
            new_document["eligibleCriteria"] = processed_criteria

        # Convert all keys to camelcase and return
        return convert_nested_dictionary_keys(new_document, snake_to_camel)

    def transform_row(self, row: str) -> Tuple[Optional[str], Optional[dict]]:
        data = json.loads(row)
        if data.get("external_id", None) is None:
            return None, None
        return data["external_id"], self.build_document(data)
