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
"""Delegate class to ETL compliant reporting referral records for workflows into Firestore."""
import json
import logging
from typing import Any, Optional, Tuple

from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import parse_int, snake_to_camel
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_etl_delegate import (
    WorkflowsSingleStateETLDelegate,
)
from recidiviz.workflows.etl.workflows_opportunity_etl_delegate import (
    WorkflowsOpportunityETLDelegate,
)


class CompliantReportingReferralRecordETLDelegate(WorkflowsSingleStateETLDelegate):
    """Delegate class to ETL the compliant_reporting_referral_record.json file into Firestore."""

    SUPPORTED_STATE_CODE = StateCode.US_TN
    EXPORT_FILENAME = "compliant_reporting_referral_record.json"
    _COLLECTION_NAME_BASE = "compliantReportingReferrals"

    generic_opportunity_delegate = WorkflowsOpportunityETLDelegate(StateCode.US_TN)

    def transform_row(self, row: str) -> Tuple[Optional[str], Optional[dict]]:
        data = json.loads(row)

        generic_opportunity_doc = {
            k: v
            for k, v in self.generic_opportunity_delegate.build_document(data).items()
            if k
            in [
                "stateCode",
                "externalId",
                "formInformation",
                "metadata",
                "criteria",
                "eligibleCriteria",
                "ineligibleCriteria",
                "caseNotes",
            ]
        }

        if not "tdoc_id" in data and "external_id" in data:
            return data["external_id"], generic_opportunity_doc

        if "compliant_reporting_eligible" not in data:
            return None, None

        new_document: dict[str, Any] = {}
        for key, value in data.items():
            if key == "compliant_reporting_eligible":
                new_document["eligibilityCategory"] = value
            elif key == "physical_address":
                # Addresses arrive in the form: "123 Fake st., Metropolis, Tn 59545
                # This fixes the misformatted state code
                new_document[key] = value.replace(" Tn ", " TN ")
            elif key == "drug_screens_past_year":
                new_document[key] = [
                    {
                        "result": screen["ContactNoteType"],
                        "date": screen["contact_date"],
                    }
                    for screen in value
                ]
            elif key == "zero_tolerance_codes":
                new_document[key] = [
                    {
                        "contactNoteType": code["ContactNoteType"],
                        "contactNoteDate": code["contact_date"],
                    }
                    for code in value
                ]
            elif key.startswith("almost_eligible_") or key in [
                "date_supervision_level_eligible",
                "cr_rejections_past_3_months",
                "date_serious_sanction_eligible",
            ]:
                # The keys for almost eligible don't line up with what the data is transformed to,
                # so handle them separately.
                continue
            elif key == "remaining_criteria_needed":
                new_document[key] = parse_int(data["remaining_criteria_needed"])
            elif (
                not key.startswith("metadata_")
                and not key.startswith("form_information_")
                and key != "reasons"
            ):
                # Skip fields that only appear in the new opportunity record, which will be parsed
                # by the generic ETL delegate
                new_document[key] = value

        if "sentence_length_days" in new_document:
            sentence_length_days = new_document["sentence_length_days"]
            if int(sentence_length_days) < 0:
                # If the sentence length is negative, assume something has gone wrong and clear out
                # the start/expiration dates as well
                new_document.pop("sentence_length_days", None)
                new_document.pop("sentence_start_date", None)
                new_document.pop("expiration_date", None)

        almost_eligible_criteria = {}

        if data.get("almost_eligible_time_on_supervision_level") and (
            current_level_eligibility_date := data.get(
                "date_supervision_level_eligible"
            )
        ):
            almost_eligible_criteria[
                "currentLevelEligibilityDate"
            ] = current_level_eligibility_date

        if data.get("almost_eligible_drug_screen"):
            almost_eligible_criteria["passedDrugScreenNeeded"] = True

        if data.get("almost_eligible_fines_fees"):
            almost_eligible_criteria["paymentNeeded"] = True

        if data.get("almost_eligible_recent_rejection") and (
            recent_rejection_codes := data.get("cr_rejections_past_3_months")
        ):
            almost_eligible_criteria["recentRejectionCodes"] = list(
                set(recent_rejection_codes)
            )

        if data.get("almost_eligible_serious_sanctions") and (
            serious_sanctions_eligibility_date := data.get(
                "date_serious_sanction_eligible"
            )
        ):
            almost_eligible_criteria[
                "seriousSanctionsEligibilityDate"
            ] = serious_sanctions_eligibility_date

        if almost_eligible_criteria:
            new_document["almostEligibleCriteria"] = almost_eligible_criteria

        new_document = convert_nested_dictionary_keys(new_document, snake_to_camel)

        # Merge the document created with the new compliant reporting opportunity record
        return data["tdoc_id"], new_document | generic_opportunity_doc


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        CompliantReportingReferralRecordETLDelegate(StateCode.US_TN).run_etl(
            "compliant_reporting_referral_record.json"
        )
