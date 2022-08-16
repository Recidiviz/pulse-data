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
import re
from typing import Optional, Tuple

from recidiviz.common.str_field_utils import parse_int, person_name_case, snake_to_camel
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate

SUPERVISION_TYPE_MAPPING = {
    "DIVERSION": "Diversion",
    "TN PROBATIONER": "Probation",
    "TN PAROLEE": "Parole",
    "ISC FROM OTHER JURISDICTION": "ISC",
    "DETERMINATE RLSE PROBATIONER": "Determinate Release Probation",
    "SPCL ALT INCARCERATION UNIT": "SAIU",
    "MISDEMEANOR PROBATIONER": "Misdemeanor Probation",
}


class ClientRecordETLDelegate(WorkflowsFirestoreETLDelegate):
    """Delegate class to ETL the client_record.json file into Firestore."""

    STATE_CODE = "US_TN"
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
            "currentBalance": data["current_balance"],
            "specialConditions": data["special_conditions"],
            "district": data["district"],
        }
        supervision_type = data["supervision_type"]
        new_document["supervisionType"] = SUPERVISION_TYPE_MAPPING.get(
            supervision_type, supervision_type
        )

        # add nullable fields
        if "fee_exemptions" in data:
            new_document["feeExemptions"] = data["fee_exemptions"]

        if "phone_number" in data:
            new_document["phoneNumber"] = data["phone_number"]

        if "address" in data:
            # incoming strings may have cased state abbreviation wrong
            new_document["address"] = re.sub(
                r"\bTn\b", lambda m: m[0].upper(), data["address"]
            )

        if "last_payment_amount" in data:
            new_document["lastPaymentAmount"] = data["last_payment_amount"]

        if "supervision_level" in data:
            new_document["supervisionLevel"] = data["supervision_level"]

        if "special_conditions_flag" in data:
            new_document["specialConditionsFlag"] = data["special_conditions_flag"]

        # Note that date fields such as these are preserved as ISO strings (i.e., "YYYY-MM-DD")
        # rather than datetimes to avoid time-zone discrepancies
        if "supervision_level_start" in data:
            new_document["supervisionLevelStart"] = data["supervision_level_start"]

        if "next_special_conditions_check" in data:
            new_document["nextSpecialConditionsCheck"] = data[
                "next_special_conditions_check"
            ]

        if "last_special_conditions_note" in data:
            new_document["lastSpecialConditionsNote"] = data[
                "last_special_conditions_note"
            ]

        if "special_conditions_terminated_date" in data:
            new_document["specialConditionsTerminatedDate"] = data[
                "special_conditions_terminated_date"
            ]

        if "last_payment_date" in data:
            new_document["lastPaymentDate"] = data["last_payment_date"]

        if "expiration_date" in data:
            new_document["expirationDate"] = data["expiration_date"]

        if "supervision_start_date" in data:
            # TODO(#14224) Remove once frontend is migrated away from earliestSupervisionStartDateInLatestSystem
            new_document["earliestSupervisionStartDateInLatestSystem"] = data[
                "supervision_start_date"
            ]
            new_document["supervisionStartDate"] = data["supervision_start_date"]

        # add nullable objects
        if "compliant_reporting_eligible" in data:
            new_document["compliantReportingEligible"] = {
                "eligibilityCategory": data["compliant_reporting_eligible"],
                "currentOffenses": data.get("current_offenses"),
                "pastOffenses": data.get("past_offenses"),
                "lifetimeOffensesExpired": data.get("lifetime_offenses_expired"),
                "judicialDistrict": data.get("judicial_district"),
                "drugScreensPastYear": [
                    {
                        "result": screen["ContactNoteType"],
                        "date": screen["contact_date"],
                    }
                    for screen in data["drug_screens_past_year"]
                ],
                "sanctionsPastYear": data.get("sanctions_past_year"),
                "finesFeesEligible": data["fines_fees_eligible"],
            }

            if "eligible_level_start" in data:
                new_document["compliantReportingEligible"]["eligibleLevelStart"] = data[
                    "eligible_level_start"
                ]

            if "most_recent_arrest_check" in data:
                new_document["compliantReportingEligible"][
                    "mostRecentArrestCheck"
                ] = data["most_recent_arrest_check"]

            if data["zero_tolerance_codes"]:
                new_document["compliantReportingEligible"]["zeroToleranceCodes"] = [
                    {
                        "contactNoteType": code["ContactNoteType"],
                        "contactNoteDate": code["contact_date"],
                    }
                    for code in data["zero_tolerance_codes"]
                ]

            remaining_criteria_needed = parse_int(data["remaining_criteria_needed"])
            new_document["compliantReportingEligible"][
                "remainingCriteriaNeeded"
            ] = remaining_criteria_needed

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
                new_document["compliantReportingEligible"][
                    "almostEligibleCriteria"
                ] = almost_eligible_criteria

        if data["board_conditions"]:
            new_document["boardConditions"] = [
                {
                    "condition": condition["condition"],
                    "conditionDescription": condition["condition_description"],
                }
                for condition in data["board_conditions"]
            ]

        return data["person_external_id"], new_document


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        ClientRecordETLDelegate().run_etl()
