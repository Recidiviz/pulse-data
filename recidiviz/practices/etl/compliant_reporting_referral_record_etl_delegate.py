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
"""Delegate class to ETL compliant reporting referral records for practices into Firestore."""
import json
import logging
from typing import Tuple

from recidiviz.practices.etl.practices_etl_delegate import PracticesFirestoreETLDelegate
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


class CompliantReportingReferralRecordETLDelegate(PracticesFirestoreETLDelegate):
    """Delegate class to ETL the compliant_reporting_referral_record.json file into Firestore."""

    EXPORT_FILENAME = "compliant_reporting_referral_record.json"
    COLLECTION_NAME = "compliantReportingReferrals"

    def transform_row(self, row: str) -> Tuple[str, dict]:
        data = json.loads(row)

        # First fill the non-nullable fields
        new_document = {
            "poFirstName": data["po_first_name"],
            "poLastName": data["po_last_name"],
            "clientFirstName": data["client_first_name"],
            "clientLastName": data.get(
                "client_last_name", ""
            ),  # this has few enough null entries in the data that we shouldn't really treat it as optional
            "dateToday": data["date_today"],
            "tdocId": data["tdoc_id"],
            "currentOffenses": json.loads(data["current_offenses"]),
            "supervisionType": data["supervision_type"],
            "supervisionFeeAssessed": data.get("supervision_fee_assessed", ""),
            "supervisionFeeArrearaged": data["supervision_fee_arrearaged"],
            "supervisionFeeArrearagedAmount": data.get(
                "supervision_fee_arrearaged_amount", ""
            ),
            "supervisionFeeExemptionType": json.loads(
                data["supervision_fee_exemption_type"]
            ),
            "courtCostsPaid": data["court_costs_paid"],
            "specialConditionsAlcDrugScreen": data[
                "special_conditions_alc_drug_screen"
            ],
            "specialConditionsAlcDrugAssessmentComplete": data[
                "special_conditions_alc_drug_assessment_complete"
            ],
            "specialConditionsAlcDrugTreatment": data[
                "special_conditions_alc_drug_treatment"
            ],
            "specialConditionsAlcDrugTreatmentCurrent": data[
                "special_conditions_alc_drug_treatment_current"
            ],
            "specialConditionsCounseling": data["special_conditions_counseling"],
            "specialConditionsCounselingAngerManagementCurrent": data[
                "special_conditions_counseling_anger_management_current"
            ],
            "specialConditionsCommunityService": data[
                "special_conditions_community_service"
            ],
            "specialConditionsCommunityServiceCurrent": data[
                "special_conditions_community_service_current"
            ],
            "specialConditionsProgramming": data["special_conditions_programming"],
            "specialConditionsProgrammingCognitiveBehavior": data[
                "special_conditions_programming_cognitive_behavior"
            ],
            "specialConditionsProgrammingCognitiveBehaviorCurrent": data[
                "special_conditions_programming_cognitive_behavior_current"
            ],
            "specialConditionsProgrammingSafe": data[
                "special_conditions_programming_safe"
            ],
            "specialConditionsProgrammingSafeCurrent": data[
                "special_conditions_programming_safe_current"
            ],
            "specialConditionsProgrammingVictimImpact": data[
                "special_conditions_programming_victim_impact"
            ],
            "specialConditionsProgrammingVictimImpactCurrent": data[
                "special_conditions_programming_victim_impact_current"
            ],
            "specialConditionsProgrammingFsw": data[
                "special_conditions_programming_fsw"
            ],
            "specialConditionsProgrammingFswCurrent": data[
                "special_conditions_programming_fsw_current"
            ],
        }

        # add nullable fields
        if "physical_address" in data:
            new_document["physicalAddress"] = data["physical_address"]

        if "current_employer" in data:
            new_document["currentEmployer"] = data["current_employer"]

        if "drivers_license" in data:
            new_document["driversLicense"] = data["drivers_license"]

        if "drivers_license_suspended" in data:
            new_document["driversLicenseSuspended"] = data["drivers_license_suspended"]

        if "drivers_license_revoked" in data:
            new_document["driversLicenseRevoked"] = data["drivers_license_revoked"]

        if "conviction_county" in data:
            new_document["convictionCounty"] = data["conviction_county"]

        if "court_name" in data:
            new_document["courtName"] = data["court_name"]

        if "all_dockets" in data:
            new_document["allDockets"] = data["all_dockets"]

        if "sentence_start_date" in data:
            new_document["sentenceStartDate"] = data["sentence_start_date"]

        if "expiration_date" in data:
            new_document["expirationDate"] = data["expiration_date"]

        if "sentence_length_days" in data:
            sentence_length_days = data["sentence_length_days"]
            if int(sentence_length_days) >= 0:
                new_document["sentenceLengthDays"] = sentence_length_days
            else:
                # If the sentence length is negative, assume something has gone wrong and clear out
                # the start/expiration dates as well
                new_document.pop("sentenceStartDate", None)
                new_document.pop("expirationDate", None)

        if "supervision_fee_exemption_expir_date" in data:
            new_document["supervisionFeeExemptionExpirDate"] = data[
                "supervision_fee_exemption_expir_date"
            ]

        if "supervision_fee_waived" in data:
            new_document["supervisionFeeWaived"] = data["supervision_fee_waived"]

        if "court_costs_monthly_amt_1" in data:
            new_document["courtCostsMonthlyAmt1"] = data["court_costs_monthly_amt_1"]

        if "court_costs_monthly_amt_2" in data:
            new_document["courtCostsMonthlyAmt2"] = data["court_costs_monthly_amt_2"]

        if "restitution_amt" in data:
            new_document["restitutionAmt"] = data["restitution_amt"]

        if "restitution_monthly_payment" in data:
            new_document["restitutionMonthlyPayment"] = data[
                "restitution_monthly_payment"
            ]

        if "restitution_monthly_payment_to" in data:
            new_document["restitutionMonthlyPaymentTo"] = json.loads(
                data["restitution_monthly_payment_to"]
            )

        if "special_conditions_alc_drug_screen_date" in data:
            new_document["specialConditionsAlcDrugScreenDate"] = data[
                "special_conditions_alc_drug_screen_date"
            ]

        if "special_conditions_alc_drug_assessment" in data:
            new_document["specialConditionsAlcDrugAssessment"] = data[
                "special_conditions_alc_drug_assessment"
            ]

        if "court_costs_balance" in data:
            new_document["courtCostsBalance"] = data["court_costs_balance"]

        if "special_conditions_alc_drug_assessment_complete_date" in data:
            new_document["specialConditionsAlcDrugAssessmentCompleteDate"] = data[
                "special_conditions_alc_drug_assessment_complete_date"
            ]

        if "special_conditions_alc_drug_treatment_in_out" in data:
            new_document["specialConditionsAlcDrugTreatmentInOut"] = data[
                "special_conditions_alc_drug_treatment_in_out"
            ]

        if "special_conditions_alc_drug_treatment_complete_dat" in data:
            new_document["specialConditionsAlcDrugTreatmentCompleteDate"] = data[
                "special_conditions_alc_drug_treatment_complete_dat"
            ]

        if "special_conditions_counseling_anger_management" in data:
            new_document["specialConditionsCounselingAngerManagement"] = data[
                "special_conditions_counseling_anger_management"
            ]

        if "special_conditions_counseling_anger_management_complete_date" in data:
            new_document[
                "specialConditionsCounselingAngerManagementCompleteDate"
            ] = data["special_conditions_counseling_anger_management_complete_date"]

        if "special_conditions_counseling_mental_health" in data:
            new_document["specialConditionsCounselingMentalHealth"] = data[
                "special_conditions_counseling_mental_health"
            ]

        if "special_conditions_counseling_mental_health_current" in data:
            new_document["specialConditionsCounselingMentalHealthCurrent"] = data[
                "special_conditions_counseling_mental_health_current"
            ]

        if "special_conditions_counseling_mental_health_complete_date" in data:
            new_document["specialConditionsCounselingMentalHealthCompleteDate"] = data[
                "special_conditions_counseling_mental_health_complete_date"
            ]

        if "special_conditions_community_service_hours" in data:
            new_document["specialConditionsCommunityServiceHours"] = data[
                "special_conditions_community_service_hours"
            ]

        if "special_conditions_programming_cognitive_behavior_completion_date" in data:
            new_document[
                "specialConditionsProgrammingCognitiveBehaviorCompletionDate"
            ] = data[
                "special_conditions_programming_cognitive_behavior_completion_date"
            ]

        if "special_conditions_community_service_completion_date" in data:
            new_document["specialConditionsCommunityServiceCompletionDate"] = data[
                "special_conditions_community_service_completion_date"
            ]

        if "special_conditions_programming_safe_completion_date" in data:
            new_document["specialConditionsProgrammingSafeCompletionDate"] = data[
                "special_conditions_programming_safe_completion_date"
            ]

        if "special_conditions_programming_victim_impact_completion_date" in data:
            new_document[
                "specialConditionsProgrammingVictimImpactCompletionDate"
            ] = data["special_conditions_programming_victim_impact_completion_date"]

        if "special_conditions_programming_fsw_completion_date" in data:
            new_document["specialConditionsProgrammingFswCompletionDate"] = data[
                "special_conditions_programming_fsw_completion_date"
            ]

        return data["tdoc_id"], new_document


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        CompliantReportingReferralRecordETLDelegate().run_etl()
