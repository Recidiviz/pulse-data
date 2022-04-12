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
"""Tests the ability for CompliantReportingReferralRecordEtlDelegate to parse json rows."""
import os
from unittest import TestCase

from recidiviz.practices.etl.compliant_reporting_referral_record_etl_delegate import (
    CompliantReportingReferralRecordETLDelegate,
)


class CompliantReportingReferralRecordEtlDelegateTest(TestCase):
    """
    Test class for the CompliantReportingReferralRecordETLDelegate
    """

    def test_transform_row(self) -> None:
        """
        Test that the transform_row method correctly parses the json
        """
        delegate = CompliantReportingReferralRecordETLDelegate()

        path_to_fixture = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "fixtures",
            "compliant_reporting_referral_record.json",
        )
        with open(path_to_fixture, "r", encoding="utf-8") as fp:
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "200")
            self.assertEqual(
                row,
                {
                    "poFirstName": "Joey",
                    "poLastName": "Joe",
                    "clientFirstName": "Matilda",
                    "clientLastName": "Mouse",
                    "dateToday": "2022-03-25",
                    "tdocId": "200",
                    "physicalAddress": "123 fake st., Metropolis, TN 59545",
                    "convictionCounty": "123ABC",
                    "currentOffenses": ["BURGLARY", "AGGRAVATED BURGLARY"],
                    "supervisionType": "TN PROBATIONER",
                    "sentenceStartDate": "2020-01-01",
                    "sentenceLengthDays": "1000",
                    "expirationDate": "2024-01-01",
                    "supervisionFeeAssessed": "1000.0",
                    "supervisionFeeArrearaged": True,
                    "supervisionFeeArrearagedAmount": "1138.0",
                    "supervisionFeeExemptionType": [],
                    "courtCostsPaid": False,
                    "restitutionAmt": "200.0",
                    "restitutionMonthlyPayment": "0.0",
                    "restitutionMonthlyPaymentTo": ["SEE ORDER IN FILE"],
                    "specialConditionsAlcDrugScreen": False,
                    "specialConditionsAlcDrugScreenDate": "2022-02-18",
                    "specialConditionsAlcDrugAssessmentComplete": False,
                    "specialConditionsAlcDrugTreatment": False,
                    "specialConditionsAlcDrugTreatmentCurrent": False,
                    "specialConditionsCounseling": False,
                    "specialConditionsCounselingAngerManagementCurrent": False,
                    "specialConditionsCommunityService": False,
                    "specialConditionsCommunityServiceCurrent": False,
                    "specialConditionsProgramming": False,
                    "specialConditionsProgrammingCognitiveBehavior": False,
                    "specialConditionsProgrammingCognitiveBehaviorCurrent": False,
                    "specialConditionsProgrammingSafe": False,
                    "specialConditionsProgrammingSafeCurrent": False,
                    "specialConditionsProgrammingVictimImpact": False,
                    "specialConditionsProgrammingVictimImpactCurrent": False,
                    "specialConditionsProgrammingFsw": False,
                    "specialConditionsProgrammingFswCurrent": False,
                },
            )

            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "201")
            self.assertEqual(
                row,
                {
                    "poFirstName": "Sally",
                    "poLastName": "Slithers",
                    "clientFirstName": "Harry",
                    "clientLastName": "",
                    "dateToday": "2022-03-25",
                    "tdocId": "201",
                    "currentEmployer": "WAIVER",
                    "driversLicense": "12345",
                    "currentOffenses": ["THEFT"],
                    "supervisionType": "TN PAROLEE",
                    "supervisionFeeAssessed": "0.0",
                    "supervisionFeeArrearaged": False,
                    "supervisionFeeArrearagedAmount": "0.0",
                    "supervisionFeeExemptionType": ["SSDB", "SSDB"],
                    "supervisionFeeWaived": "Fees Waived",
                    "courtCostsPaid": False,
                    "specialConditionsAlcDrugScreen": False,
                    "specialConditionsAlcDrugAssessmentComplete": False,
                    "specialConditionsAlcDrugTreatment": False,
                    "specialConditionsAlcDrugTreatmentCurrent": False,
                    "specialConditionsCounseling": False,
                    "specialConditionsCounselingAngerManagementCurrent": False,
                    "specialConditionsCommunityService": False,
                    "specialConditionsCommunityServiceCurrent": False,
                    "specialConditionsProgramming": False,
                    "specialConditionsProgrammingCognitiveBehavior": False,
                    "specialConditionsProgrammingCognitiveBehaviorCurrent": False,
                    "specialConditionsProgrammingSafe": False,
                    "specialConditionsProgrammingSafeCurrent": False,
                    "specialConditionsProgrammingVictimImpact": False,
                    "specialConditionsProgrammingVictimImpactCurrent": False,
                    "specialConditionsProgrammingFsw": False,
                    "specialConditionsProgrammingFswCurrent": False,
                },
            )

            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "202")
            self.assertEqual(
                row,
                {
                    "poFirstName": "TEST",
                    "poLastName": "OFFICER1",
                    "clientFirstName": "TONYE",
                    "clientLastName": "THOMPSON",
                    "dateToday": "2022-03-25",
                    "tdocId": "202",
                    "currentOffenses": ["FAILURE TO APPEAR (FELONY)"],
                    "supervisionType": "TN PROBATIONER",
                    "supervisionFeeAssessed": "",
                    "supervisionFeeArrearaged": False,
                    "supervisionFeeArrearagedAmount": "",
                    "supervisionFeeExemptionType": ["SSDB", "SSDB"],
                    "courtCostsPaid": False,
                    "specialConditionsAlcDrugScreen": False,
                    "specialConditionsAlcDrugAssessmentComplete": False,
                    "specialConditionsAlcDrugTreatment": False,
                    "specialConditionsAlcDrugTreatmentCurrent": False,
                    "specialConditionsCounseling": False,
                    "specialConditionsCounselingAngerManagementCurrent": False,
                    "specialConditionsCommunityService": False,
                    "specialConditionsCommunityServiceCurrent": False,
                    "specialConditionsProgramming": False,
                    "specialConditionsProgrammingCognitiveBehavior": False,
                    "specialConditionsProgrammingCognitiveBehaviorCurrent": False,
                    "specialConditionsProgrammingSafe": False,
                    "specialConditionsProgrammingSafeCurrent": False,
                    "specialConditionsProgrammingVictimImpact": False,
                    "specialConditionsProgrammingVictimImpactCurrent": False,
                    "specialConditionsProgrammingFsw": False,
                    "specialConditionsProgrammingFswCurrent": False,
                },
            )
