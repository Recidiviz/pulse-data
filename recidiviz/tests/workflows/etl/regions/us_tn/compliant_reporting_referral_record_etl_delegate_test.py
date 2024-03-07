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
from datetime import datetime, timezone
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from freezegun import freeze_time

from recidiviz.common.constants.states import StateCode
from recidiviz.tests.workflows.etl.workflows_firestore_etl_delegate_test import (
    FakeFileStream,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.regions.us_tn.compliant_reporting_referral_record_etl_delegate import (
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
        delegate = CompliantReportingReferralRecordETLDelegate(StateCode.US_TN)

        path_to_fixture = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "fixtures",
            "compliant_reporting_referral_record.json",
        )
        with open(path_to_fixture, "r", encoding="utf-8") as fp:
            # Row 1: Data in old query only
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "200")
            self.assertEqual(
                {
                    "stateCode": "US_XX",
                    "poFirstName": "Joey",
                    "poLastName": "Joe",
                    "clientFirstName": "Matilda",
                    "clientLastName": "Mouse",
                    "dateToday": "2022-03-25",
                    "tdocId": "200",
                    "physicalAddress": "123 fake st., Metropolis, TN 59545",
                    "convictionCounty": "123ABC",
                    "convictionCounties": ["123ABC"],
                    "allDockets": "[45367,87521]",
                    "docketNumbers": ["45367", "87521"],
                    "currentOffenses": ["BURGLARY", "AGGRAVATED BURGLARY"],
                    "finesFeesEligible": "regular_payments",
                    "drugScreensPastYear": [
                        {"date": "2021-02-03", "result": "DRUN"},
                        {"date": "2021-04-20", "result": "DRUN"},
                    ],
                    "eligibilityCategory": "c1",
                    "remainingCriteriaNeeded": 3,
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
                    "offenseTypeEligibility": 2,
                    "formInformation": {},
                    "metadata": {},
                    "eligibleCriteria": {},
                    "ineligibleCriteria": {},
                    "caseNotes": {},
                },
                row,
            )

            # Row 2: Eligible in new query
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "201")
            self.assertEqual(
                {
                    "stateCode": "US_XX",
                    "poFirstName": "Sally",
                    "poLastName": "Slithers",
                    "clientFirstName": "Harry",
                    "dateToday": "2022-03-25",
                    "tdocId": "201",
                    "currentEmployer": "WAIVER",
                    "driversLicense": "12345",
                    "drugScreensPastYear": [],
                    "eligibilityCategory": "c2",
                    "finesFeesEligible": "regular_payments",
                    "currentOffenses": ["THEFT"],
                    "remainingCriteriaNeeded": 0,
                    "zeroToleranceCodes": [
                        {"contactNoteDate": "2020-02-02", "contactNoteType": "COHC"},
                        {"contactNoteDate": "2020-03-03", "contactNoteType": "PWAR"},
                    ],
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
                    "externalId": "201",
                    "eligibleCriteria": {
                        "hasActiveSentence": {"hasActiveSentence": True},
                        "supervisionLevelIsNotInternalUnknown": None,
                        "supervisionLevelIsNotInterstateCompact": None,
                        "supervisionLevelIsNotUnassigned": None,
                        "supervisionNotPastFullTermCompletionDateOrUpcoming90Days": {
                            "eligibleDate": "2023-07-01"
                        },
                        "usTnFinesFeesEligible": {
                            "hasFinesFeesBalanceBelow500": {"amountOwed": 45},
                            "hasPayments3ConsecutiveMonths": {
                                "amountOwed": 45,
                                "consecutiveMonthlyPayments": 3,
                            },
                        },
                        "usTnIneligibleOffensesExpired": None,
                        "usTnNoArrestsInPastYear": None,
                        "usTnNoDuiOffenseInPast5Years": None,
                        "usTnNoHighSanctionsInPastYear": None,
                        "usTnNoMurderConvictions": None,
                        "usTnNoPriorRecordWithIneligibleCrOffense": {
                            "ineligibleOffenseDates": ["2006-01-09"],
                            "ineligibleOffenses": ["ASSAULT"],
                        },
                        "usTnNoRecentCompliantReportingRejections": None,
                        "usTnNoZeroToleranceCodesSpans": None,
                        "usTnNotInJudicialDistrict17WhileOnProbation": None,
                        "usTnNotOnLifeSentenceOrLifetimeSupervision": {
                            "lifetimeFlag": False
                        },
                        "usTnNotPermanentlyRejectedFromCompliantReporting": None,
                        "usTnNotServingIneligibleCrOffense": None,
                        "usTnNotServingUnknownCrOffense": None,
                        "usTnOnEligibleLevelForSufficientTime": {
                            "eligibleDate": "2018-04-05",
                            "eligibleLevel": "MINIMUM",
                        },
                        "usTnPassedDrugScreenCheck": {
                            "hasAtLeast1NegativeDrugTestPastYear": {
                                "latestNegativeScreenDates": ["2023-03-04"],
                                "latestNegativeScreenResults": ["DRUN"],
                            },
                            "hasAtLeast2NegativeDrugTestsPastYear": {
                                "latestNegativeScreenDates": ["2023-03-04"],
                                "latestNegativeScreenResults": ["DRUN"],
                            },
                            "latestAlcoholDrugNeedLevel": "LOW",
                            "latestDrugTestIsNegative": {
                                "latestDrugScreenDate": "2023-03-04",
                                "latestDrugScreenResult": "DRUN",
                            },
                        },
                        "usTnSpecialConditionsAreCurrent": {"speNoteDue": None},
                    },
                    "ineligibleCriteria": {},
                    "formInformation": {
                        "courtCostsPaid": None,
                        "courtName": "Circuit Court",
                        "currentExemptionsAndExpiration": None,
                        "currentOffenses": ["THEFT"],
                        "dateToday": "2023-07-21",
                        "docketNumbers": ["123A", "456B"],
                        "driversLicense": "12345",
                        "driversLicenseRevoked": None,
                        "driversLicenseSuspended": None,
                        "expirationDate": "2023-07-01",
                        "judicialDistrict": ["0", "1"],
                        "restitutionAmt": None,
                        "restitutionMonthlyPayment": None,
                        "restitutionMonthlyPaymentTo": [],
                        "sentenceLengthDays": "3552",
                        "sentenceStartDate": "2013-10-09",
                        "supervisionFeeArrearaged": "true",
                        "supervisionFeeArrearagedAmount": "45.0",
                        "supervisionFeeAssessed": "45.0",
                        "supervisionFeeWaived": "false",
                    },
                    "metadata": {
                        "allOffenses": ["BURGLARY", "THEFT"],
                        "convictionCounties": ["123 - ABC", "456 - DEF"],
                        "ineligibleOffensesExpired": [],
                        "mostRecentArrestCheck": {
                            "contactDate": "2023-03-01",
                            "contactType": "ARRN",
                        },
                        "mostRecentSpeNote": {
                            "contactDate": "2021-01-22",
                            "contactType": "SPET",
                        },
                        "specialConditionsTerminatedDate": "2021-01-22",
                    },
                    "caseNotes": {},
                },
                row,
            )

            # Row 3: Almost eligible in new query
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "202")
            self.assertEqual(
                {
                    "stateCode": "US_XX",
                    "poFirstName": "TEST",
                    "poLastName": "OFFICER1",
                    "clientFirstName": "TONYE",
                    "clientLastName": "THOMPSON",
                    "dateToday": "2022-03-25",
                    "tdocId": "202",
                    "drugScreensPastYear": [],
                    "eligibilityCategory": "c3",
                    "finesFeesEligible": "low_balance",
                    "remainingCriteriaNeeded": 1,
                    "currentOffenses": ["FAILURE TO APPEAR (FELONY)"],
                    "supervisionType": "TN PROBATIONER",
                    "supervisionFeeArrearaged": False,
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
                    "externalId": "202",
                    "eligibleCriteria": {
                        "hasActiveSentence": {"hasActiveSentence": True},
                        "supervisionLevelIsNotInternalUnknown": None,
                        "supervisionLevelIsNotInterstateCompact": None,
                        "supervisionLevelIsNotUnassigned": None,
                        "supervisionNotPastFullTermCompletionDateOrUpcoming90Days": {
                            "eligibleDate": "2029-06-06"
                        },
                        "usTnIneligibleOffensesExpired": None,
                        "usTnNoArrestsInPastYear": None,
                        "usTnNoDuiOffenseInPast5Years": None,
                        "usTnNoHighSanctionsInPastYear": None,
                        "usTnNoMurderConvictions": None,
                        "usTnNoPriorRecordWithIneligibleCrOffense": None,
                        "usTnNoRecentCompliantReportingRejections": None,
                        "usTnNoZeroToleranceCodesSpans": None,
                        "usTnNotInJudicialDistrict17WhileOnProbation": None,
                        "usTnNotOnLifeSentenceOrLifetimeSupervision": {
                            "lifetimeFlag": False
                        },
                        "usTnNotPermanentlyRejectedFromCompliantReporting": None,
                        "usTnNotServingIneligibleCrOffense": None,
                        "usTnNotServingUnknownCrOffense": None,
                        "usTnOnEligibleLevelForSufficientTime": {
                            "eligibleDate": "2020-08-01",
                            "eligibleLevel": "MINIMUM",
                        },
                        "usTnPassedDrugScreenCheck": {
                            "hasAtLeast1NegativeDrugTestPastYear": {
                                "latestNegativeScreenDates": ["2023-02-28"],
                                "latestNegativeScreenResults": ["DRUN"],
                            },
                            "hasAtLeast2NegativeDrugTestsPastYear": {
                                "latestNegativeScreenDates": ["2023-02-28"],
                                "latestNegativeScreenResults": ["DRUN"],
                            },
                            "latestAlcoholDrugNeedLevel": "LOW",
                            "latestDrugTestIsNegative": {
                                "latestDrugScreenDate": "2023-02-28",
                                "latestDrugScreenResult": "DRUN",
                            },
                        },
                        "usTnSpecialConditionsAreCurrent": {"speNoteDue": None},
                    },
                    "ineligibleCriteria": {
                        "usTnFinesFeesEligible": {
                            "hasFinesFeesBalanceBelow500": {"amountOwed": 700},
                            "hasPayments3ConsecutiveMonths": {
                                "amountOwed": 700,
                                "consecutiveMonthlyPayments": None,
                            },
                        }
                    },
                    "formInformation": {
                        "courtCostsPaid": None,
                        "courtName": "Circuit Court",
                        "currentExemptionsAndExpiration": None,
                        "currentOffenses": ["FAILURE TO APPEAR (FELONY)"],
                        "dateToday": "2023-07-21",
                        "docketNumbers": ["10000"],
                        "driversLicense": None,
                        "driversLicenseRevoked": None,
                        "driversLicenseSuspended": None,
                        "expirationDate": "2030-02-12",
                        "judicialDistrict": ["1"],
                        "restitutionAmt": "100.0",
                        "restitutionMonthlyPayment": "0.0",
                        "restitutionMonthlyPaymentTo": ["PAYMENT TO"],
                        "sentenceLengthDays": "3629",
                        "sentenceStartDate": "2020-03-07",
                        "supervisionFeeArrearaged": "true",
                        "supervisionFeeArrearagedAmount": "700.0",
                        "supervisionFeeAssessed": "700.0",
                        "supervisionFeeWaived": "false",
                    },
                    "metadata": {
                        "allOffenses": ["FAILURE TO APPEAR (FELONY)", "EVADING ARREST"],
                        "convictionCounties": ["123ABC"],
                        "ineligibleOffensesExpired": [],
                        "mostRecentArrestCheck": {
                            "contactDate": "2023-04-01",
                            "contactType": "ARRN",
                        },
                        "mostRecentSpeNote": {
                            "contactDate": "2019-08-15",
                            "contactType": "SPET",
                        },
                        "specialConditionsTerminatedDate": "2019-08-15",
                    },
                    "caseNotes": {},
                },
                row,
            )

            # Row 4: Data in new query only
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.maxDiff = None
            self.assertEqual(doc_id, "203")
            self.assertEqual(
                {
                    "stateCode": "US_XX",
                    "externalId": "203",
                    "eligibleCriteria": {
                        "hasActiveSentence": {"hasActiveSentence": True},
                        "supervisionLevelIsNotInternalUnknown": None,
                        "supervisionLevelIsNotInterstateCompact": None,
                        "supervisionLevelIsNotUnassigned": None,
                        "supervisionNotPastFullTermCompletionDateOrUpcoming90Days": {
                            "eligibleDate": "2025-01-18"
                        },
                        "usTnFinesFeesEligible": {
                            "hasFinesFeesBalanceBelow500": {"amountOwed": 0},
                            "hasPayments3ConsecutiveMonths": {
                                "amountOwed": 0,
                                "consecutiveMonthlyPayments": None,
                            },
                            "hasPermanentFinesFeesExemption": {
                                "currentExemptions": ["SSDB"],
                            },
                        },
                        "usTnIneligibleOffensesExpired": None,
                        "usTnNoArrestsInPastYear": None,
                        "usTnNoDuiOffenseInPast5Years": None,
                        "usTnNoHighSanctionsInPastYear": None,
                        "usTnNoMurderConvictions": None,
                        "usTnNoPriorRecordWithIneligibleCrOffense": None,
                        "usTnNoRecentCompliantReportingRejections": None,
                        "usTnNoZeroToleranceCodesSpans": {
                            "zeroToleranceCodeDates": None,
                        },
                        "usTnNotInJudicialDistrict17WhileOnProbation": None,
                        "usTnNotOnLifeSentenceOrLifetimeSupervision": {
                            "lifetimeFlag": False
                        },
                        "usTnNotPermanentlyRejectedFromCompliantReporting": None,
                        "usTnNotServingIneligibleCrOffense": None,
                        "usTnNotServingUnknownCrOffense": None,
                        "usTnOnEligibleLevelForSufficientTime": {
                            "eligibleDate": "2024-04-02",
                            "eligibleLevel": "MINIMUM",
                            "startDateOnEligibleLevel": "2023-04-02",
                        },
                        "usTnPassedDrugScreenCheck": {
                            "hasAtLeast1NegativeDrugTestPastYear": [
                                {
                                    "negativeScreenDate": "2023-11-03",
                                    "negativeScreenResult": "DRUN",
                                }
                            ],
                            "hasAtLeast2NegativeDrugTestsPastYear": [
                                {
                                    "negativeScreenDate": "2023-11-03",
                                    "negativeScreenResult": "DRUN",
                                }
                            ],
                            "latestAlcoholDrugNeedLevel": "LOW",
                            "latestDrugTestIsNegative": {
                                "latestDrugScreenDate": "2023-11-03",
                                "latestDrugScreenResult": "DRUN",
                            },
                        },
                        "usTnSpecialConditionsAreCurrent": {"speNoteDue": "2024-05-01"},
                    },
                    "ineligibleCriteria": {},
                    "formInformation": {
                        "courtCostsPaid": False,
                        "currentExemptionsAndExpiration": [
                            {"exemptionEndDate": None, "exemptionReason": "SSDB"}
                        ],
                        "currentOffenses": ["THEFT OF PROPERTY"],
                        "docketNumbers": ["20000"],
                        "driversLicense": "00000000",
                        "expirationDate": "2025-01-18",
                        "judicialDistrict": [],
                        "restitutionAmt": 0,
                        "restitutionMonthlyPayment": 0,
                        "restitutionMonthlyPaymentTo": [],
                        "sentenceLengthDays": "729",
                        "sentenceStartDate": "2023-01-20",
                        "supervisionFeeArrearaged": False,
                        "supervisionFeeArrearagedAmount": 0,
                        "supervisionFeeAssessed": 0,
                        "supervisionFeeWaived": True,
                    },
                    "metadata": {
                        "allOffenses": ["THEFT OF PROPERTY"],
                        "convictionCounties": ["ABC123"],
                        "ineligibleOffensesExpired": [],
                        "mostRecentArrestCheck": {
                            "contactDate": "2023-10-20",
                            "contactType": "ARRN",
                        },
                        "mostRecentSpeNote": {
                            "contactDate": "2023-10-25",
                            "contactType": "SPEC",
                        },
                    },
                    "caseNotes": {},
                },
                row,
            )

    @patch("google.cloud.firestore_admin_v1.FirestoreAdminClient")
    @patch("google.cloud.firestore_v1.Client")
    @patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.get_collection")
    @patch("recidiviz.firestore.firestore_client.FirestoreClientImpl.batch")
    @patch(
        "recidiviz.firestore.firestore_client.FirestoreClientImpl.delete_old_documents"
    )
    @patch(
        "recidiviz.workflows.etl.workflows_etl_delegate.WorkflowsETLDelegate.get_file_stream"
    )
    def test_run_etl(
        self,
        mock_get_file_stream: MagicMock,
        _mock_delete_old_documents: MagicMock,
        mock_batch_writer: MagicMock,
        mock_get_collection: MagicMock,
        _mock_firestore_client: MagicMock,
        _mock_firestore_admin_client: MagicMock,
    ) -> None:
        """Tests that the ETL Delegate for CompliantReportingReferralRecord imports the collection with the
        document_id."""
        mock_batch_set = MagicMock()
        mock_batch_writer.return_value = mock_batch_set
        mock_get_file_stream.return_value = [FakeFileStream(1)]
        mock_collection = MagicMock()
        mock_get_collection.return_value = mock_collection
        mock_document_ref = MagicMock()
        mock_collection.document.return_value = mock_document_ref
        mock_now = datetime(2022, 5, 1, tzinfo=timezone.utc)
        document_id = "us_tn_123"
        with local_project_id_override("test-project"):
            with freeze_time(mock_now):
                with mock.patch.object(
                    CompliantReportingReferralRecordETLDelegate, "transform_row"
                ) as mock_transform:
                    mock_transform.return_value = (123, {"personExternalId": 123})
                    delegate = CompliantReportingReferralRecordETLDelegate(
                        StateCode.US_TN
                    )
                    delegate.run_etl("compliant_reporting_referral_record.json")
                    mock_collection.document.assert_called_once_with(document_id)
                    mock_batch_set.set.assert_called_once_with(
                        mock_document_ref,
                        {
                            "personExternalId": 123,
                            "__loadedAt": mock_now,
                        },
                    )
