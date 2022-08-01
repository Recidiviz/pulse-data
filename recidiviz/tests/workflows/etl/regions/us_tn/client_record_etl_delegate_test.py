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
"""Tests the ability for ClientRecordEtlDelegate to parse json rows."""
import os
from unittest import TestCase

from recidiviz.workflows.etl.regions.us_tn.client_record_etl_delegate import (
    ClientRecordETLDelegate,
)


class ClientRecordEtlDelegateTest(TestCase):
    """
    Test class for the ClientRecordEtlDelegate
    """

    def test_transform_row(self) -> None:
        """
        Test that the transform_row method correctly parses the json
        """
        self.maxDiff = None
        delegate = ClientRecordETLDelegate()

        path_to_fixture = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "fixtures",
            "client_record.json",
        )
        with open(path_to_fixture, "r", encoding="utf-8") as fp:
            # first row is comprehensive
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "200")
            self.assertEqual(
                row,
                {
                    "address": "123 Etna st., Faketown, TN 12345",
                    "compliantReportingEligible": {
                        "eligibilityCategory": "c3",
                        "remainingCriteriaNeeded": 0,
                        "currentOffenses": None,
                        "pastOffenses": ["TRAFFIC OFFENSE", "EVADING ARREST"],
                        "drugScreensPastYear": [
                            {"date": "2021-02-03", "result": "DRUN"},
                            {"date": "2021-04-20", "result": "DRUN"},
                        ],
                        "eligibleLevelStart": "2021-03-17",
                        "judicialDistrict": "7",
                        "lifetimeOffensesExpired": None,
                        "mostRecentArrestCheck": "2021-11-15",
                        "sanctionsPastYear": ["OPRD"],
                        "finesFeesEligible": "regular_payments",
                        "zeroToleranceCodes": [
                            {
                                "contactNoteType": "COHC",
                                "contactNoteDate": "2020-02-02",
                            },
                            {
                                "contactNoteType": "PWAR",
                                "contactNoteDate": "2020-03-03",
                            },
                        ],
                    },
                    "currentBalance": 45.1,
                    "expirationDate": "2022-02-28",
                    "feeExemptions": "Exemption 1, Exemption2",
                    "lastPaymentAmount": 10.25,
                    "lastPaymentDate": "2021-12-20",
                    "nextSpecialConditionsCheck": "2021-12-02",
                    "lastSpecialConditionsNote": "2021-07-07",
                    "specialConditionsTerminatedDate": "2021-08-08",
                    "officerId": "100",
                    "personExternalId": "200",
                    "pseudonymizedId": "p200",
                    "personName": {
                        "givenNames": "Matilda",
                        "middleNames": "",
                        "nameSuffix": "",
                        "surname": "Mouse-House",
                    },
                    "phoneNumber": "8889997777",
                    "specialConditions": "SPECIAL",
                    "stateCode": "US_XX",
                    "supervisionLevel": "MEDIUM",
                    "supervisionLevelStart": "2020-03-10",
                    "supervisionType": "Probation",
                    "boardConditions": [
                        {
                            "condition": "CT",
                            "conditionDescription": "COMPLETE THERAPEUTIC COMMUNITY",
                        },
                        {
                            "condition": "CW",
                            "conditionDescription": "COMMUNITY SERVICE REFERRAL",
                        },
                    ],
                    "earliestSupervisionStartDateInLatestSystem": "2021-03-04",
                    "district": "DISTRICT 0",
                    "specialConditionsFlag": "current",
                },
            )

            # second row has none of the nullable fields
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "201")
            self.assertEqual(
                row,
                {
                    "personExternalId": "201",
                    "pseudonymizedId": "p201",
                    "stateCode": "US_XX",
                    "personName": {
                        "givenNames": "Harry",
                        "middleNames": "Henry",
                        "nameSuffix": "",
                        "surname": "Houdini IV",
                    },
                    "officerId": "102",
                    "currentBalance": 282,
                    "district": "DISTRICT X",
                    "supervisionType": "ISC",
                    "specialConditions": "NULL",
                },
            )

            # third row has almost-eligible data
            fixture = fp.readline()
            doc_id, row = delegate.transform_row(fixture)
            self.assertEqual(doc_id, "202")
            self.assertEqual(
                row,
                {
                    "personExternalId": "202",
                    "pseudonymizedId": "p202",
                    "personName": {
                        "givenNames": "Third",
                        "middleNames": "Persons",
                        "nameSuffix": "",
                        "surname": "Realname",
                    },
                    "address": "456 Etna st., Faketown, TN 12345",
                    "currentBalance": 45.1,
                    "expirationDate": "2022-02-28",
                    "feeExemptions": "Exemption 1, Exemption2",
                    "lastPaymentAmount": 10.25,
                    "lastPaymentDate": "2021-12-20",
                    "nextSpecialConditionsCheck": "2021-12-02",
                    "lastSpecialConditionsNote": "2021-07-07",
                    "specialConditionsTerminatedDate": "2021-08-08",
                    "officerId": "100",
                    "phoneNumber": "8889997777",
                    "specialConditions": "SPECIAL",
                    "stateCode": "US_XX",
                    "supervisionLevel": "MEDIUM",
                    "supervisionLevelStart": "2020-03-10",
                    "supervisionType": "Probation",
                    "boardConditions": [
                        {
                            "condition": "CT",
                            "conditionDescription": "COMPLETE THERAPEUTIC COMMUNITY",
                        },
                        {
                            "condition": "CW",
                            "conditionDescription": "COMMUNITY SERVICE REFERRAL",
                        },
                    ],
                    "earliestSupervisionStartDateInLatestSystem": "2021-03-04",
                    "district": "DISTRICT 0",
                    "specialConditionsFlag": "current",
                    "compliantReportingEligible": {
                        "eligibilityCategory": "c1",
                        "remainingCriteriaNeeded": 1,
                        "almostEligibleCriteria": {
                            "passedDrugScreenNeeded": True,
                        },
                        "currentOffenses": None,
                        "pastOffenses": ["TRAFFIC OFFENSE", "EVADING ARREST"],
                        "drugScreensPastYear": [],
                        "eligibleLevelStart": "2021-03-17",
                        "judicialDistrict": "7",
                        "lifetimeOffensesExpired": None,
                        "mostRecentArrestCheck": "2021-11-15",
                        "sanctionsPastYear": [],
                        "finesFeesEligible": "regular_payments",
                    },
                },
            )
