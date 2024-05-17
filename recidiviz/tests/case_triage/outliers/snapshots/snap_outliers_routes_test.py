"""
Snapshots for recidiviz/tests/case_triage/outliers/outliers_routes_test.py
Update snapshots automatically by running `pytest recidiviz/tests/case_triage/outliers/outliers_routes_test.py --snapshot-update
Remember to include a docstring like this after updating the snapshots for Pylint purposes
"""

# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_benchmarks"] = {
    "metrics": [
        {
            "benchmarks": [
                {"endDate": "2023-05-01", "target": 0.14, "threshold": 0.21},
                {"endDate": "2023-04-01", "target": 0.14, "threshold": 0.21},
                {"endDate": "2023-03-01", "target": 0.14, "threshold": 0.21},
                {"endDate": "2023-02-01", "target": 0.14, "threshold": 0.21},
                {"endDate": "2023-01-01", "target": 0.14, "threshold": 0.21},
                {"endDate": "2022-12-01", "target": 0.14, "threshold": 0.21},
            ],
            "caseloadType": "ALL",
            "latestPeriodValues": {"far": [0.8], "met": [0.1], "near": [0.32]},
            "metricId": "absconsions_bench_warrants",
        }
    ]
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_client_success"] = {
    "client": {
        "birthdate": "1989-01-01",
        "clientId": "111",
        "clientName": {"givenNames": "Harry", "middleNames": None, "surname": "Potter"},
        "gender": "MALE",
        "pseudonymizedClientId": "clienthash1",
        "raceOrEthnicity": "WHITE",
        "stateCode": "US_PA",
    }
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_client_success_for_csg_user"
] = {
    "client": {
        "birthdate": "1989-01-01",
        "clientId": "111",
        "clientName": {"givenNames": "Harry", "middleNames": None, "surname": "Potter"},
        "gender": "MALE",
        "pseudonymizedClientId": "clienthash1",
        "raceOrEthnicity": "WHITE",
        "stateCode": "US_PA",
    }
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_events_by_client_no_name"] = {
    "events": [
        {
            "attributes": None,
            "clientId": "555",
            "clientName": None,
            "eventDate": "2023-04-01",
            "metricId": "violations",
            "officerId": "03",
            "pseudonymizedClientId": "clienthash5",
            "stateCode": "US_PA",
        }
    ]
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_events_by_client_success"] = {
    "events": [
        {
            "attributes": {"testKey": "test_value"},
            "clientId": "111",
            "clientName": {
                "givenNames": "Harry",
                "middleNames": "",
                "surname": "Potter",
            },
            "eventDate": "2023-05-01",
            "metricId": "violations",
            "officerId": "03",
            "pseudonymizedClientId": "clienthash1",
            "stateCode": "US_PA",
        }
    ]
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_events_by_client_success_default_metrics"
] = {
    "events": [
        {
            "attributes": {"testKey": "test_value"},
            "clientId": "111",
            "clientName": {
                "givenNames": "Harry",
                "middleNames": "",
                "surname": "Potter",
            },
            "eventDate": "2023-05-01",
            "metricId": "violations",
            "officerId": "03",
            "pseudonymizedClientId": "clienthash1",
            "stateCode": "US_PA",
        }
    ]
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_events_by_officer_mismatched_supervisor_can_access_all"
] = {
    "events": [
        {
            "attributes": None,
            "clientId": "222",
            "clientName": {
                "givenNames": "Olivia",
                "middleNames": "",
                "surname": "Rodrigo",
            },
            "eventDate": "2023-04-01",
            "metricId": "absconsions_bench_warrants",
            "officerAssignmentDate": "2022-01-01",
            "officerAssignmentEndDate": "2023-06-01",
            "officerId": "03",
            "pseudonymizedClientId": "clienthash2",
            "stateCode": "US_PA",
            "supervisionEndDate": "2023-06-01",
            "supervisionStartDate": "2022-01-01",
            "supervisionType": "PROBATION",
        }
    ]
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_events_by_officer_success"
] = {
    "events": [
        {
            "attributes": None,
            "clientId": "222",
            "clientName": {
                "givenNames": "Olivia",
                "middleNames": "",
                "surname": "Rodrigo",
            },
            "eventDate": "2023-04-01",
            "metricId": "absconsions_bench_warrants",
            "officerAssignmentDate": "2022-01-01",
            "officerAssignmentEndDate": "2023-06-01",
            "officerId": "03",
            "pseudonymizedClientId": "clienthash2",
            "stateCode": "US_PA",
            "supervisionEndDate": "2023-06-01",
            "supervisionStartDate": "2022-01-01",
            "supervisionType": "PROBATION",
        }
    ]
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_events_by_officer_success_with_null_dates"
] = {
    "events": [
        {
            "attributes": None,
            "clientId": "444",
            "clientName": {
                "givenNames": "Barbie",
                "middleNames": "Millicent",
                "surname": "Roberts",
            },
            "eventDate": None,
            "metricId": "incarceration_starts_and_inferred",
            "officerAssignmentDate": None,
            "officerAssignmentEndDate": None,
            "officerId": "03",
            "pseudonymizedClientId": "clienthash4",
            "stateCode": "US_PA",
            "supervisionEndDate": None,
            "supervisionStartDate": None,
            "supervisionType": "PROBATION",
        }
    ]
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_officer_not_outlier"] = {
    "officer": {
        "caseloadType": None,
        "district": "Guts",
        "externalId": "123",
        "fullName": {
            "givenNames": "Olivia",
            "middleNames": None,
            "nameSuffix": None,
            "surname": "Rodrigo",
        },
        "outlierMetrics": [],
        "pseudonymizedId": "hashhash",
        "supervisorExternalId": "102",
        "topXPctMetrics": [],
    }
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_officer_success"] = {
    "officer": {
        "caseloadType": None,
        "district": "Guts",
        "externalId": "123",
        "fullName": {
            "givenNames": "Olivia",
            "middleNames": None,
            "nameSuffix": None,
            "surname": "Rodrigo",
        },
        "outlierMetrics": [
            {
                "metricId": "absconsions_bench_warrants",
                "statusesOverTime": [
                    {"endDate": "2023-05-01", "metricRate": 0.1, "status": "FAR"}
                ],
            }
        ],
        "pseudonymizedId": "hashhash",
        "supervisorExternalId": "102",
        "topXPctMetrics": [],
    }
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_officers_for_supervisor"] = {
    "officers": [
        {
            "caseloadType": None,
            "district": "Hogwarts",
            "externalId": "123",
            "fullName": {
                "givenNames": "Harry",
                "middleNames": None,
                "nameSuffix": None,
                "surname": "Potter",
            },
            "outlierMetrics": [
                {
                    "metricId": "metric_one",
                    "statusesOverTime": [
                        {"endDate": "2023-05-01", "metricRate": 0.1, "status": "FAR"},
                        {"endDate": "2023-04-01", "metricRate": 0.1, "status": "FAR"},
                    ],
                }
            ],
            "pseudonymizedId": "hashhash",
            "supervisorExternalId": "102",
            "topXPctMetrics": [
                {"metricId": "incarceration_starts_and_inferred", "topXPct": 10}
            ],
        },
        {
            "caseloadType": None,
            "district": "Hogwarts",
            "externalId": "456",
            "fullName": {
                "givenNames": "Ron",
                "middleNames": None,
                "nameSuffix": None,
                "surname": "Weasley",
            },
            "outlierMetrics": [],
            "pseudonymizedId": "hashhashhash",
            "supervisorExternalId": "102",
            "topXPctMetrics": [],
        },
    ]
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_officers_mismatched_supervisor_can_access_all"
] = {
    "officers": [
        {
            "caseloadType": None,
            "district": "Hogwarts",
            "externalId": "123",
            "fullName": {
                "givenNames": "Harry",
                "middleNames": None,
                "nameSuffix": None,
                "surname": "Potter",
            },
            "outlierMetrics": [
                {
                    "metricId": "metric_one",
                    "statusesOverTime": [
                        {"endDate": "2023-05-01", "metricRate": 0.1, "status": "FAR"},
                        {"endDate": "2023-04-01", "metricRate": 0.1, "status": "FAR"},
                    ],
                }
            ],
            "pseudonymizedId": "hashhash",
            "supervisorExternalId": "102",
            "topXPctMetrics": [],
        },
        {
            "caseloadType": None,
            "district": "Hogwarts",
            "externalId": "456",
            "fullName": {
                "givenNames": "Ron",
                "middleNames": None,
                "nameSuffix": None,
                "surname": "Weasley",
            },
            "outlierMetrics": [],
            "pseudonymizedId": "hashhashhash",
            "supervisorExternalId": "102",
            "topXPctMetrics": [],
        },
    ]
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_state_configuration_success"
] = {
    "config": {
        "atOrAboveRateLabel": "label3",
        "atOrBelowRateLabel": "At or below statewide rate",
        "clientEvents": [{"displayName": "Sanctions", "name": "violation_responses"}],
        "exclusionReasonDescription": "description",
        "featureVariant": None,
        "learnMoreUrl": "https://recidiviz.org",
        "metrics": [
            {
                "bodyDisplayName": "incarceration rate",
                "descriptionMarkdown": """Incarceration rate description

<br />
Incarceration rate denominator description""",
                "eventName": "incarcerations",
                "eventNamePastTense": "were incarcerated",
                "eventNameSingular": "incarceration",
                "name": "incarceration_starts_and_inferred",
                "outcomeType": "ADVERSE",
                "titleDisplayName": "Incarceration Rate (CPVs & TPVs)",
                "topXPct": None,
            },
            {
                "bodyDisplayName": "absconsion rate",
                "descriptionMarkdown": "",
                "eventName": "absconsions",
                "eventNamePastTense": "absconded",
                "eventNameSingular": "absconsion",
                "name": "absconsions_bench_warrants",
                "outcomeType": "ADVERSE",
                "titleDisplayName": "Absconsion Rate",
                "topXPct": None,
            },
        ],
        "noneAreOutliersLabel": "label1",
        "slightlyWorseThanRateLabel": "Slightly worse than statewide rate",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "updatedAt": "Mon, 01 Jan 2024 00:00:00 GMT",
        "updatedBy": "alexa@recidiviz.org",
        "worseThanRateLabel": "label2",
    }
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_supervisors"] = {
    "supervisors": [
        {
            "email": "supervisor2@recidiviz.org",
            "externalId": "102",
            "fullName": {
                "givenNames": "Supervisor",
                "middleNames": None,
                "nameSuffix": None,
                "surname": "2",
            },
            "hasOutliers": True,
            "pseudonymizedId": "hash2",
            "supervisionDistrict": "2",
        }
    ]
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_update_user_info_for_supervisor"
] = {
    "entity": {
        "email": "supervisor2@recidiviz.org",
        "externalId": "102",
        "fullName": {
            "givenNames": "Supervisor",
            "middleNames": None,
            "nameSuffix": None,
            "surname": "2",
        },
        "hasOutliers": True,
        "pseudonymizedId": "hashhash",
        "supervisionDistrict": "2",
    },
    "hasSeenOnboarding": True,
    "role": "supervision_officer_supervisor",
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_user_info_for_recidiviz_user"] = {
    "entity": {
        "email": "supervisor2@recidiviz.org",
        "externalId": "102",
        "fullName": {
            "givenNames": "Supervisor",
            "middleNames": None,
            "nameSuffix": None,
            "surname": "2",
        },
        "hasOutliers": True,
        "pseudonymizedId": "hashhash",
        "supervisionDistrict": "2",
    },
    "hasSeenOnboarding": False,
    "role": "supervision_officer_supervisor",
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_user_info_for_supervisor_match"
] = {
    "entity": {
        "email": "supervisor2@recidiviz.org",
        "externalId": "102",
        "fullName": {
            "givenNames": "Supervisor",
            "middleNames": None,
            "nameSuffix": None,
            "surname": "2",
        },
        "hasOutliers": True,
        "pseudonymizedId": "hashhash",
        "supervisionDistrict": "2",
    },
    "hasSeenOnboarding": False,
    "role": "supervision_officer_supervisor",
}
