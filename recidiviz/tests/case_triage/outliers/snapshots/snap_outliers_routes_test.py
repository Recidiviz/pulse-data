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

snapshots["TestOutliersRoutes.TestOutliersRoutes Bad request incomplete body"] = {
    "message": "Invalid request data: missing 1 required positional argument: 'request_change_type'"
}

snapshots["TestOutliersRoutes.TestOutliersRoutes Missing request body"] = {
    "code": 400,
    "description": "The browser (or proxy) sent a request that this server could not understand.",
    "name": "Bad Request",
}

snapshots["TestOutliersRoutes.TestOutliersRoutes Target supervisor not found"] = {
    "message": "Target supervisor with pseudonymized_id, badhash, not found."
}

snapshots["TestOutliersRoutes.TestOutliersRoutes Unauthorized user"] = {
    "message": "User with pseudonymized_id pseudo123 cannot make roster change request."
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes Unauthorized user without feature variant"
] = {
    "message": "User with pseudonymized_id pseudo123 cannot make roster change request."
}

snapshots["TestOutliersRoutes.TestOutliersRoutes Valid request from supervisor"] = {
    "email": "mock_email",
    "id": "1",
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes Valid request from supervisor with all supervisor access"
] = {"email": "mock_email", "id": "1"}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes get_vitals_metrics_for_officer_when_can_access_all_supervisors"
] = [
    {
        "metricId": "timely_contact",
        "vitalsMetrics": [
            {
                "metric30DDelta": -17.0,
                "metricDate": "2025-07-01",
                "metricValue": 80.0,
                "officerPseudonymizedId": "officerhash2",
                "previousMetricDate": "2025-06-01",
                "metricNumerator": 8.0,
                "metricDenominator": 10.0,
            }
        ],
    },
    {
        "metricId": "timely_risk_assessment",
        "vitalsMetrics": [
            {
                "metric30DDelta": 0.0,
                "metricDate": "2025-07-01",
                "metricValue": 94.0,
                "officerPseudonymizedId": "officerhash2",
                "previousMetricDate": None,
                "metricNumerator": 94.0,
                "metricDenominator": 100.0,
            }
        ],
    },
]

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes get_vitals_metrics_for_officer_when_user_is_officer"
] = [
    {
        "metricId": "timely_contact",
        "vitalsMetrics": [
            {
                "metric30DDelta": -17.0,
                "metricDate": "2025-07-01",
                "metricValue": 80.0,
                "officerPseudonymizedId": "officerhash2",
                "previousMetricDate": "2025-06-01",
                "metricNumerator": 8.0,
                "metricDenominator": 10.0,
            }
        ],
    },
    {
        "metricId": "timely_risk_assessment",
        "vitalsMetrics": [
            {
                "metric30DDelta": 0.0,
                "metricDate": "2025-07-01",
                "metricValue": 94.0,
                "officerPseudonymizedId": "officerhash2",
                "previousMetricDate": None,
                "metricNumerator": 94.0,
                "metricDenominator": 100.0,
            }
        ],
    },
]

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes get_vitals_metrics_for_supervisor_when_can_access_all_supervisors"
] = [
    {
        "metricId": "timely_contact",
        "vitalsMetrics": [
            {
                "metric30DDelta": -3.0,
                "metricDate": "2025-07-01",
                "metricValue": 95.0,
                "officerPseudonymizedId": "officerhash1",
                "previousMetricDate": "2025-06-01",
                "metricNumerator": 19.0,
                "metricDenominator": 20.0,
            },
            {
                "metric30DDelta": -17.0,
                "metricDate": "2025-07-01",
                "metricValue": 80.0,
                "officerPseudonymizedId": "officerhash2",
                "previousMetricDate": "2025-06-01",
                "metricNumerator": 8.0,
                "metricDenominator": 10.0,
            },
        ],
    },
    {
        "metricId": "timely_risk_assessment",
        "vitalsMetrics": [
            {
                "metric30DDelta": 0.0,
                "metricDate": "2025-07-01",
                "metricValue": 100.0,
                "officerPseudonymizedId": "officerhash1",
                "previousMetricDate": None,
                "metricNumerator": 20.0,
                "metricDenominator": 20.0,
            },
            {
                "metric30DDelta": 0.0,
                "metricDate": "2025-07-01",
                "metricValue": 94.0,
                "officerPseudonymizedId": "officerhash2",
                "previousMetricDate": None,
                "metricNumerator": 94.0,
                "metricDenominator": 100.0,
            },
        ],
    },
]

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_action_strategies_3_months_as_outlier_already_surfaced"
] = {"hash2": None, "hash4": None, "hash6": None}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_action_strategies_3_months_as_outlier_eligible"
] = {
    "hash2": None,
    "hash4": "ACTION_STRATEGY_OUTLIER_3_MONTHS",
    "hash6": "ACTION_STRATEGY_OUTLIER_3_MONTHS",
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_action_strategies_3_months_as_outlier_ineligible"
] = {"hash2": None, "hash5": None, "hash7": "ACTION_STRATEGY_OUTLIER", "hash8": None}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_action_strategies_as_outlier_already_surfaced"
] = {"hash2": None, "hashhash": None, "hashhashhash": None}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_action_strategies_as_outlier_comprehensive"
] = {
    "hash1": "ACTION_STRATEGY_OUTLIER",
    "hash2": "ACTION_STRATEGY_OUTLIER_3_MONTHS",
    "hash3": "ACTION_STRATEGY_OUTLIER_ABSCONSION",
    "hash4": None,
    "hash5": "ACTION_STRATEGY_OUTLIER_NEW_OFFICER",
    "hash6": None,
    "supervisorHash": "ACTION_STRATEGY_60_PERC_OUTLIERS",
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_action_strategies_as_outlier_eligible"
] = {"hash2": None, "hashhash": "ACTION_STRATEGY_OUTLIER", "hashhashhash": None}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_action_strategies_filters_for_included_officers"
] = {"hash2": None, "hashhash": "ACTION_STRATEGY_OUTLIER"}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_action_strategies_leadership_user"
] = {"leadershipHash": None}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_action_strategies_leadership_user_also_supervisor"
] = {"hash2": None, "hashhash": "ACTION_STRATEGY_OUTLIER", "hashhashhash": None}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_action_strategy_60_perc_outliers_eligible_eligible"
] = {
    "hash1": "ACTION_STRATEGY_OUTLIER",
    "hash2": "ACTION_STRATEGY_OUTLIER",
    "hash3": "ACTION_STRATEGY_OUTLIER",
    "hash4": None,
    "supervisorHash": "ACTION_STRATEGY_60_PERC_OUTLIERS",
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_all_officers_success"] = {
    "officers": [
        {
            "avgDailyPopulation": None,
            "district": "Hogwarts",
            "earliestPersonAssignmentDate": None,
            "email": "officer123@recidiviz.org",
            "externalId": "123",
            "fullName": {
                "givenNames": "Harry",
                "middleNames": None,
                "nameSuffix": None,
                "surname": "Potter",
            },
            "includeInOutcomes": True,
            "latestLoginDate": None,
            "pseudonymizedId": "hashhash",
            "supervisorExternalId": "102",
            "supervisorExternalIds": ["102"],
            "zeroGrantOpportunities": None,
        },
        {
            "avgDailyPopulation": None,
            "district": "Hogwarts",
            "earliestPersonAssignmentDate": None,
            "email": "officer456@recidiviz.org",
            "externalId": "456",
            "fullName": {
                "givenNames": "Ron",
                "middleNames": None,
                "nameSuffix": None,
                "surname": "Weasley",
            },
            "includeInOutcomes": True,
            "latestLoginDate": None,
            "pseudonymizedId": "hashhashhash",
            "supervisorExternalId": "102",
            "supervisorExternalIds": ["103"],
            "zeroGrantOpportunities": None,
        },
    ]
}

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
            "caseloadCategory": "ALL",
            "latestPeriodValues": {"far": [0.8], "met": [0.1], "near": [0.32]},
            "metricId": "absconsions_bench_warrants",
        }
    ]
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_client_success"] = {
    "client": {
        "birthdate": "1989-01-01",
        "clientId": "111",
        "clientName": {
            "givenNames": "Harry",
            "middleNames": "James",
            "surname": "Potter",
        },
        "gender": "MALE",
        "pseudonymizedClientId": "clienthash1",
        "raceOrEthnicity": "WHITE",
        "stateCode": "US_XX",
    }
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_client_success_for_csg_user"
] = {
    "client": {
        "birthdate": "1989-01-01",
        "clientId": "111",
        "clientName": {
            "givenNames": "Harry",
            "middleNames": "James",
            "surname": "Potter",
        },
        "gender": "MALE",
        "pseudonymizedClientId": "clienthash1",
        "raceOrEthnicity": "WHITE",
        "stateCode": "US_XX",
    }
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_events_by_client_no_name"] = {
    "events": [
        {
            "attributes": {"code": "NON-TECHNICAL", "description": None},
            "clientId": "555",
            "clientName": None,
            "eventDate": "2023-04-01",
            "metricId": "violations",
            "officerId": "OFFICER3",
            "pseudonymizedClientId": "clienthash5",
            "stateCode": "US_XX",
        },
        {
            "attributes": None,
            "clientId": "556665",
            "clientName": None,
            "eventDate": "2023-06-01",
            "metricId": "violations",
            "officerId": None,
            "pseudonymizedClientId": "clienthash5",
            "stateCode": "US_XX",
        },
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
            "officerId": "OFFICER3",
            "pseudonymizedClientId": "clienthash1",
            "stateCode": "US_XX",
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
            "officerId": "OFFICER3",
            "pseudonymizedClientId": "clienthash1",
            "stateCode": "US_XX",
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
            "metricId": "incarceration_starts_and_inferred",
            "officerAssignmentDate": "2022-01-01",
            "officerAssignmentEndDate": "2023-06-01",
            "officerId": "OFFICER3",
            "pseudonymizedClientId": "clienthash2",
            "stateCode": "US_XX",
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
            "metricId": "incarceration_starts_and_inferred",
            "officerAssignmentDate": "2022-01-01",
            "officerAssignmentEndDate": "2023-06-01",
            "officerId": "OFFICER3",
            "pseudonymizedClientId": "clienthash2",
            "stateCode": "US_XX",
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
            "metricId": "absconsions_bench_warrants",
            "officerAssignmentDate": None,
            "officerAssignmentEndDate": None,
            "officerId": "OFFICER3",
            "pseudonymizedClientId": "clienthash4",
            "stateCode": "US_XX",
            "supervisionEndDate": None,
            "supervisionStartDate": None,
            "supervisionType": "PROBATION",
        }
    ]
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_officer_as_supervision_officer_success"
] = {
    "officer": {
        "avgDailyPopulation": 10.0,
        "district": "Guts",
        "earliestPersonAssignmentDate": None,
        "email": "officer123@recidiviz.org",
        "externalId": "123",
        "fullName": {
            "givenNames": "Olivia",
            "middleNames": None,
            "nameSuffix": None,
            "surname": "Rodrigo",
        },
        "includeInOutcomes": True,
        "latestLoginDate": None,
        "pseudonymizedId": "officerhash1",
        "supervisorExternalId": "102",
        "supervisorExternalIds": ["102"],
        "zeroGrantOpportunities": None,
    }
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_officer_success"] = {
    "officer": {
        "avgDailyPopulation": 10.0,
        "district": "Guts",
        "earliestPersonAssignmentDate": None,
        "email": "officer123@recidiviz.org",
        "externalId": "123",
        "fullName": {
            "givenNames": "Olivia",
            "middleNames": None,
            "nameSuffix": None,
            "surname": "Rodrigo",
        },
        "includeInOutcomes": True,
        "latestLoginDate": None,
        "pseudonymizedId": "hashhash",
        "supervisorExternalId": "102",
        "supervisorExternalIds": ["102"],
        "zeroGrantOpportunities": None,
    }
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_officers_for_supervisor"] = {
    "officers": [
        {
            "avgDailyPopulation": 10.0,
            "district": "Hogwarts",
            "earliestPersonAssignmentDate": "Mon, 01 Jan 2024 00:00:00 GMT",
            "email": "officer123@recidiviz.org",
            "externalId": "123",
            "fullName": {
                "givenNames": "Harry",
                "middleNames": None,
                "nameSuffix": None,
                "surname": "Potter",
            },
            "includeInOutcomes": True,
            "latestLoginDate": "2025-01-01",
            "pseudonymizedId": "hashhash",
            "supervisorExternalId": "102",
            "supervisorExternalIds": ["102"],
            "zeroGrantOpportunities": ["usPaAdminSupervision"],
        },
        {
            "avgDailyPopulation": 10.0,
            "district": "Hogwarts",
            "earliestPersonAssignmentDate": "Mon, 01 Jan 2024 00:00:00 GMT",
            "email": "officer456@recidiviz.org",
            "externalId": "456",
            "fullName": {
                "givenNames": "Ron",
                "middleNames": None,
                "nameSuffix": None,
                "surname": "Weasley",
            },
            "includeInOutcomes": True,
            "latestLoginDate": "2025-01-01",
            "pseudonymizedId": "hashhashhash",
            "supervisorExternalId": "102",
            "supervisorExternalIds": ["102"],
            "zeroGrantOpportunities": [],
        },
    ]
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_officers_mismatched_supervisor_can_access_all"
] = {
    "officers": [
        {
            "avgDailyPopulation": 10.0,
            "district": "Hogwarts",
            "earliestPersonAssignmentDate": None,
            "email": "officer123@recidiviz.org",
            "externalId": "123",
            "fullName": {
                "givenNames": "Harry",
                "middleNames": None,
                "nameSuffix": None,
                "surname": "Potter",
            },
            "includeInOutcomes": True,
            "latestLoginDate": None,
            "pseudonymizedId": "hashhash",
            "supervisorExternalId": "102",
            "supervisorExternalIds": ["102"],
            "zeroGrantOpportunities": None,
        },
        {
            "avgDailyPopulation": 10.0,
            "district": "Hogwarts",
            "earliestPersonAssignmentDate": None,
            "email": "officer456@recidiviz.org",
            "externalId": "456",
            "fullName": {
                "givenNames": "Ron",
                "middleNames": None,
                "nameSuffix": None,
                "surname": "Weasley",
            },
            "includeInOutcomes": True,
            "latestLoginDate": None,
            "pseudonymizedId": "hashhashhash",
            "supervisorExternalId": "102",
            "supervisorExternalIds": ["102"],
            "zeroGrantOpportunities": None,
        },
    ]
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_outcomes_for_supervisor"] = {
    "message": "User with pseudo id [None] cannot access requested supervisor with pseudo id [hash1]."
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_outcomes_for_supervisor_mismatched_supervisor_can_access_all"
] = {
    "outcomes": [
        {
            "caseloadCategory": "ALL",
            "externalId": "123",
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
            "topXPctMetrics": [],
        },
        {
            "caseloadCategory": "ALL",
            "externalId": "456",
            "outlierMetrics": [],
            "pseudonymizedId": "hashhashhash",
            "topXPctMetrics": [],
        },
    ]
}

snapshots["TestOutliersRoutes.TestOutliersRoutes test_get_outcomes_success"] = {
    "outcomes": {
        "caseloadCategory": "ALL",
        "externalId": "123",
        "outlierMetrics": [
            {
                "metricId": "absconsions_bench_warrants",
                "statusesOverTime": [
                    {"endDate": "2023-05-01", "metricRate": 0.1, "status": "FAR"}
                ],
            }
        ],
        "pseudonymizedId": "hashhash",
        "topXPctMetrics": [
            {"metricId": "incarceration_starts_and_inferred", "topXPct": 10}
        ],
    }
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_state_configuration_success"
] = {
    "config": {
        "absconderLabel": "absconder",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "label3",
        "atOrBelowRateLabel": "At or below statewide rate",
        "caseloadCategories": [
            {"displayName": "Sex Offense Caseload", "id": "SEX_OFFENSE"},
            {"displayName": "General + Other Caseloads", "id": "NOT_SEX_OFFENSE"},
        ],
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
                "isAbsconsionMetric": False,
                "listTableText": "Clients will appear on this list multiple times if they have been incarcerated more than once under this officer in the time period.",
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
                "isAbsconsionMetric": True,
                "listTableText": "Clients will appear on this list multiple times if they have had more than one absconsion under this officer in the time period.",
                "name": "absconsions_bench_warrants",
                "outcomeType": "ADVERSE",
                "titleDisplayName": "Absconsion Rate",
                "topXPct": None,
            },
        ],
        "noneAreOutliersLabel": "label1",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "primaryCategoryType": "SEX_OFFENSE_BINARY",
        "slightlyWorseThanRateLabel": "Slightly worse than statewide rate",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "Mon, 01 Jan 2024 00:00:00 GMT",
        "updatedBy": "alexa@recidiviz.org",
        "vitalsMetrics": [
            {
                "bodyDisplayName": "Assessment",
                "metricId": "timely_risk_assessment",
                "titleDisplayName": "Timely Risk Assessment",
                "numeratorQueryFragment": "avg_population_assessment_required - avg_population_assessment_overdue",
                "denominatorQueryFragment": "avg_population_assessment_required",
                "metricTimePeriod": "DAY",
            },
            {
                "bodyDisplayName": "Contact",
                "metricId": "timely_contact",
                "titleDisplayName": "Timely Contact",
                "numeratorQueryFragment": "avg_population_contacts_required - avg_population_contacts_overdue",
                "denominatorQueryFragment": "avg_population_contacts_required",
                "metricTimePeriod": "DAY",
            },
        ],
        "vitalsMetricsMethodologyUrl": "https://recidiviz.org",
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
            "supervisionLocationForListPage": "region 2",
            "supervisionLocationForSupervisorPage": "unit 2",
        }
    ]
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_get_vitals_metrics_for_officer"
] = [
    {
        "metricId": "timely_contact",
        "vitalsMetrics": [
            {
                "metric30DDelta": -17.0,
                "metricDate": "2025-07-01",
                "metricValue": 80.0,
                "officerPseudonymizedId": "officerhash2",
                "previousMetricDate": "2025-06-01",
                "metricNumerator": 8.0,
                "metricDenominator": 10.0,
            }
        ],
    },
    {
        "metricId": "timely_risk_assessment",
        "vitalsMetrics": [
            {
                "metric30DDelta": 0.0,
                "metricDate": "2025-07-01",
                "metricValue": 94.0,
                "officerPseudonymizedId": "officerhash2",
                "previousMetricDate": None,
                "metricNumerator": 94.0,
                "metricDenominator": 100.0,
            }
        ],
    },
]

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_patch_action_strategy_success"
] = {
    "actionStrategy": "ACTION_STRATEGY_OUTLIER_ABSCONSION",
    "officerPseudonymizedId": "officerHash",
    "stateCode": "US_XX",
    "timestamp": "Tue, 20 Aug 2024 00:00:00 GMT",
    "userPseudonymizedId": "hashhash",
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
        "supervisionLocationForListPage": "region 2",
        "supervisionLocationForSupervisorPage": "unit 2",
    },
    "hasDismissedDataUnavailableNote": False,
    "hasDismissedRateOver100PercentNote": False,
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
        "supervisionLocationForListPage": "region 2",
        "supervisionLocationForSupervisorPage": "unit 2",
    },
    "hasDismissedDataUnavailableNote": False,
    "hasDismissedRateOver100PercentNote": False,
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
        "supervisionLocationForListPage": "region 2",
        "supervisionLocationForSupervisorPage": "unit 2",
    },
    "hasDismissedDataUnavailableNote": False,
    "hasDismissedRateOver100PercentNote": False,
    "hasSeenOnboarding": False,
    "role": "supervision_officer_supervisor",
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_user_info_for_supervisor_match_with_officer_and_supervisor_entity"
] = {
    "entity": {
        "email": "supervisor2@recidiviz.org",
        "externalId": "102",
        "fullName": {
            "givenNames": "Olivia",
            "middleNames": None,
            "nameSuffix": None,
            "surname": "Rodrigo",
        },
        "hasOutliers": True,
        "pseudonymizedId": "hashhash",
        "supervisionLocationForListPage": "region 2",
        "supervisionLocationForSupervisorPage": "unit 2",
    },
    "hasDismissedDataUnavailableNote": False,
    "hasDismissedRateOver100PercentNote": False,
    "hasSeenOnboarding": False,
    "role": "supervision_officer_supervisor",
}

snapshots[
    "TestOutliersRoutes.TestOutliersRoutes test_vitals_metrics_for_supervisor"
] = [
    {
        "metricId": "timely_contact",
        "vitalsMetrics": [
            {
                "metric30DDelta": -3.0,
                "metricDate": "2025-07-01",
                "metricValue": 95.0,
                "officerPseudonymizedId": "officerhash1",
                "previousMetricDate": "2025-06-01",
                "metricNumerator": 19.0,
                "metricDenominator": 20.0,
            },
            {
                "metric30DDelta": -17.0,
                "metricDate": "2025-07-01",
                "metricValue": 80.0,
                "officerPseudonymizedId": "officerhash2",
                "previousMetricDate": "2025-06-01",
                "metricNumerator": 8.0,
                "metricDenominator": 10.0,
            },
        ],
    },
    {
        "metricId": "timely_risk_assessment",
        "vitalsMetrics": [
            {
                "metric30DDelta": 0.0,
                "metricDate": "2025-07-01",
                "metricValue": 100.0,
                "officerPseudonymizedId": "officerhash1",
                "previousMetricDate": "2025-06-01",
                "metricNumerator": 20.0,
                "metricDenominator": 20.0,
            },
            {
                "metric30DDelta": 0.0,
                "metricDate": "2025-07-01",
                "metricValue": 94.0,
                "officerPseudonymizedId": "officerhash2",
                "previousMetricDate": "2025-06-01",
                "metricNumerator": 94.0,
                "metricDenominator": 100.0,
            },
        ],
    },
]
