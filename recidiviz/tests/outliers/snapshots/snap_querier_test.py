"""
Snapshots for recidiviz/tests/outliers/querier_test.py
Update snapshots automatically by running `pytest recidiviz/tests/outliers/querier_test.py --snapshot-update
Remember to include a docstring like this after updating the snapshots for Pylint purposes
"""
# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import GenericRepr, Snapshot

snapshots = Snapshot()

snapshots["TestOutliersQuerier.TestOutliersQuerier test_get_benchmarks"] = [
    {
        "benchmarks": [
            {"end_date": "2023-05-01", "target": 0.14, "threshold": 0.21},
            {"end_date": "2023-04-01", "target": 0.14, "threshold": 0.21},
            {"end_date": "2023-03-01", "target": 0.14, "threshold": 0.21},
            {"end_date": "2023-02-01", "target": 0.14, "threshold": 0.21},
            {"end_date": "2023-01-01", "target": 0.14, "threshold": 0.21},
        ],
        "caseload_type": "ALL",
        "latest_period_values": {"far": [0.8], "met": [0.1, 0.1], "near": [0.32]},
        "metric_id": "absconsions_bench_warrants",
    },
    {
        "benchmarks": [
            {"end_date": "2023-05-01", "target": 0.13, "threshold": 0.2},
            {"end_date": "2023-04-01", "target": 0.14, "threshold": 0.21},
        ],
        "caseload_type": "ALL",
        "latest_period_values": {
            "far": [0.26, 0.333],
            "met": [0.0, 0.04, 0.11, 0.12],
            "near": [0.17, 0.184],
        },
        "metric_id": "incarceration_starts_and_inferred",
    },
    {
        "benchmarks": [
            {"end_date": "2023-05-01", "target": 0.008, "threshold": 0.1},
            {"end_date": "2023-04-01", "target": 0.008, "threshold": 0.1},
        ],
        "caseload_type": "ALL",
        "latest_period_values": {
            "far": [0.0],
            "met": [0.039, 0.11, 0.126, 0.171, 0.184, 0.27, 0.333],
            "near": [],
        },
        "metric_id": "task_completions_transfer_to_limited_supervision",
    },
]

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_officer_level_report_data_by_supervisor"
] = {
    "101": GenericRepr(
        "OfficerSupervisorReportData(metrics=[OutlierMetricInfo(metric=OutliersMetricConfig(name='incarceration_starts_and_inferred', outcome_type=<MetricOutcome.ADVERSE: 'ADVERSE'>, title_display_name='Incarceration Rate (CPVs & TPVs)', body_display_name='incarceration rate', event_name='incarcerations', event_name_singular='incarceration', event_name_past_tense='were incarcerated', description_markdown='Incarceration rate description\\n\\n<br />\\nIncarceration rate denominator description', metric_event_conditions_string='(event = \"INCARCERATION_START\"  AND JSON_EXTRACT_SCALAR(event_attributes, \"$.is_discretionary\") IN (\"true\")) OR (event = \"SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON\"  AND JSON_EXTRACT_SCALAR(event_attributes, \"$.is_discretionary\") IN (\"true\"))'), target=0.13, other_officers={<TargetStatus.FAR: 'FAR'>: [], <TargetStatus.MET: 'MET'>: [0.11, 0.04, 0.0, 0.12], <TargetStatus.NEAR: 'NEAR'>: [0.184, 0.17]}, highlighted_officers=[OfficerMetricEntity(name=PersonName(given_names='Officer', surname='1', middle_names=None, name_suffix=None), rate=0.26, target_status=<TargetStatus.FAR: 'FAR'>, prev_rate=0.32, supervisor_external_id='101', supervision_district='1', prev_target_status=<TargetStatus.NEAR: 'NEAR'>), OfficerMetricEntity(name=PersonName(given_names='Officer', surname='8', middle_names=None, name_suffix=None), rate=0.333, target_status=<TargetStatus.FAR: 'FAR'>, prev_rate=None, supervisor_external_id='101', supervision_district='1', prev_target_status=None)], target_status_strategy=<TargetStatusStrategy.IQR_THRESHOLD: 'IQR_THRESHOLD'>)], metrics_without_outliers=[OutliersMetricConfig(name='task_completions_transfer_to_limited_supervision', outcome_type=<MetricOutcome.FAVORABLE: 'FAVORABLE'>, title_display_name='Limited Supervision Unit Transfer Rate', body_display_name='Limited Supervision Unit transfer rate(s)', event_name='LSU transfers', event_name_singular='LSU transfer', event_name_past_tense='were transferred to LSU', description_markdown='', metric_event_conditions_string='(event = \"TASK_COMPLETED\"  AND JSON_EXTRACT_SCALAR(event_attributes, \"$.task_type\") IN (\"TRANSFER_TO_LIMITED_SUPERVISION\"))')], recipient_email_address='supervisor1@recidiviz.org', additional_recipients=[])"
    ),
    "102": GenericRepr(
        "OfficerSupervisorReportData(metrics=[OutlierMetricInfo(metric=OutliersMetricConfig(name='task_completions_transfer_to_limited_supervision', outcome_type=<MetricOutcome.FAVORABLE: 'FAVORABLE'>, title_display_name='Limited Supervision Unit Transfer Rate', body_display_name='Limited Supervision Unit transfer rate(s)', event_name='LSU transfers', event_name_singular='LSU transfer', event_name_past_tense='were transferred to LSU', description_markdown='', metric_event_conditions_string='(event = \"TASK_COMPLETED\"  AND JSON_EXTRACT_SCALAR(event_attributes, \"$.task_type\") IN (\"TRANSFER_TO_LIMITED_SUPERVISION\"))'), target=0.008, other_officers={<TargetStatus.FAR: 'FAR'>: [], <TargetStatus.MET: 'MET'>: [0.27, 0.11, 0.039, 0.184, 0.126, 0.171, 0.333], <TargetStatus.NEAR: 'NEAR'>: []}, highlighted_officers=[OfficerMetricEntity(name=PersonName(given_names='Officer', surname='4', middle_names=None, name_suffix=None), rate=0.0, target_status=<TargetStatus.FAR: 'FAR'>, prev_rate=0.0, supervisor_external_id='102', supervision_district='2', prev_target_status=None)], target_status_strategy=<TargetStatusStrategy.ZERO_RATE: 'ZERO_RATE'>)], metrics_without_outliers=[OutliersMetricConfig(name='incarceration_starts_and_inferred', outcome_type=<MetricOutcome.ADVERSE: 'ADVERSE'>, title_display_name='Incarceration Rate (CPVs & TPVs)', body_display_name='incarceration rate', event_name='incarcerations', event_name_singular='incarceration', event_name_past_tense='were incarcerated', description_markdown='Incarceration rate description\\n\\n<br />\\nIncarceration rate denominator description', metric_event_conditions_string='(event = \"INCARCERATION_START\"  AND JSON_EXTRACT_SCALAR(event_attributes, \"$.is_discretionary\") IN (\"true\")) OR (event = \"SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON\"  AND JSON_EXTRACT_SCALAR(event_attributes, \"$.is_discretionary\") IN (\"true\"))')], recipient_email_address='supervisor2@recidiviz.org', additional_recipients=['manager2@recidiviz.org', 'manager3@recidiviz.org'])"
    ),
    "103": GenericRepr(
        "OfficerSupervisorReportData(metrics=[], metrics_without_outliers=[OutliersMetricConfig(name='incarceration_starts_and_inferred', outcome_type=<MetricOutcome.ADVERSE: 'ADVERSE'>, title_display_name='Incarceration Rate (CPVs & TPVs)', body_display_name='incarceration rate', event_name='incarcerations', event_name_singular='incarceration', event_name_past_tense='were incarcerated', description_markdown='Incarceration rate description\\n\\n<br />\\nIncarceration rate denominator description', metric_event_conditions_string='(event = \"INCARCERATION_START\"  AND JSON_EXTRACT_SCALAR(event_attributes, \"$.is_discretionary\") IN (\"true\")) OR (event = \"SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON\"  AND JSON_EXTRACT_SCALAR(event_attributes, \"$.is_discretionary\") IN (\"true\"))'), OutliersMetricConfig(name='task_completions_transfer_to_limited_supervision', outcome_type=<MetricOutcome.FAVORABLE: 'FAVORABLE'>, title_display_name='Limited Supervision Unit Transfer Rate', body_display_name='Limited Supervision Unit transfer rate(s)', event_name='LSU transfers', event_name_singular='LSU transfer', event_name_past_tense='were transferred to LSU', description_markdown='', metric_event_conditions_string='(event = \"TASK_COMPLETED\"  AND JSON_EXTRACT_SCALAR(event_attributes, \"$.task_type\") IN (\"TRANSFER_TO_LIMITED_SUPERVISION\"))')], recipient_email_address='manager3@recidiviz.org', additional_recipients=['manager2@recidiviz.org'])"
    ),
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_officer_level_report_data_by_supervisor_json"
] = {
    "101": {
        "additional_recipients": [],
        "metrics": [
            {
                "highlighted_officers": [
                    {
                        "name": {
                            "given_names": "Officer",
                            "middle_names": None,
                            "name_suffix": None,
                            "surname": "1",
                        },
                        "prev_rate": 0.32,
                        "prev_target_status": "NEAR",
                        "rate": 0.26,
                        "supervision_district": "1",
                        "supervisor_external_id": "101",
                        "target_status": "FAR",
                    },
                    {
                        "name": {
                            "given_names": "Officer",
                            "middle_names": None,
                            "name_suffix": None,
                            "surname": "8",
                        },
                        "prev_rate": None,
                        "prev_target_status": None,
                        "rate": 0.333,
                        "supervision_district": "1",
                        "supervisor_external_id": "101",
                        "target_status": "FAR",
                    },
                ],
                "metric": {
                    "body_display_name": "incarceration rate",
                    "description_markdown": """Incarceration rate description

<br />
Incarceration rate denominator description""",
                    "event_name": "incarcerations",
                    "event_name_past_tense": "were incarcerated",
                    "event_name_singular": "incarceration",
                    "name": "incarceration_starts_and_inferred",
                    "outcome_type": "ADVERSE",
                    "title_display_name": "Incarceration Rate (CPVs & TPVs)",
                },
                "other_officers": {
                    "FAR": [],
                    "MET": [0.11, 0.04, 0.0, 0.12],
                    "NEAR": [0.184, 0.17],
                },
                "target": 0.13,
                "target_status_strategy": "IQR_THRESHOLD",
            }
        ],
        "metrics_without_outliers": [
            {
                "body_display_name": "Limited Supervision Unit transfer rate(s)",
                "description_markdown": "",
                "event_name": "LSU transfers",
                "event_name_past_tense": "were transferred to LSU",
                "event_name_singular": "LSU transfer",
                "name": "task_completions_transfer_to_limited_supervision",
                "outcome_type": "FAVORABLE",
                "title_display_name": "Limited Supervision Unit Transfer Rate",
            }
        ],
        "recipient_email_address": "supervisor1@recidiviz.org",
    },
    "102": {
        "additional_recipients": ["manager2@recidiviz.org", "manager3@recidiviz.org"],
        "metrics": [
            {
                "highlighted_officers": [
                    {
                        "name": {
                            "given_names": "Officer",
                            "middle_names": None,
                            "name_suffix": None,
                            "surname": "4",
                        },
                        "prev_rate": 0.0,
                        "prev_target_status": None,
                        "rate": 0.0,
                        "supervision_district": "2",
                        "supervisor_external_id": "102",
                        "target_status": "FAR",
                    }
                ],
                "metric": {
                    "body_display_name": "Limited Supervision Unit transfer rate(s)",
                    "description_markdown": "",
                    "event_name": "LSU transfers",
                    "event_name_past_tense": "were transferred to LSU",
                    "event_name_singular": "LSU transfer",
                    "name": "task_completions_transfer_to_limited_supervision",
                    "outcome_type": "FAVORABLE",
                    "title_display_name": "Limited Supervision Unit Transfer Rate",
                },
                "other_officers": {
                    "FAR": [],
                    "MET": [0.27, 0.11, 0.039, 0.184, 0.126, 0.171, 0.333],
                    "NEAR": [],
                },
                "target": 0.008,
                "target_status_strategy": "ZERO_RATE",
            }
        ],
        "metrics_without_outliers": [
            {
                "body_display_name": "incarceration rate",
                "description_markdown": """Incarceration rate description

<br />
Incarceration rate denominator description""",
                "event_name": "incarcerations",
                "event_name_past_tense": "were incarcerated",
                "event_name_singular": "incarceration",
                "name": "incarceration_starts_and_inferred",
                "outcome_type": "ADVERSE",
                "title_display_name": "Incarceration Rate (CPVs & TPVs)",
            }
        ],
        "recipient_email_address": "supervisor2@recidiviz.org",
    },
    "103": {
        "additional_recipients": ["manager2@recidiviz.org"],
        "metrics": [],
        "metrics_without_outliers": [
            {
                "body_display_name": "incarceration rate",
                "description_markdown": """Incarceration rate description

<br />
Incarceration rate denominator description""",
                "event_name": "incarcerations",
                "event_name_past_tense": "were incarcerated",
                "event_name_singular": "incarceration",
                "name": "incarceration_starts_and_inferred",
                "outcome_type": "ADVERSE",
                "title_display_name": "Incarceration Rate (CPVs & TPVs)",
            },
            {
                "body_display_name": "Limited Supervision Unit transfer rate(s)",
                "description_markdown": "",
                "event_name": "LSU transfers",
                "event_name_past_tense": "were transferred to LSU",
                "event_name_singular": "LSU transfer",
                "name": "task_completions_transfer_to_limited_supervision",
                "outcome_type": "FAVORABLE",
                "title_display_name": "Limited Supervision Unit Transfer Rate",
            },
        ],
        "recipient_email_address": "manager3@recidiviz.org",
    },
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_officers_for_supervisor"
] = [
    GenericRepr(
        "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='3', middle_names=None, name_suffix=None), external_id='03', pseudonymized_id='officerhash3', supervisor_external_id='102', district='2', caseload_type=None, outlier_metrics=[{'metric_id': 'absconsions_bench_warrants', 'statuses_over_time': [{'status': 'FAR', 'end_date': '2023-05-01', 'metric_rate': 0.8}, {'status': 'FAR', 'end_date': '2023-04-01', 'metric_rate': 0.8}, {'status': 'FAR', 'end_date': '2023-03-01', 'metric_rate': 0.8}, {'status': 'FAR', 'end_date': '2023-02-01', 'metric_rate': 0.8}, {'status': 'FAR', 'end_date': '2023-01-01', 'metric_rate': 0.8}, {'status': 'FAR', 'end_date': '2022-12-01', 'metric_rate': 0.8}]}])"
    ),
    GenericRepr(
        "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='4', middle_names=None, name_suffix=None), external_id='04', pseudonymized_id='officerhash4', supervisor_external_id='102', district='2', caseload_type=None, outlier_metrics=[{'metric_id': 'task_completions_transfer_to_limited_supervision', 'statuses_over_time': [{'status': 'FAR', 'end_date': '2023-05-01', 'metric_rate': 0}, {'status': 'FAR', 'end_date': '2023-04-01', 'metric_rate': 0}]}])"
    ),
    GenericRepr(
        "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='6', middle_names=None, name_suffix=None), external_id='06', pseudonymized_id='officerhash6', supervisor_external_id='102', district='2', caseload_type=None, outlier_metrics=[])"
    ),
]

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_supervision_officer_entities"
] = [
    GenericRepr(
        "SupervisionOfficerSupervisorEntity(full_name=PersonName(given_names='Supervisor', surname='1', middle_names=None, name_suffix=None), external_id='101', pseudonymized_id='hash1', supervision_district=None, email='supervisor1@recidiviz.org', has_outliers=True)"
    ),
    GenericRepr(
        "SupervisionOfficerSupervisorEntity(full_name=PersonName(given_names='Supervisor', surname='2', middle_names=None, name_suffix=None), external_id='102', pseudonymized_id='hash2', supervision_district='2', email='supervisor2@recidiviz.org', has_outliers=True)"
    ),
    GenericRepr(
        "SupervisionOfficerSupervisorEntity(full_name=PersonName(given_names='Supervisor', surname='3', middle_names=None, name_suffix=None), external_id='103', pseudonymized_id='hash3', supervision_district='2', email='manager3@recidiviz.org', has_outliers=False)"
    ),
]

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_supervision_officer_entity_found_match"
] = GenericRepr(
    "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='3', middle_names=None, name_suffix=None), external_id='03', pseudonymized_id='officerhash3', supervisor_external_id='102', district='2', caseload_type=None, outlier_metrics=[{'metric_id': 'absconsions_bench_warrants', 'statuses_over_time': [{'status': 'FAR', 'end_date': '2023-05-01', 'metric_rate': 0.8}]}])"
)