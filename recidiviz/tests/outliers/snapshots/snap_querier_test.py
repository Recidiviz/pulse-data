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
    "TestOutliersQuerier.TestOutliersQuerier test_get_benchmarks_non_all_category"
] = [
    {
        "benchmarks": [
            {"end_date": "2023-05-01", "target": 0.12, "threshold": 0.18},
            {"end_date": "2023-04-01", "target": 0.14, "threshold": 0.18},
            {"end_date": "2023-03-01", "target": 0.11, "threshold": 0.18},
            {"end_date": "2023-02-01", "target": 0.15, "threshold": 0.18},
            {"end_date": "2023-01-01", "target": 0.13, "threshold": 0.18},
        ],
        "caseload_type": "NOT_SEX_OFFENSE",
        "latest_period_values": {"far": [0.8], "met": [0.1, 0.1], "near": [0.32]},
        "metric_id": "absconsions_bench_warrants",
    },
    {
        "benchmarks": [
            {"end_date": "2023-05-01", "target": 0.1, "threshold": 0.18},
            {"end_date": "2023-04-01", "target": 0.11, "threshold": 0.18},
            {"end_date": "2023-03-01", "target": 0.09, "threshold": 0.18},
            {"end_date": "2023-02-01", "target": 0.12, "threshold": 0.18},
            {"end_date": "2023-01-01", "target": 0.1, "threshold": 0.18},
        ],
        "caseload_type": "SEX_OFFENSE",
        "latest_period_values": {"far": [], "met": [], "near": []},
        "metric_id": "absconsions_bench_warrants",
    },
    {
        "benchmarks": [
            {"end_date": "2023-05-01", "target": 0.09, "threshold": 0.18},
            {"end_date": "2023-04-01", "target": 0.12, "threshold": 0.18},
        ],
        "caseload_type": "NOT_SEX_OFFENSE",
        "latest_period_values": {
            "far": [0.26],
            "met": [0.0, 0.04, 0.11],
            "near": [0.184],
        },
        "metric_id": "incarceration_starts_and_inferred",
    },
    {
        "benchmarks": [
            {"end_date": "2023-05-01", "target": 0.08, "threshold": 0.18},
            {"end_date": "2023-04-01", "target": 0.1, "threshold": 0.18},
        ],
        "caseload_type": "SEX_OFFENSE",
        "latest_period_values": {"far": [0.333], "met": [0.12], "near": [0.17]},
        "metric_id": "incarceration_starts_and_inferred",
    },
    {
        "benchmarks": [
            {"end_date": "2023-05-01", "target": 0.011, "threshold": 0.05},
            {"end_date": "2023-04-01", "target": 0.011, "threshold": 0.05},
        ],
        "caseload_type": "NOT_SEX_OFFENSE",
        "latest_period_values": {
            "far": [0.0],
            "met": [0.039, 0.11, 0.184, 0.27],
            "near": [],
        },
        "metric_id": "task_completions_transfer_to_limited_supervision",
    },
    {
        "benchmarks": [
            {"end_date": "2023-05-01", "target": 0.016, "threshold": 0.05},
            {"end_date": "2023-04-01", "target": 0.016, "threshold": 0.05},
        ],
        "caseload_type": "SEX_OFFENSE",
        "latest_period_values": {"far": [], "met": [0.126, 0.171, 0.333], "near": []},
        "metric_id": "task_completions_transfer_to_limited_supervision",
    },
]

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_excluded_officers_for_supervisor"
] = [
    GenericRepr(
        "ExcludedSupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='10', middle_names='', name_suffix=''), external_id='10', pseudonymized_id='officerhash10', supervisor_external_id='102', supervisor_external_ids=['102'], district=None)"
    ),
    GenericRepr(
        "ExcludedSupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='11', middle_names='', name_suffix=''), external_id='11', pseudonymized_id='officerhash11', supervisor_external_id='102', supervisor_external_ids=['102'], district=None)"
    ),
]

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_excluded_supervision_officer_entity_found_match"
] = GenericRepr(
    "ExcludedSupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='10', middle_names='', name_suffix=''), external_id='10', pseudonymized_id='officerhash10', supervisor_external_id='102', supervisor_external_ids=['102'], district=None)"
)

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_officer_level_report_data_by_supervisor"
] = {
    "101": GenericRepr(
        "OfficerSupervisorReportData(metrics=[OutlierMetricInfo(metric=OutliersMetricConfig(name='incarceration_starts_and_inferred', outcome_type=<MetricOutcome.ADVERSE: 'ADVERSE'>, title_display_name='Incarceration Rate (CPVs & TPVs)', body_display_name='incarceration rate', event_name='incarcerations', event_name_singular='incarceration', event_name_past_tense='were incarcerated', description_markdown='Incarceration rate description\\n\\n<br />\\nIncarceration rate denominator description', metric_event_conditions_string=None, top_x_pct=None), target=0.13, other_officers={<TargetStatus.FAR: 'FAR'>: [], <TargetStatus.MET: 'MET'>: [0.11, 0.04, 0.0, 0.12], <TargetStatus.NEAR: 'NEAR'>: [0.184, 0.17]}, highlighted_officers=[OfficerMetricEntity(name=PersonName(given_names='Officer', surname='1', middle_names='', name_suffix=''), external_id='01', rate=0.26, target_status=<TargetStatus.FAR: 'FAR'>, prev_rate=0.32, supervisor_external_id='101', supervisor_external_ids=['101', '104'], supervision_district='1', prev_target_status=<TargetStatus.NEAR: 'NEAR'>), OfficerMetricEntity(name=PersonName(given_names='Officer', surname='8', middle_names='', name_suffix=''), external_id='08', rate=0.333, target_status=<TargetStatus.FAR: 'FAR'>, prev_rate=None, supervisor_external_id='101', supervisor_external_ids=['101'], supervision_district='1', prev_target_status=None)], target_status_strategy=<TargetStatusStrategy.IQR_THRESHOLD: 'IQR_THRESHOLD'>)], metrics_without_outliers=[OutliersMetricConfig(name='task_completions_transfer_to_limited_supervision', outcome_type=<MetricOutcome.FAVORABLE: 'FAVORABLE'>, title_display_name='Limited Supervision Unit Transfer Rate', body_display_name='Limited Supervision Unit transfer rate(s)', event_name='LSU transfers', event_name_singular='LSU transfer', event_name_past_tense='were transferred to LSU', description_markdown='', metric_event_conditions_string=None, top_x_pct=None)], recipient_email_address='supervisor1@recidiviz.org', additional_recipients=[])"
    ),
    "102": GenericRepr(
        "OfficerSupervisorReportData(metrics=[OutlierMetricInfo(metric=OutliersMetricConfig(name='task_completions_transfer_to_limited_supervision', outcome_type=<MetricOutcome.FAVORABLE: 'FAVORABLE'>, title_display_name='Limited Supervision Unit Transfer Rate', body_display_name='Limited Supervision Unit transfer rate(s)', event_name='LSU transfers', event_name_singular='LSU transfer', event_name_past_tense='were transferred to LSU', description_markdown='', metric_event_conditions_string=None, top_x_pct=None), target=0.008, other_officers={<TargetStatus.FAR: 'FAR'>: [], <TargetStatus.MET: 'MET'>: [0.27, 0.11, 0.039, 0.184, 0.126, 0.171, 0.333], <TargetStatus.NEAR: 'NEAR'>: []}, highlighted_officers=[OfficerMetricEntity(name=PersonName(given_names='Officer', surname='4', middle_names='', name_suffix=''), external_id='04', rate=0.0, target_status=<TargetStatus.FAR: 'FAR'>, prev_rate=0.0, supervisor_external_id='102', supervisor_external_ids=['102'], supervision_district='2', prev_target_status=None)], target_status_strategy=<TargetStatusStrategy.ZERO_RATE: 'ZERO_RATE'>)], metrics_without_outliers=[OutliersMetricConfig(name='incarceration_starts_and_inferred', outcome_type=<MetricOutcome.ADVERSE: 'ADVERSE'>, title_display_name='Incarceration Rate (CPVs & TPVs)', body_display_name='incarceration rate', event_name='incarcerations', event_name_singular='incarceration', event_name_past_tense='were incarcerated', description_markdown='Incarceration rate description\\n\\n<br />\\nIncarceration rate denominator description', metric_event_conditions_string=None, top_x_pct=None)], recipient_email_address='supervisor2@recidiviz.org', additional_recipients=['manager2@recidiviz.org', 'manager3@recidiviz.org'])"
    ),
    "103": GenericRepr(
        "OfficerSupervisorReportData(metrics=[], metrics_without_outliers=[OutliersMetricConfig(name='incarceration_starts_and_inferred', outcome_type=<MetricOutcome.ADVERSE: 'ADVERSE'>, title_display_name='Incarceration Rate (CPVs & TPVs)', body_display_name='incarceration rate', event_name='incarcerations', event_name_singular='incarceration', event_name_past_tense='were incarcerated', description_markdown='Incarceration rate description\\n\\n<br />\\nIncarceration rate denominator description', metric_event_conditions_string=None, top_x_pct=None), OutliersMetricConfig(name='task_completions_transfer_to_limited_supervision', outcome_type=<MetricOutcome.FAVORABLE: 'FAVORABLE'>, title_display_name='Limited Supervision Unit Transfer Rate', body_display_name='Limited Supervision Unit transfer rate(s)', event_name='LSU transfers', event_name_singular='LSU transfer', event_name_past_tense='were transferred to LSU', description_markdown='', metric_event_conditions_string=None, top_x_pct=None)], recipient_email_address='manager3@recidiviz.org', additional_recipients=['manager2@recidiviz.org'])"
    ),
    "104": GenericRepr(
        "OfficerSupervisorReportData(metrics=[OutlierMetricInfo(metric=OutliersMetricConfig(name='incarceration_starts_and_inferred', outcome_type=<MetricOutcome.ADVERSE: 'ADVERSE'>, title_display_name='Incarceration Rate (CPVs & TPVs)', body_display_name='incarceration rate', event_name='incarcerations', event_name_singular='incarceration', event_name_past_tense='were incarcerated', description_markdown='Incarceration rate description\\n\\n<br />\\nIncarceration rate denominator description', metric_event_conditions_string=None, top_x_pct=None), target=0.13, other_officers={<TargetStatus.FAR: 'FAR'>: [0.333], <TargetStatus.MET: 'MET'>: [0.11, 0.04, 0.0, 0.12], <TargetStatus.NEAR: 'NEAR'>: [0.184, 0.17]}, highlighted_officers=[OfficerMetricEntity(name=PersonName(given_names='Officer', surname='1', middle_names='', name_suffix=''), external_id='01', rate=0.26, target_status=<TargetStatus.FAR: 'FAR'>, prev_rate=0.32, supervisor_external_id='101', supervisor_external_ids=['101', '104'], supervision_district='1', prev_target_status=<TargetStatus.NEAR: 'NEAR'>)], target_status_strategy=<TargetStatusStrategy.IQR_THRESHOLD: 'IQR_THRESHOLD'>)], metrics_without_outliers=[OutliersMetricConfig(name='task_completions_transfer_to_limited_supervision', outcome_type=<MetricOutcome.FAVORABLE: 'FAVORABLE'>, title_display_name='Limited Supervision Unit Transfer Rate', body_display_name='Limited Supervision Unit transfer rate(s)', event_name='LSU transfers', event_name_singular='LSU transfer', event_name_past_tense='were transferred to LSU', description_markdown='', metric_event_conditions_string=None, top_x_pct=None)], recipient_email_address='manager4@recidiviz.org', additional_recipients=['manager2@recidiviz.org', 'manager3@recidiviz.org'])"
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
                        "external_id": "01",
                        "name": {
                            "given_names": "Officer",
                            "middle_names": "",
                            "name_suffix": "",
                            "surname": "1",
                        },
                        "prev_rate": 0.32,
                        "prev_target_status": "NEAR",
                        "rate": 0.26,
                        "supervision_district": "1",
                        "supervisor_external_id": "101",
                        "supervisor_external_ids": ["101", "104"],
                        "target_status": "FAR",
                    },
                    {
                        "external_id": "08",
                        "name": {
                            "given_names": "Officer",
                            "middle_names": "",
                            "name_suffix": "",
                            "surname": "8",
                        },
                        "prev_rate": None,
                        "prev_target_status": None,
                        "rate": 0.333,
                        "supervision_district": "1",
                        "supervisor_external_id": "101",
                        "supervisor_external_ids": ["101"],
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
                    "top_x_pct": None,
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
                "top_x_pct": None,
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
                        "external_id": "04",
                        "name": {
                            "given_names": "Officer",
                            "middle_names": "",
                            "name_suffix": "",
                            "surname": "4",
                        },
                        "prev_rate": 0.0,
                        "prev_target_status": None,
                        "rate": 0.0,
                        "supervision_district": "2",
                        "supervisor_external_id": "102",
                        "supervisor_external_ids": ["102"],
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
                    "top_x_pct": None,
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
                "top_x_pct": None,
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
                "top_x_pct": None,
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
                "top_x_pct": None,
            },
        ],
        "recipient_email_address": "manager3@recidiviz.org",
    },
    "104": {
        "additional_recipients": ["manager2@recidiviz.org", "manager3@recidiviz.org"],
        "metrics": [
            {
                "highlighted_officers": [
                    {
                        "external_id": "01",
                        "name": {
                            "given_names": "Officer",
                            "middle_names": "",
                            "name_suffix": "",
                            "surname": "1",
                        },
                        "prev_rate": 0.32,
                        "prev_target_status": "NEAR",
                        "rate": 0.26,
                        "supervision_district": "1",
                        "supervisor_external_id": "101",
                        "supervisor_external_ids": ["101", "104"],
                        "target_status": "FAR",
                    }
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
                    "top_x_pct": None,
                },
                "other_officers": {
                    "FAR": [0.333],
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
                "top_x_pct": None,
            }
        ],
        "recipient_email_address": "manager4@recidiviz.org",
    },
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_officers_for_supervisor"
] = [
    GenericRepr(
        "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='3', middle_names='', name_suffix=''), external_id='03', pseudonymized_id='officerhash3', supervisor_external_id='102', supervisor_external_ids=['102'], district='2', outlier_metrics=[{'metric_id': 'absconsions_bench_warrants', 'statuses_over_time': [{'status': 'FAR', 'end_date': '2023-05-01', 'metric_rate': 0.8, 'caseload_category': 'ALL'}, {'status': 'FAR', 'end_date': '2023-04-01', 'metric_rate': 0.8, 'caseload_category': 'ALL'}, {'status': 'FAR', 'end_date': '2023-03-01', 'metric_rate': 0.8, 'caseload_category': 'ALL'}, {'status': 'FAR', 'end_date': '2023-02-01', 'metric_rate': 0.8, 'caseload_category': 'ALL'}, {'status': 'FAR', 'end_date': '2023-01-01', 'metric_rate': 0.8, 'caseload_category': 'ALL'}, {'status': 'FAR', 'end_date': '2022-12-01', 'metric_rate': 0.8, 'caseload_category': 'ALL'}]}], top_x_pct_metrics=[{'metric_id': 'incarceration_starts_and_inferred', 'top_x_pct': 10}], avg_daily_population=54.321, caseload_category='ALL', caseload_type=None)"
    ),
    GenericRepr(
        "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='4', middle_names='', name_suffix=''), external_id='04', pseudonymized_id='officerhash4', supervisor_external_id='102', supervisor_external_ids=['102'], district='2', outlier_metrics=[{'metric_id': 'task_completions_transfer_to_limited_supervision', 'statuses_over_time': [{'status': 'FAR', 'end_date': '2023-05-01', 'metric_rate': 0, 'caseload_category': 'ALL'}, {'status': 'FAR', 'end_date': '2023-04-01', 'metric_rate': 0, 'caseload_category': 'ALL'}]}], top_x_pct_metrics=[], avg_daily_population=3.45, caseload_category='ALL', caseload_type=None)"
    ),
    GenericRepr(
        "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='6', middle_names='', name_suffix=''), external_id='06', pseudonymized_id='officerhash6', supervisor_external_id='102', supervisor_external_ids=['102'], district='2', outlier_metrics=[], top_x_pct_metrics=[], avg_daily_population=90.09, caseload_category='ALL', caseload_type=None)"
    ),
]

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_officers_for_supervisor_non_all_category"
] = [
    GenericRepr(
        "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='3', middle_names='', name_suffix=''), external_id='03', pseudonymized_id='officerhash3', supervisor_external_id='102', supervisor_external_ids=['102'], district='2', outlier_metrics=[{'metric_id': 'absconsions_bench_warrants', 'statuses_over_time': [{'status': 'FAR', 'end_date': '2023-05-01', 'metric_rate': 0.8, 'caseload_category': 'NOT_SEX_OFFENSE'}, {'status': 'FAR', 'end_date': '2023-04-01', 'metric_rate': 0.8, 'caseload_category': 'NOT_SEX_OFFENSE'}, {'status': 'FAR', 'end_date': '2023-03-01', 'metric_rate': 0.8, 'caseload_category': 'NOT_SEX_OFFENSE'}, {'status': 'FAR', 'end_date': '2023-02-01', 'metric_rate': 0.8, 'caseload_category': 'NOT_SEX_OFFENSE'}, {'status': 'FAR', 'end_date': '2023-01-01', 'metric_rate': 0.8, 'caseload_category': 'NOT_SEX_OFFENSE'}, {'status': 'FAR', 'end_date': '2022-12-01', 'metric_rate': 0.8, 'caseload_category': 'NOT_SEX_OFFENSE'}]}], top_x_pct_metrics=[{'metric_id': 'incarceration_starts_and_inferred', 'top_x_pct': 10}], avg_daily_population=54.321, caseload_category='NOT_SEX_OFFENSE', caseload_type=None)"
    ),
    GenericRepr(
        "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='4', middle_names='', name_suffix=''), external_id='04', pseudonymized_id='officerhash4', supervisor_external_id='102', supervisor_external_ids=['102'], district='2', outlier_metrics=[{'metric_id': 'task_completions_transfer_to_limited_supervision', 'statuses_over_time': [{'status': 'FAR', 'end_date': '2023-05-01', 'metric_rate': 0, 'caseload_category': 'NOT_SEX_OFFENSE'}, {'status': 'FAR', 'end_date': '2023-04-01', 'metric_rate': 0, 'caseload_category': 'NOT_SEX_OFFENSE'}]}], top_x_pct_metrics=[], avg_daily_population=3.45, caseload_category='NOT_SEX_OFFENSE', caseload_type=None)"
    ),
    GenericRepr(
        "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='6', middle_names='', name_suffix=''), external_id='06', pseudonymized_id='officerhash6', supervisor_external_id='102', supervisor_external_ids=['102'], district='2', outlier_metrics=[], top_x_pct_metrics=[], avg_daily_population=90.09, caseload_category='SEX_OFFENSE', caseload_type=None)"
    ),
]

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_product_configuration"
] = GenericRepr(
    "OutliersProductConfiguration(updated_by='alexa@recidiviz.org', updated_at=datetime.datetime(2024, 1, 26, 13, 30), feature_variant=None, metrics=[OutliersMetricConfig(name='incarceration_starts_and_inferred', outcome_type=<MetricOutcome.ADVERSE: 'ADVERSE'>, title_display_name='Incarceration Rate (CPVs & TPVs)', body_display_name='incarceration rate', event_name='incarcerations', event_name_singular='incarceration', event_name_past_tense='were incarcerated', description_markdown='', metric_event_conditions_string=None, top_x_pct=None), OutliersMetricConfig(name='incarceration_starts_and_inferred_technical_violation', outcome_type=<MetricOutcome.ADVERSE: 'ADVERSE'>, title_display_name='Technical Incarceration Rate (TPVs)', body_display_name='technical incarceration rate', event_name='technical incarcerations', event_name_singular='technical incarceration', event_name_past_tense='had a technical incarceration', description_markdown='', metric_event_conditions_string=None, top_x_pct=None), OutliersMetricConfig(name='absconsions_bench_warrants', outcome_type=<MetricOutcome.ADVERSE: 'ADVERSE'>, title_display_name='Absconsion Rate', body_display_name='absconsion rate', event_name='absconsions', event_name_singular='absconsion', event_name_past_tense='absconded', description_markdown='', metric_event_conditions_string=None, top_x_pct=None)], learn_more_url='fake.com', supervision_officer_label='agent', supervision_district_label='district', supervision_unit_label='unit', supervision_supervisor_label='supervisor', supervision_district_manager_label='district manager', supervision_jii_label='client', supervisor_has_no_outlier_officers_label='Nice! No officers are outliers on any metrics this month.', officer_has_no_outlier_metrics_label='Nice! No outlying metrics this month.', supervisor_has_no_officers_with_eligible_clients_label='Nice! No outstanding opportunities for now.', officer_has_no_eligible_clients_label='Nice! No outstanding opportunities for now.', none_are_outliers_label='are outliers', worse_than_rate_label='Far worse than statewide rate', exclusion_reason_description='excluded because x', slightly_worse_than_rate_label='slightly worse than statewide rate', at_or_below_rate_label='At or below statewide rate', at_or_above_rate_label='At or above statewide rate', client_events=[], primary_category_type=<InsightsCaseloadCategoryType.ALL: 'ALL'>, caseload_categories=[], outliers_hover='Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.', doc_label='DOC', absconder_label='absconder')"
)

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_product_configuration_with_specialized_category_type"
] = GenericRepr(
    "OutliersProductConfiguration(updated_by='alexa@recidiviz.org', updated_at=datetime.datetime(2024, 1, 26, 13, 30), feature_variant=None, metrics=[OutliersMetricConfig(name='incarceration_starts_and_inferred', outcome_type=<MetricOutcome.ADVERSE: 'ADVERSE'>, title_display_name='Incarceration Rate (CPVs & TPVs)', body_display_name='incarceration rate', event_name='incarcerations', event_name_singular='incarceration', event_name_past_tense='were incarcerated', description_markdown='Incarceration rate description\\n\\n<br />\\nIncarceration rate denominator description', metric_event_conditions_string=None, top_x_pct=None), OutliersMetricConfig(name='task_completions_transfer_to_limited_supervision', outcome_type=<MetricOutcome.FAVORABLE: 'FAVORABLE'>, title_display_name='Limited Supervision Unit Transfer Rate', body_display_name='Limited Supervision Unit transfer rate(s)', event_name='LSU transfers', event_name_singular='LSU transfer', event_name_past_tense='were transferred to LSU', description_markdown='', metric_event_conditions_string=None, top_x_pct=None)], learn_more_url='fake.com', supervision_officer_label='agent', supervision_district_label='district', supervision_unit_label='unit', supervision_supervisor_label='supervisor', supervision_district_manager_label='district manager', supervision_jii_label='client', supervisor_has_no_outlier_officers_label='Nice! No officers are outliers on any metrics this month.', officer_has_no_outlier_metrics_label='Nice! No outlying metrics this month.', supervisor_has_no_officers_with_eligible_clients_label='Nice! No outstanding opportunities for now.', officer_has_no_eligible_clients_label='Nice! No outstanding opportunities for now.', none_are_outliers_label='are outliers', worse_than_rate_label='Far worse than statewide rate', exclusion_reason_description='excluded because x', slightly_worse_than_rate_label='slightly worse than statewide rate', at_or_below_rate_label='At or below statewide rate', at_or_above_rate_label='At or above statewide rate', client_events=[], primary_category_type=<InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY: 'SEX_OFFENSE_BINARY'>, caseload_categories=[CaseloadCategory(id='SEX_OFFENSE', display_name='Sex Offense Caseload'), CaseloadCategory(id='NOT_SEX_OFFENSE', display_name='General + Other Caseloads')], outliers_hover='Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.', doc_label='DOC', absconder_label='absconder')"
)

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_supervision_officer_entity_changing_caseload_categories"
] = GenericRepr(
    "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='1', middle_names='', name_suffix=''), external_id='01', pseudonymized_id='officerhash1', supervisor_external_id='101', supervisor_external_ids=['101', '104'], district='1', outlier_metrics=[{'metric_id': 'incarceration_starts_and_inferred', 'statuses_over_time': [{'status': 'FAR', 'end_date': '2023-05-01', 'metric_rate': 0.26, 'caseload_category': 'NOT_SEX_OFFENSE'}, {'status': 'NEAR', 'end_date': '2023-04-01', 'metric_rate': 0.32, 'caseload_category': 'SEX_OFFENSE'}]}], top_x_pct_metrics=[], avg_daily_population=59.95, caseload_category='NOT_SEX_OFFENSE', caseload_type=None)"
)

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_supervision_officer_entity_found_match"
] = GenericRepr(
    "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='3', middle_names='', name_suffix=''), external_id='03', pseudonymized_id='officerhash3', supervisor_external_id='102', supervisor_external_ids=['102'], district='2', outlier_metrics=[{'metric_id': 'absconsions_bench_warrants', 'statuses_over_time': [{'status': 'FAR', 'end_date': '2023-05-01', 'metric_rate': 0.8, 'caseload_category': 'ALL'}]}], top_x_pct_metrics=[{'metric_id': 'incarceration_starts_and_inferred', 'top_x_pct': 10}], avg_daily_population=54.321, caseload_category='ALL', caseload_type=None)"
)

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_supervision_officer_entity_found_match_not_top_x_pct"
] = GenericRepr(
    "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='9', middle_names='', name_suffix=''), external_id='09', pseudonymized_id='officerhash9', supervisor_external_id='103', supervisor_external_ids=['103'], district=None, outlier_metrics=[], top_x_pct_metrics=[], avg_daily_population=99.9, caseload_category='ALL', caseload_type=None)"
)

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_supervision_officer_entity_found_match_with_highlights"
] = GenericRepr(
    "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='3', middle_names='', name_suffix=''), external_id='03', pseudonymized_id='officerhash3', supervisor_external_id='102', supervisor_external_ids=['102'], district='2', outlier_metrics=[{'metric_id': 'absconsions_bench_warrants', 'statuses_over_time': [{'status': 'FAR', 'end_date': '2023-05-01', 'metric_rate': 0.8, 'caseload_category': 'ALL'}]}], top_x_pct_metrics=[{'metric_id': 'incarceration_starts_and_inferred', 'top_x_pct': 10}], avg_daily_population=54.321, caseload_category='ALL', caseload_type=None)"
)

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_supervision_officer_entity_highlight_in_prev_period_only"
] = GenericRepr(
    "SupervisionOfficerEntity(full_name=PersonName(given_names='Officer', surname='7', middle_names='', name_suffix=''), external_id='07', pseudonymized_id='officerhash7', supervisor_external_id='101', supervisor_external_ids=['101'], district='1', outlier_metrics=[], top_x_pct_metrics=[], avg_daily_population=23.456, caseload_category='ALL', caseload_type=None)"
)

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_supervision_officer_supervisor_entities"
] = [
    GenericRepr(
        "SupervisionOfficerSupervisorEntity(full_name=PersonName(given_names='Supervisor', surname='1', middle_names='', name_suffix=''), external_id='101', pseudonymized_id='hash1', supervision_district=None, email='supervisor1@recidiviz.org', has_outliers=True)"
    ),
    GenericRepr(
        "SupervisionOfficerSupervisorEntity(full_name=PersonName(given_names='Supervisor', surname='2', middle_names='', name_suffix=''), external_id='102', pseudonymized_id='hash2', supervision_district='2', email='supervisor2@recidiviz.org', has_outliers=True)"
    ),
    GenericRepr(
        "SupervisionOfficerSupervisorEntity(full_name=PersonName(given_names='Supervisor', surname='3', middle_names='', name_suffix=''), external_id='103', pseudonymized_id='hash3', supervision_district='2', email='manager3@recidiviz.org', has_outliers=False)"
    ),
]
