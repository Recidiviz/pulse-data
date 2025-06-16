"""
Snapshots for recidiviz/tests/outliers/aggregated_metrics_collector_test.py
Update snapshots automatically by running `pytest recidiviz/tests/outliers/aggregated_metrics_collector_test.py --snapshot-update
Remember to include a docstring like this after updating the snapshots for Pylint purposes
"""

# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots[
    "TestAggregatedMetricsCollector.TestAggregatedMetricsCollector test_get_metrics"
] = [
    "incarceration_starts_technical_violation",
    "absconsions_bench_warrants",
    "absconsions_bench_warrants_from_parole",
    "absconsions_bench_warrants_from_probation",
    "incarceration_starts",
    "incarceration_starts_most_severe_violation_type_not_absconsion",
    "incarceration_starts_and_inferred",
    "incarceration_starts_and_inferred_from_parole",
    "incarceration_starts_and_inferred_from_probation",
    "incarceration_starts_and_inferred_technical_violation",
    "early_discharge_requests",
    "task_completions_transfer_to_limited_supervision",
    "task_completions_full_term_discharge",
    "treatment_starts",
    "violations_absconsion",
]
