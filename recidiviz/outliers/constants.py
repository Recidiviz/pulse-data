# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Outliers-related constants"""

import recidiviz.aggregated_metrics.models.aggregated_metric_configurations as metric_config
from recidiviz.outliers.types import (
    MetricOutcome,
    OutliersClientEvent,
    OutliersMetric,
    OutliersVitalsMetricConfig,
)

DEFAULT_NUM_LOOKBACK_PERIODS = 5

###############################################
# Outliers Metrics used to configure by states
# Note: some of these metrics use string literals because the corresponding aggregated metric configuration objects
# create their names dynamically and are not available to reference without filtering through a list.
###############################################

# Adverse metrics
INCARCERATION_STARTS_TECHNICAL_VIOLATION = OutliersMetric(
    aggregated_metric=next(
        metric
        for metric in metric_config.INCARCERATION_STARTS_WITH_VIOLATION_TYPE_METRICS
        if metric.name == "incarceration_starts_technical_violation"
    ),
    outcome_type=MetricOutcome.ADVERSE,
)

ABSCONSIONS_BENCH_WARRANTS = OutliersMetric(
    aggregated_metric=metric_config.ABSCONSIONS_BENCH_WARRANTS,
    outcome_type=MetricOutcome.ADVERSE,
)
ABSCONSIONS_BENCH_WARRANTS_FROM_PAROLE = OutliersMetric(
    aggregated_metric=metric_config.ABSCONSIONS_BENCH_WARRANTS_FROM_PAROLE,
    outcome_type=MetricOutcome.ADVERSE,
)
ABSCONSIONS_BENCH_WARRANTS_FROM_PROBATION = OutliersMetric(
    aggregated_metric=metric_config.ABSCONSIONS_BENCH_WARRANTS_FROM_PROBATION,
    outcome_type=MetricOutcome.ADVERSE,
)

INCARCERATION_STARTS = OutliersMetric(
    aggregated_metric=metric_config.INCARCERATION_STARTS,
    outcome_type=MetricOutcome.ADVERSE,
)

INCARCERATION_STARTS_MOST_SEVERE_VIOLATION_TYPE_NOT_ABSCONSION = OutliersMetric(
    aggregated_metric=metric_config.INCARCERATION_STARTS_MOST_SEVERE_VIOLATION_TYPE_NOT_ABSCONSION,
    outcome_type=MetricOutcome.ADVERSE,
)

INCARCERATION_STARTS_AND_INFERRED = OutliersMetric(
    aggregated_metric=metric_config.INCARCERATION_STARTS_AND_INFERRED,
    outcome_type=MetricOutcome.ADVERSE,
)
INCARCERATION_STARTS_AND_INFERRED_FROM_PAROLE = OutliersMetric(
    aggregated_metric=metric_config.INCARCERATION_STARTS_AND_INFERRED_FROM_PAROLE,
    outcome_type=MetricOutcome.ADVERSE,
)
INCARCERATION_STARTS_AND_INFERRED_FROM_PROBATION = OutliersMetric(
    aggregated_metric=metric_config.INCARCERATION_STARTS_AND_INFERRED_FROM_PROBATION,
    outcome_type=MetricOutcome.ADVERSE,
)
INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION = OutliersMetric(
    aggregated_metric=next(
        metric
        for metric in metric_config.INCARCERATION_STARTS_AND_INFERRED_WITH_VIOLATION_TYPE_METRICS
        if metric.name == "incarceration_starts_and_inferred_technical_violation"
    ),
    outcome_type=MetricOutcome.ADVERSE,
)

# Favorable metrics
EARLY_DISCHARGE_REQUESTS = OutliersMetric(
    aggregated_metric=metric_config.EARLY_DISCHARGE_REQUESTS,
    outcome_type=MetricOutcome.FAVORABLE,
)

TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION = OutliersMetric(
    aggregated_metric=next(
        metric
        for metric in metric_config.TASK_COMPLETED_METRICS_SUPERVISION
        if metric.name == "task_completions_transfer_to_limited_supervision"
    ),
    outcome_type=MetricOutcome.FAVORABLE,
)

TASK_COMPLETIONS_FULL_TERM_DISCHARGE = OutliersMetric(
    aggregated_metric=next(
        metric
        for metric in metric_config.TASK_COMPLETED_METRICS_SUPERVISION
        if metric.name == "task_completions_full_term_discharge"
    ),
    outcome_type=MetricOutcome.FAVORABLE,
)

TREATMENT_STARTS = OutliersMetric(
    aggregated_metric=metric_config.TREATMENT_STARTS,
    outcome_type=MetricOutcome.FAVORABLE,
)

VIOLATIONS_ABSCONSION = OutliersMetric(
    aggregated_metric=next(
        metric
        for metric in metric_config.VIOLATIONS_BY_TYPE_METRICS
        if metric.name == "violations_absconsion"
    ),
    outcome_type=MetricOutcome.ADVERSE,
)

# Vitals Metrics
TIMELY_RISK_ASSESSMENT = OutliersVitalsMetricConfig(
    metric_id="timely_risk_assessment",
    title_display_name="Timely Risk Assessment",
    body_display_name="Assessment",
)
TIMELY_CONTACT = OutliersVitalsMetricConfig(
    metric_id="timely_contact",
    title_display_name="Timely Contact",
    body_display_name="Contact",
)
TIMELY_F2F_CONTACT = OutliersVitalsMetricConfig(
    metric_id="timely_contact",
    title_display_name="Timely F2F Contact",
    body_display_name="Contact",
)
TIMELY_CONTACT_DUE_DATE_BASED = OutliersVitalsMetricConfig(
    metric_id="timely_contact_due_date_based",
    title_display_name="Timely Contact",
    body_display_name="Contact",
)

# Lantern Events
VIOLATIONS = OutliersClientEvent(aggregated_metric=metric_config.VIOLATIONS)
VIOLATION_RESPONSES = OutliersClientEvent(metric_config.VIOLATION_RESPONSES)
TREATMENT_REFERRALS = OutliersClientEvent(metric_config.TREATMENT_REFERRALS)
