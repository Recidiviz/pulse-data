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
from recidiviz.outliers.types import MetricOutcome, OutliersMetric

###############################################
# Outliers Metrics used to configure by states
# Note: some of these metrics use string literals because the corresponding aggregated metric configuration objects
# create their names dynamically and are not available to reference without filtering through a list.
###############################################

# Adverse metrics
INCARCERATION_STARTS_TECHNICAL_VIOLATION = OutliersMetric(
    name=next(
        metric.name
        for metric in metric_config.INCARCERATION_STARTS_WITH_VIOLATION_TYPE_METRICS
        if metric.name == "incarceration_starts_technical_violation"
    ),
    outcome_type=MetricOutcome.ADVERSE,
)

ABSCONSIONS_BENCH_WARRANTS = OutliersMetric(
    name=metric_config.ABSCONSIONS_BENCH_WARRANTS.name,
    outcome_type=MetricOutcome.ADVERSE,
)

INCARCERATION_STARTS = OutliersMetric(
    name=metric_config.INCARCERATION_STARTS.name,
    outcome_type=MetricOutcome.ADVERSE,
)

INCARCERATION_STARTS_AND_INFERRED = OutliersMetric(
    name=metric_config.INCARCERATION_STARTS_AND_INFERRED.name,
    outcome_type=MetricOutcome.ADVERSE,
)


# Favorable metrics
EARLY_DISCHARGE_REQUESTS = OutliersMetric(
    name=metric_config.EARLY_DISCHARGE_REQUESTS.name,
    outcome_type=MetricOutcome.FAVORABLE,
)

TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION = OutliersMetric(
    name=next(
        metric.name
        for metric in metric_config.TASK_COMPLETED_METRICS_SUPERVISION
        if metric.name == "task_completions_transfer_to_limited_supervision"
    ),
    outcome_type=MetricOutcome.FAVORABLE,
)

TASK_COMPLETIONS_FULL_TERM_DISCHARGE = OutliersMetric(
    name=next(
        metric.name
        for metric in metric_config.TASK_COMPLETED_METRICS_SUPERVISION
        if metric.name == "task_completions_full_term_discharge"
    ),
    outcome_type=MetricOutcome.FAVORABLE,
)
