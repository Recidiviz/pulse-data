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
"""Helpers for building supervision metrics views """
from typing import Dict

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    EARLY_DISCHARGE_REQUESTS,
    INCARCERATION_STARTS,
    INCARCERATION_STARTS_AND_INFERRED,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION,
    TASK_COMPLETIONS_FULL_TERM_DISCHARGE,
    TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
)
from recidiviz.outliers.types import OutliersConfig

OUTLIERS_CONFIGS_BY_STATE: Dict[StateCode, OutliersConfig] = {
    StateCode.US_IX: OutliersConfig(
        metrics=[
            INCARCERATION_STARTS_TECHNICAL_VIOLATION,
            ABSCONSIONS_BENCH_WARRANTS,
            INCARCERATION_STARTS,
            EARLY_DISCHARGE_REQUESTS,
            TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
            TASK_COMPLETIONS_FULL_TERM_DISCHARGE,
        ],
    ),
    StateCode.US_PA: OutliersConfig(
        metrics=[INCARCERATION_STARTS_AND_INFERRED],
        unit_of_analysis_to_exclusion={
            MetricUnitOfAnalysisType.SUPERVISION_DISTRICT: ["FAST", "CO"]
        },
    ),
}


def supervision_metric_query_template(
    unit_of_analysis: MetricUnitOfAnalysis,
) -> str:
    """
    Helper for querying supervision_<unit_of_analysis>_aggregated_metrics views
    """
    subqueries = []
    for state_code, config in OUTLIERS_CONFIGS_BY_STATE.items():
        subqueries.append(
            f"""
SELECT
    {list_to_query_string(unit_of_analysis.primary_key_columns)},
    period,
    end_date,
    "avg_daily_population" AS metric_id,
    avg_daily_population AS metric_value,
FROM `{{project_id}}.{{aggregated_metrics_dataset}}.supervision_{unit_of_analysis.level_name_short}_aggregated_metrics_materialized`
WHERE state_code = '{state_code.value}'
        """
        )

        for metric in config.metrics:
            subqueries.append(
                f"""
SELECT
    {list_to_query_string(unit_of_analysis.primary_key_columns)},
    period,
    end_date,
    "{metric.name}" AS metric_id,
    {metric.name} AS metric_value,
FROM `{{project_id}}.{{aggregated_metrics_dataset}}.supervision_{unit_of_analysis.level_name_short}_aggregated_metrics_materialized`
WHERE state_code = '{state_code.value}'
"""
            )

    query = "\nUNION ALL\n".join(subqueries)
    return query
