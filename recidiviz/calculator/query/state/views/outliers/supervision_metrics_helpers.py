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
from typing import Dict, List

import attr

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)
from recidiviz.common.constants.states import StateCode


@attr.s
class OutliersConfig:
    # List of metrics that are relevant for this state,
    # where each element corresponds to a column name in an aggregated_metrics views
    metrics: List[str] = attr.ib()

    # Location exclusions; a unit of analysis mapped to a list of ids to exclude
    unit_of_analysis_to_exclusion: Dict[MetricUnitOfAnalysisType, List[str]] = attr.ib(
        default=None
    )


OUTLIERS_CONFIGS_BY_STATE: Dict[StateCode, OutliersConfig] = {
    StateCode.US_IX: OutliersConfig(
        metrics=[
            "incarceration_starts_technical_violation",
            "absconsions_bench_warrants",
            "incarceration_starts",
            "task_completions_early_discharge",
            "task_completions_transfer_to_limited_supervision",
            "task_completions_full_term_discharge",
        ],
    ),
    StateCode.US_PA: OutliersConfig(
        metrics=["incarceration_starts_and_inferred"],
        unit_of_analysis_to_exclusion={
            MetricUnitOfAnalysisType.SUPERVISION_DISTRICT: ["FAST", "CO"]
        },
    ),
}


def supervision_metric_query_template(
    unit_of_analysis: MetricUnitOfAnalysis,
) -> str:
    subqueries = []
    for state_code, config in OUTLIERS_CONFIGS_BY_STATE.items():
        for metric in config.metrics:
            subqueries.append(
                f"""
SELECT
    {list_to_query_string(unit_of_analysis.primary_key_columns)},
    period,
    end_date,
    "{metric}" AS metric_id,
    {metric} AS metric_value,
    avg_daily_population,
FROM `{{project_id}}.{{aggregated_metrics_dataset}}.supervision_{unit_of_analysis.level_name_short}_aggregated_metrics_materialized`
WHERE state_code = '{state_code.value}'
"""
            )

    query = "\nUNION ALL\n".join(subqueries)
    return query
