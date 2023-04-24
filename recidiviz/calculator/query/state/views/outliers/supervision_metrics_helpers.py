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

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
)
from recidiviz.common.constants.states import StateCode

METRICS_TO_EXPORT: Dict[str, List[str]] = {
    "incarceration_starts_and_inferred_technical_violation": [
        StateCode.US_PA.value,
    ],
    "incarceration_starts_technical_violation": [
        StateCode.US_IX.value,
    ],
    "absconsions_bench_warrants": [
        StateCode.US_IX.value,
    ],
    "incarceration_starts": [
        StateCode.US_IX.value,
    ],
    "task_completions_early_discharge": [
        StateCode.US_IX.value,
    ],
    "task_completions_transfer_to_limited_supervision": [
        StateCode.US_IX.value,
    ],
    "task_completions_full_term_discharge": [
        StateCode.US_IX.value,
    ],
}


def supervision_metric_query_template(
    unit_of_analysis: MetricUnitOfAnalysis,
) -> str:
    query = "\nUNION ALL\n".join(
        [
            f"""
SELECT 
    {list_to_query_string(unit_of_analysis.primary_key_columns)},
    period,
    end_date,
    "{metric}" AS metric_id,
    {metric} AS metric_value,
FROM `{{project_id}}.{{aggregated_metrics_dataset}}.supervision_{unit_of_analysis.level_name_short}_aggregated_metrics_materialized` 
WHERE state_code IN ({list_to_query_string(states, quoted=True)})
            """
            for metric, states in METRICS_TO_EXPORT.items()
        ]
    )
    return query
