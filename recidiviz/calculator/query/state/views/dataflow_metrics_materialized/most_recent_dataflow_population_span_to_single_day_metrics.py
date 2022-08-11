# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Generator to transform the most recent population span metrics into day-by-day metrics."""
from typing import Dict, List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

POPULATION_SPAN_METRIC_TO_DAY_BY_DAY_METRIC: Dict[str, str] = {
    "supervision_population_span_metrics": "date_of_supervision",
    "incarceration_population_span_metrics": "date_of_stay",
}

MOST_RECENT_POPULATION_SPAN_TO_SINGLE_DAY_METRICS_TEMPLATE: str = """
    /*{description}*/
    SELECT * EXCEPT (start_date_inclusive, end_date_exclusive)
    FROM
    `{project_id}.{materialized_metrics_dataset}.most_recent_{population_span_metrics_table}_materialized`,
    UNNEST(GENERATE_DATE_ARRAY(start_date_inclusive,
    -- End date *inclusive* or today
    DATE_SUB(
        -- End date exclusive or tomorrow
        IFNULL(end_date_exclusive, 
            DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 1 DAY)
        ), 
    INTERVAL 1 DAY), INTERVAL 1 DAY)) AS {metric_date_column}
"""


def _make_most_recent_population_span_to_single_day_metric_view_builder(
    metric_name: str,
) -> SimpleBigQueryViewBuilder:
    description = (
        f"{metric_name} output converting to the most recent single day metrics"
    )
    metric_table_view_name = metric_name.rstrip("_metrics")
    return SimpleBigQueryViewBuilder(
        dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
        view_id=f"most_recent_{metric_table_view_name}_to_single_day_metrics",
        view_query_template=MOST_RECENT_POPULATION_SPAN_TO_SINGLE_DAY_METRICS_TEMPLATE,
        description=description,
        metric_date_column=POPULATION_SPAN_METRIC_TO_DAY_BY_DAY_METRIC[metric_name],
        population_span_metrics_table=metric_name,
        materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
        should_materialize=True,
    )


def generate_most_recent_population_span_to_single_day_metric_view_builders(
    metric_tables: List[str],
) -> List[SimpleBigQueryViewBuilder]:
    return [
        _make_most_recent_population_span_to_single_day_metric_view_builder(
            metric_table
        )
        for metric_table in metric_tables
    ]


MOST_RECENT_POPULATION_SPAN_TO_SINGLE_DAY_METRICS_VIEW_BUILDERS: List[
    SimpleBigQueryViewBuilder
] = generate_most_recent_population_span_to_single_day_metric_view_builders(
    list(POPULATION_SPAN_METRIC_TO_DAY_BY_DAY_METRIC.keys())
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for builder in MOST_RECENT_POPULATION_SPAN_TO_SINGLE_DAY_METRICS_VIEW_BUILDERS:
            builder.build_and_print()
